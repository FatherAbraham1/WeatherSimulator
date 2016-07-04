package eu.fastdata.ws

import java.io.FileWriter
import java.util.Date

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, graphx}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Duration, LocalDateTime}

import scala.util.Random

object GraphWeatherOps {

  case class Vertex(lat: Float, lon: Float, temp: Float, terrain: Int,
                    tempChangeFactor: Float, precip: Int, name: Option[String]){
    override def productPrefix = ""
  }

  //need to checkpoint regularly to avoid stack overflow for too long linage
  val checkpointInterval = 52

  def loadGraph(sc: SparkContext, edgesFile: String = "edges.csv", verticesFile: String = "vertices.csv"): Graph[Vertex, Long] = {
    val vertices: RDD[(Long, Vertex)] = sc.textFile(verticesFile)
      .map(line => line.replaceAll("Some\\(","").replaceAll("[()]","").split(",").map(elem => elem.trim))
      .filter(a => !a.isEmpty)
      .map(x => (x(0).toLong, Vertex(x(1).toFloat, x(2).toFloat, x(3).toFloat, x(4).toInt, x(5).toFloat, x(6).toInt,
        if(x(7).equalsIgnoreCase("none")) None else Some(x(7)))))

    val edges: RDD[Edge[Long]] = sc.textFile(edgesFile)
      .map(line => line.replaceAll("Edge\\(","").replaceAll("\\)","").split(",").map(elem => elem.trim))
      .filter(a => !a.isEmpty).map(x => Edge(x(0).toLong, x(1).toLong))

    Graph(vertices, edges)
  }

  def forecast(sc: SparkContext, g: Graph[Vertex, Long],
               fileName: String, numSteps: Int, currentIteration: Int = 0): Unit = {
    if(currentIteration <= numSteps) {

      val graph: Graph[Vertex, Long] = if(currentIteration % checkpointInterval == 0) {
        val ts = new Date().getTime
        val edgesFile = s"checkpoints/edges-${currentIteration}-${ts}"
        val verticesFile = s"checkpoints/vertices-${currentIteration}-${ts}"
        g.edges.saveAsTextFile(edgesFile)
        g.vertices.saveAsTextFile(verticesFile)
        loadGraph(sc, edgesFile, verticesFile)
      } else {
        g
      }

      val neighbours = graph.collectNeighbors(EdgeDirection.Either)
        .map(x => (x._1, (neighboursAverageTemp(x._2), neighboursPrecipCount(x._2))))

      val updatedGraph = updateWeather(graph, neighbours, currentIteration)

      printWeatherStationsStats(updatedGraph, currentIteration)
      saveWeatherStationsStats(updatedGraph, fileName, currentIteration)

      //iterate recursively
      forecast(sc, updatedGraph, fileName, numSteps, currentIteration + 1)
    }
  }

  def neighboursAverageTemp(va: Array[(graphx.VertexId, Vertex)]): Float = {
    val count = va.length
    val total = va.map(x => x._2).foldLeft(0.0F)((t, v) => t + v.temp)
    total / count
  }

  def neighboursPrecipCount(va: Array[(graphx.VertexId, Vertex)]): Int = va.map(x => x._2.precip).sum


  def updateWeather(graph: Graph[Vertex, Long], neighbours: RDD[(graphx.VertexId, (Float, Int))], week: Int): Graph[Vertex, Long] = {

    def updatedTemp(v: Vertex, neighboursTemp: Float, step: Int): Float = {
      //certain latitudes get seasons
      //invoking only two seasons per year (52 weeks)
      //poles have bigger temperature difference than equator, thus additional +0.2
      val week = step % 52
      val seasonTemp = if(v.lat > 30 && v.lat < 90) {
        if(week > 39) 1.2f
        else if(week < 12) -1
        else 0
      } else if(v.lat < -30 && v.lat > -90) {
        if(week > 39) -1
        else if(week < 12) 1.2f
        else 0
      } else 0

      val oldTemp = v.temp
      if(oldTemp > neighboursTemp) oldTemp - (oldTemp - neighboursTemp) * v.tempChangeFactor + seasonTemp
      else oldTemp + (neighboursTemp - oldTemp) * v.tempChangeFactor + seasonTemp
    }

    def updatedPrecip(p: Int, neighboursCount: Int): Int = {
      // using modified Game of Life rules
      if(p == 1) {
        if(neighboursCount == 2) 1 else 0
      } else if(neighboursCount == 1) 1 else 0
    }

    val updatedVertices = graph.vertices.join(neighbours)
      .mapValues(x =>
        Vertex(x._1.lat, x._1.lon, updatedTemp(x._1, x._2._1, week), x._1.terrain,
          x._1.tempChangeFactor, updatedPrecip(x._1.precip, x._2._2), x._1.name))

    Graph(updatedVertices, graph.edges)
  }

  def vertexToWeather(x: Vertex, initialDate: LocalDateTime): String = {

    def calculateRh(isSunny: Boolean, temp: Float): Int = {
      val randomDelta = Random.nextInt(5)
      //associating rain with increased relative humidity
      val dew = if(isSunny) (temp / 2) - randomDelta else (temp / 2) + randomDelta
      val calculatedRh = (100 * Math.pow((112 - 0.1 * temp + dew)/(112 + 0.9 * temp), 8)).toInt
      if(calculatedRh > 100 ) 100 else calculatedRh
    }

    def calculateDate(lon: Float): String = {
      //starting from 180E and adding four minutes for every degree
      val localDate = initialDate.withDurationAdded(Duration.standardMinutes(4), lon.toInt + 180)
      val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
      dateFormat.print(localDate)
    }

    def getWeatherDescription(temp: Float, precipitation: Int): String = {
      if(precipitation == 1) {
        if(temp > 0) "Rain" else "Snow"
      } else "Sunny"
    }

    def calculatePressure(isSunny: Boolean, elevation: Int = 0): String = {
      val averagePressure = 1013.3
      val randomDelta = Math.abs(Random.nextGaussian() * 10)
      //associating lower pressure with rain
      val pressure = if(isSunny) averagePressure + randomDelta
      else averagePressure - randomDelta
      pressure.formatted("%.1f")
    }

    val weatherDescription = getWeatherDescription(x.temp,x.precip)
    val isSunny = weatherDescription.equalsIgnoreCase("sunny")
    val calculatedPressure = calculatePressure(isSunny)
    val calculatedHumidity = calculateRh(isSunny, x.temp)

    s"${x.name.get}|${x.lat}|${x.lon}|${calculateDate(x.lon)}|${weatherDescription}|" +
      s"${x.temp.formatted("%.1f")}|${calculatedPressure}|${calculatedHumidity}"
  }

  def getInitialDate(currentIteration: Int): LocalDateTime = {
    new LocalDateTime().withDurationAdded(Duration.standardDays(currentIteration),7)
  }

  def printWeatherStationsStats(graph: Graph[Vertex, Long], currentIteration: Int = 0): Unit = {

    graph.vertices
      .map(x => x._2.asInstanceOf[Vertex])
      .filter(x => x.name.isDefined)
      .foreach(x => println(vertexToWeather(x, getInitialDate(currentIteration))))

    println("--------------------------------------------------------------------")
  }

  def saveWeatherStationsStats(graph: Graph[Vertex, Long], fileName: String, currentIteration: Int = 0): Unit = {

    def writeToFile(fileName: String, record: String): Unit = {
      val fw = new FileWriter(fileName, true)
      try {
        fw.write(record + "\n")
      }
      finally fw.close()
    }

    graph.vertices
      .map(x => x._2.asInstanceOf[Vertex])
      .filter(x => x.name.isDefined)
      .foreach(x => writeToFile(fileName, vertexToWeather(x, getInitialDate(currentIteration))))
  }

}
