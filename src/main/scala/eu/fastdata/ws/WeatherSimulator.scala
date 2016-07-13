package eu.fastdata.ws

import org.apache.spark.{SparkConf, SparkContext}
import eu.fastdata.ws.GraphWeatherOps._

import scala.util.Try

object WeatherSimulator {

  def main(args: Array[String]): Unit = {

    val steps = Try(args(0).toInt).getOrElse(52)
    val outputFileName = Try(args(1)).getOrElse("results/weather-stations-results.psv")
    val seedVerticesFileName = Try(args(2)).getOrElse("seed-data/vertices.csv")
    val seedEdgesFileName = Try(args(3)).getOrElse("seed-data/edges.csv")

    val sparkConf = new SparkConf().setAppName("WeatherSimulator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val graph = loadGraph(sc, seedEdgesFileName, seedVerticesFileName)

    saveWeatherStationsStats(graph, outputFileName)

    forecast(sc, graph, outputFileName, steps)

    sc.stop()
  }

}
