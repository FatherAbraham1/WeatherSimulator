package eu.fastdata.ws

import java.io.File

import org.joda.time.{DateTime, Weeks}
import org.joda.time.format.DateTimeFormat

import scala.io.Source
import org.scalatest.FunSuite

class WeatherSimulatorTest extends FunSuite {

  val testFile = "results/test"

  case class WeatherResult(name: String, lat: Float, lon: Float, date: DateTime, description: String, temp: Float,
                           pressure: Float, relativeHumidity: Int)

  def toWeatherResult(s: String): Option[WeatherResult] = {
    val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val split = s.split("\\|")
    if(split.length == 8)
    //  YYC|51.012783|-114.35434|2017-06-20T02:38:11Z|Rain|10.3|1006.1|70
      Some(WeatherResult(split(0), split(1).toFloat, split(2).toFloat, dateFormat.parseDateTime(split(3)), split(4),
      split(5).toFloat, split(6).toFloat, split(7).toInt))
    else None
  }

  test("generates correct output file") {
    new File(testFile).delete()
    val args = Array[String]("52","results/test")
    WeatherSimulator.main(args)
    assert(new File(testFile).exists())
    assert(Source.fromFile(testFile).getLines().map(s => toWeatherResult(s)).filter(x => !x.isDefined).isEmpty)
  }

  test("all weather states should be present: Rain, Snow and Sunny") {
    val expected = Set[String]("Rain", "Snow", "Sunny")
    val actual = Source.fromFile(testFile).getLines().map(s => toWeatherResult(s).get.description).toSet
    assert(expected === actual)
  }

  test("dates of weather predictions are incremented correctly") {
    val dates = Source.fromFile(testFile).getLines().map(s => toWeatherResult(s).get.date).toList
    assert(Weeks.weeksBetween(dates(0), dates(dates.size-1)).getWeeks === 52)
  }

  //TODO: Add many more tests...

}

