package learning

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructType}

import scala.util.Try

object Main {

  val sc: SparkSession = SparkSession.builder
    .master("local")
    .appName("User Visits")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import sc.implicits._

    println("main")

    val userVisits = readUserVisits()
    val ipLocations = readIpLocations()
    val cities = readCities()

    userVisits.show()
    ipLocations.show()
    cities.show()

    val citiesFiltered = cities.filter(cities("city").isNotNull)
    val ipLocationsWithCities = ipLocations
      .as("ip")
      .join(
        citiesFiltered.as("c"),
        $"ip.locId" === $"c.locId"
      )

    val window = Window.partitionBy("year").orderBy($"sum(adRevenue)".desc)

    userVisits
      .select(userVisits.col("*"), year(to_timestamp($"visitDate", "HH-dd-MM-yyyy")).as("year"))
      .join(ipLocationsWithCities, $"ip" >= $"startIpNum" and $"ip" <= $"endIpNum")
      .select($"year", $"country", $"city", $"adRevenue")
      .groupBy($"year", $"country", $"city")
      .agg("adRevenue" -> "sum")
      .withColumn("rank", dense_rank().over(window))
      .filter($"rank" <= 3)
      .orderBy($"year".asc, $"rank".asc)
      .show()
  }

  //  sourceIP VARCHAR(116)
  //  destURL VARCHAR(100)
  //  visitDate DATE
  //    adRevenue FLOAT
  //    userAgent VARCHAR(256)
  //  countryCode CHAR(3)
  //  languageCode CHAR(6)
  //  searchWord VARCHAR(32)
  //  duration INT
  def readUserVisits(): DataFrame = {
    import sc.implicits._

    val schema = new StructType()
      .add("sourceIP", StringType, nullable = false)
      .add("destURL", StringType, nullable = false)
      .add("visitDate", DateType, nullable = false)
      .add("adRevenue", FloatType, nullable = false)
      .add("userAgent", StringType, nullable = false)
      .add("countryCode", StringType, nullable = false)
      .add("languageCode", StringType, nullable = false)
      .add("searchWord", StringType, nullable = false)
      .add("duration", IntegerType, nullable = false)

    val df: Dataset[Row] = sc.read
      .format("csv")
      .options(Map("delimiter" -> ",", "header" -> "false"))
      .schema(schema)
      .csv("src/main/resources/local_small/UserVisits.txt")
    //      .load("hdfs:///tmp/data/bids2.txt")

    val ipToIntUDF = udf(ipToInt _)

    df.select(df.col("*"), ipToIntUDF($"sourceIP").as("ip"))
  }

  def readIpLocations(): DataFrame = {
    val schema = new StructType()
      .add("startIpNum", IntegerType, nullable = false)
      .add("endIpNum", IntegerType, nullable = false)
      .add("locId", IntegerType, nullable = false)

    val df: Dataset[Row] = sc.read
      .format("csv")
      .options(Map("delimiter" -> ",", "header" -> "true"))
      .schema(schema)
      .csv("src/main/resources/geo/ip.csv")
    //      .load("hdfs:///tmp/data/bids2.txt")

    df
  }

  def readCities(): DataFrame = {
    val schema = new StructType()
      .add("locId", IntegerType, nullable = false)
      .add("country", StringType, nullable = false)
      .add("region", IntegerType, nullable = true)
      .add("city", StringType, nullable = true)
      .add("postalCode", IntegerType, nullable = true)
      .add("latitude", FloatType, nullable = false)
      .add("longitude", FloatType, nullable = false)
      .add("metroCode", IntegerType, nullable = true)
      .add("areaCode", IntegerType, nullable = true)

    val df: Dataset[Row] = sc.read
      .format("csv")
      .options(Map("delimiter" -> ",", "header" -> "true"))
      .schema(schema)
      .csv("src/main/resources/geo/city.csv")
    //      .load("hdfs:///tmp/data/bids2.txt")

    df
  }

  def ipToInt(ip: String): Long = {
    //    https://stackoverflow.com/questions/34798559/how-to-convert-ipv4-addresses-to-from-long-in-scala
    Try[Long](
      ip
        .split('.')
        .ensuring(_.length == 4)
        .map(_.toLong)
        .ensuring(_.forall(x => x >= 0 && x < 256))
        .reverse
        .zip(List(0, 8, 16, 24))
        .map(xi => xi._1 << xi._2)
        .sum
    ).toOption.getOrElse(0)
  }

  // todo delete
  def arrayToRange(start: Int, end: Int): List[Int] = {
    start to end toList
  }
}
