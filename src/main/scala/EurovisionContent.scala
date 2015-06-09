import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{DataFrame, SQLContext}

case class EuroData(val fromCountry: String, val toCountry: String, val juryA: Int, val juryB: Int, val juryC: Int,
  val juryD: Int, val juryE: Int, val rank: Int, val televote: Int, val combined: Int, val points: Int)

object EurovisionContent  {

def winner:Unit= {
  val inputFile = "ESC-2015-grand_final-full_results.csv"
  val sc = new SparkContext("local", "Simple App")
  // remove empty lines
  val input = sc.textFile(inputFile).filter(line => line.isEmpty == false)
  val result = input.map { line =>
    val reader = new CSVReader(new StringReader(line));
    reader.readNext();
  }


  val fulData = result.filter(x => x.size == 11)
  val euData = fulData.map(x => EuroData(x(0), x(1), x(2).toInt, x(3).toInt, x(4).toInt, x(5).toInt, x(6).toInt, x(7).toInt, x(8).toInt, x(9).toInt, x(10).toInt))

  // create sql RDD
  // get all country pairs who gave 12 to another country
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  import sqlContext.sql
  // Convenient for running SQL queries.
  // Create a DataFrame and register as a temporary "table".
  val countries = sqlContext.createDataFrame(euData)
  countries.registerTempTable("votes")
  countries.cache()
  // print the 1st 20 lines (Use dump(verses), defined above, for more lines)
  val highestRank = sql("SELECT toCountry,sum(points) points FROM votes Group by toCountry order by points desc")
  highestRank.collect().map(println(_))

  // use graph to get hihest ranking country
}

  def winnerGraphX: Unit= {
    val inputFile = "ESC-2015-grand_final-full_results.csv"
    val sc = new SparkContext("local", "Simple App")
    // remove empty lines
    val input = sc.textFile(inputFile).filter(line => line.isEmpty == false)
    val result = input.map { line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    }


    val fulData = result.filter(x => x.size == 11)
    val euData = fulData.map(x => EuroData(x(0), x(1), x(2).toInt, x(3).toInt, x(4).toInt, x(5).toInt, x(6).toInt, x(7).toInt, x(8).toInt, x(9).toInt, x(10).toInt))


  }

  def main(args: Array[String]) {
    winner
  }

}
