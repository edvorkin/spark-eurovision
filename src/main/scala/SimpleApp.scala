import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame


case class EuroData(val fromCountry:String,	val toCountry:String,	val juryA:Int, val juryB:Int, val	juryC:Int,
                    val juryD:Int,val juryE:Int, val rank:Int, val televote:Int,	val combined:Int, val points:Int)


object SimpleApp extends App {


    val inputFile = "ESC-2015-grand_final-full_results.csv"
    val sc = new SparkContext("local", "Simple App")
  val input = sc.textFile(inputFile)
   val result = input.map{ line =>
    val reader = new CSVReader(new StringReader(line));
    reader.readNext();
  }
    val euData =  result.map(x => EuroData(x(0), x(1), x(2).toInt, x(3).toInt, x(4).toInt, x(5).toInt,x(6).toInt,x(7).toInt,x(8).toInt,x(9).toInt,x(10).toInt))


    println(euData.count)



}
