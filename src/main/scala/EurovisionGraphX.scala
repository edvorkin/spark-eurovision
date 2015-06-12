/**
 * Created by edvorkin on 6/11/15.
 */

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object EurovisionGraphX {

def topRanked() : Unit={
  val inputFile = "ESC-2015-grand_final-full_results.csv"
  val sc = new SparkContext("local", "Simple App")
  // remove empty lines
  val input = sc.textFile(inputFile).filter(line => line.isEmpty == false)
  val result = input.map { line =>
    val reader = new CSVReader(new StringReader(line));
    reader.readNext();
    }
    .filter(x => x.size == 11)

  val votes:RDD[Edge[String]] =result.map{ x=> (Edge(x(0).hashCode.toLong, x(1).hashCode.toLong, x(10)))}

  val countries:RDD[(VertexId, (String))] = result.map(line=>(line(0).hashCode.toLong,(line(0)))).distinct()
  //println (countries.collect().mkString("\n"))
  //println (votes.collect().mkString("\n"))
  val graph = Graph(countries, votes)

  val cc = graph.connectedComponents().vertices

  val ccByCountry = countries.join(cc).map {
    case (id, (country, cc)) => (country, cc)
  }
  println ("connected components")
  println(ccByCountry.collect().mkString("\n"))

  //println(graph.vertices.count())
  //graph.edges.filter { case Edge(src, dst, vote) => vote.toInt == 12 }.foreach(println(_))
}

  def main(args: Array[String]) {
    topRanked()
  }
}
