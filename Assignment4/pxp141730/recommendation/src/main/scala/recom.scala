/**************************************************************/
/** NAME: Prashant Prakash                                   **/
/** NET ID: pxp141730                                        **/
/** PURPOSE: Big Data Management and Analytics Assignment - 4**/
/** CLASS: CS6350.001 										 **/
/** Description: Recommendation based on item Similarity     **/
/**************************************************************/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.math._
import java.io._
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set
import scala.util.control._
import java.util.Properties

object recom {

	/*
		Function to parse rating file and get only movieID 
	*/
	def parseRating(line: String) : Int = {
		val x = line.split("::")
		val userid=x(0).toInt
		val movieid = x(1).toInt
		val rating =x(2).toDouble
		
		return movieid
		
	
	}
	
	/*
		Main Function 
	*/

	def main(args: Array[String]) {
			val conf = new SparkConf().setAppName("Recommendation")
        	conf.setMaster("local[4]")
        	val sc = new SparkContext(conf)
			val ratings = sc.textFile("E:\\BigData\\datasethw4\\dataset\\ratings.dat")
			val itemsim = sc.textFile("E:\\BigData\\datasethw4\\dataset\\itemsim")
			val movies = sc.textFile("E:\\BigData\\datasethw4\\dataset\\movies.dat")
			val userid=20
			/* Filter ratings based on User and Rating */
			val filterRRD = ratings.filter(x => x.split("::")(0).toInt==userid && x.split("::")(2).toDouble==3.0).map(x=>parseRating(x))
			val movieRDD = movies.map(x=>(x.split("::")(0).toInt,x.split("::")(1)))
			val filterset = filterRRD.collect.toSet
			/* Get those items from similarity matrix which is there in the filtered set*/
			val itemsimRDD = itemsim.filter(x=>filterset.contains(x.split("\t")(0).toInt))
			val movieMap = movieRDD.collect.toMap
			itemsimRDD.foreach(a=> {
				if(movieMap.contains(a.split("\t")(0).toInt)){
					println(a.split("\t")(0).toInt + ":" +movieMap.get(a.split("\t")(0).toInt))
					a.split("\t")(1).split(" ").foreach( b=> { 
						if(movieMap.contains(b.split(":")(0).toInt)) {
							print(b.split(":")(0) +":"+ movieMap.get(b.split(":")(0).toInt)+",")
						}
					})
					println("-----------------------------------------------------------")
					
				}
			})
			/*val itemsimRDD = itemsim.map(x=>x)
			itemsimRDD.foreach( a=> {
				println(a.split("\t")(0))
			})
			println(itemsimRDD.count)*/
			
	}
}