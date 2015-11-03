/**************************************************************/
/** NAME: Prashant Prakash                                   **/
/** NET ID: pxp141730                                        **/
/** PURPOSE: Big Data Management and Analytics Assignment - 4**/
/** CLASS: CS6350.001 										 **/
/** Description: KMeans Clustering with 10 iterations        **/
/**************************************************************/


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.math._
import java.io._
import scala.util.control._
import java.util.Properties

object KMeans {
	class Point(xt:Double, yt:Double) extends Serializable{
        	var x: Double = xt
        	var y: Double = yt


        	def distance(other: Point) : Double = sqrt(pow(x - other.x, 2) + pow(y - other.y, 2))

        	def getX() : Double = x

        	def getY() : Double = y

        	override def toString(): String = "(" + x + ", " + y + ")"

  	}
	def getPoint(p: String) : Point = {
	return new Point(p.split(" ")(0).toDouble,p.split(" ")(1).toDouble)
	}
	
	/*
		Function to get the ClusterIndex for a given point 
	*/
	def closestClusterIndex(p: Point, centers: Array[Point]) : Int = {
        var clusterIndex = 0
		var closest = Double.PositiveInfinity
		for (i <- 0 until centers.length) {
			val tempDist = p.distance(centers(i))
			if (tempDist < closest) {
				closest = tempDist
				clusterIndex = i
			}
		}

		return clusterIndex
  	}
	
	/*
		Function to calculate Average for all the points and to get the 
		new centroids
	*/
	def average(pi: Iterable[Point]): Point = {
	
		var averageX = 0.0
		var averageY = 0.0

		var points:Array[Point] = pi.toArray

		for(i<-0 until points.length) {
			averageX+=points(i).getX()
			averageY+=points(i).getY()
		}

		averageX=averageX/points.size
		averageY = averageY/points.size
		return getPoint(averageX.toString+" "+averageY.toString)
  	}
	
	/*
	 Main Function  Entry point for Scala Program	 
	*/
	
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("KMeans Clustering")
        	conf.setMaster("local[2]")
        val sc = new SparkContext(conf)
		val k = 3;
        val pointRDD = sc.textFile("E:\\BigData\\datasethw4\\dataset\\Q1_testkmean.txt")
		val points = pointRDD.map(point=>getPoint(point))
		
		var centroids = pointRDD.takeSample(false,k,8).map(point=>getPoint(point))
		
		for(iter <- 1 to 10) {
         println("\n Iteration " + iter)
		 // this closest will have information of all points with the clusterInfo
		 var closest = points.map(p=>(closestClusterIndex(p, centroids),p))
		 val clusters = closest.groupByKey()
		 var newCentroids = clusters.map(pts=>average(pts._2)).toArray
		 // changing Centroids taking average every time
		 centroids = newCentroids
		 if(iter==10){
					closest.saveAsTextFile("outputKmeans")
					println("-------------printing Final Centroids-------------------")
					centroids.foreach(println)					
			}
		 
		}
		
		
		
		
	}
}