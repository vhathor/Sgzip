package spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.{ SparkContext, SparkConf }

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import org.apache.spark.sql.hive._

object Sgzip {

  def main(args: Array[String]) {

    try {

      //Parameters
      val sourceDirectoryParam = args(0)
      val targetDirectoryParam = args(1)
      val numberOfPartitionsParam = args(2)

      //Spark Context and prepare variables
      val sparkConf = new SparkConf().setAppName("sgzip")
      val sparkContext = new SparkContext(sparkConf)
      val sourceDirectoryHDFS = sparkContext.textFile(sourceDirectoryParam)
      val targetDirectoryHDFS = targetDirectoryParam
      val numberOfPartitions = numberOfPartitionsParam.toInt

          //Compress Source files and place them in target directory
      sourceDirectoryHDFS.coalesce(numberOfPartitions, false).saveAsTextFile(targetDirectoryHDFS, classOf[org.apache.hadoop.io.compress.GzipCodec])

      println("Sgzip -> Compression completed sucessfully, check HDFS location " + targetDirectoryHDFS)

    } catch {
      case ex: Exception => {
        //Exception, print stack and exit with error code
        println(ex.printStackTrace())
        System.exit(-1)
      }
    }

    //Success, Exit gracefully now
    System.exit(0)
  }

}
