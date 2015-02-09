package spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql

import org.apache.spark.{ SparkContext, SparkConf }

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import org.apache.spark.sql.hive._

//spark-submit  --master yarn-cluster --num-executors 3 --executor-cores 1 --executor-memory 512M --class com.sony.ps.App /mnt/hdfs/user/vverma/sgzip-0.1.jar '/user/vverma/sd' '/user/vverma/sdout'
//spark-submit  --master local  --num-executors 1 --executor-cores 1 --executor-memory 512M --class com.sony.ps.App /mnt/hdfs/user/vverma/sgzip-0.1.jar '/user/vverma/sd' '/user/vverma/sdout'


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

      // Delete target directory if exists

      /*
      val path = new Path(targetDirectoryParam)
      val conf = new Configuration()
      val hdfsCoreSitePath = new Path("core-site.xml")
      val hdfsHDFSSitePath = new Path("hdfs-site.xml")
      conf.addResource(hdfsCoreSitePath)
      val fileSystem = FileSystem.get(conf)
      fileSystem.delete(path, true) //true recursive

       */
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
