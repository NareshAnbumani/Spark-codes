package org.inceptez.spark.core
import org.apache.spark.sql.SQLContext;;
import org.apache.spark._;
import org.apache.spark.sql.SparkSession

object allcore {
  def main(args:Array[String])
  {
    val sparkconf=new SparkConf().setAppName("sampleapp").setMaster("local[*]")
    val sc=new SparkContext(sparkconf);
    val sqlc=new SQLContext(sc);
    import sqlc.implicits._
    
val hadooplines= sc.textFile("hdfs://localhost:54310/user/hduser/empdata.txt")
val lines = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
val alllines = sc.textFile("file:/home/hduser/sparkdata/*data.txt")

val spark = SparkSession.builder().getOrCreate()
val hadooplines1 = spark.sparkContext.textFile("hdfs://localhost:54310/user/hduser/empdata.txt")
val lines1 = spark.sparkContext.textFile("file:/home/hduser/sparkdata/empdata.txt")
val alllines1 = spark.sparkContext.textFile("file:/home/hduser/sparkdata/*data.txt")
alllines.foreach(println)
hadooplines.foreach(println)





  }
}