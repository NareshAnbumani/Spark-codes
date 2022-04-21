package org.inceptez.streaming
import org.apache.spark.SparkConf
import  org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Writable, IntWritable, Text}
import org.apache.hadoop.mapred.{TextOutputFormat, JobConf}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat


object kafkatweet {
  
  def main(args:Array[String])
  {
        val sparkConf = new SparkConf().setAppName("kafkatweets").setMaster("local[*]")
        val sparkcontext = new SparkContext(sparkConf)
        sparkcontext.setLogLevel("ERROR")
        val ssc = new StreamingContext(sparkcontext, Seconds(10))
        ssc.checkpoint("checkpointdir")
        val sqlcontext = new SQLContext(sparkcontext)
        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "localhost:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "nifikafkatopic",
          "auto.offset.reset" -> "latest"
          )

        val topics = Array("tkk1")
        val stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )

        
/* Twitter Api Key
*****************
SctND2bnuvjyTdEJmLucECYCo
Api secret
**********
lmypWKJFDvSPkPbSlvHdVzOaFbwM9pnyUJSPvVLi0erhEX3XeP

Access token & access token secret
**********************************
3568494252-KMumLSUsXTblS1VgyvutfmNBaNddgncx35Hyuv8
ckLmsDppgjY2yXI6JHvfjiUQy44RSZy8xcWEJ4b5o5jAO
*/
        
        val kafkastream = stream.map(record => (record.key, record.value))
        val inputStream = kafkastream.map(rec => rec._2);
//        inputStream.print()
        inputStream.foreachRDD(rdd =>
          {
            if(!rdd.isEmpty())
            {
               val df = sqlcontext.read.json(rdd)
              // df.printSchema()
               //val df1 = df.withColumn("NonHeap", col("systemDiagnostics.aggregateSnapshot.totalNonHeap"))
               /*val df2 = df.select(col("systemDiagnostics.aggregateSnapshot.totalNonHeap").alias("totalNonHeap"),
                   col("systemDiagnostics.aggregateSnapshot.totalNonHeapBytes").alias("totalNonHeapBytes"),
                   col("systemDiagnostics.aggregateSnapshot.usedNonHeap"))*/
                   df.printSchema()
                   //sdf.show(false)
                   df.select("id","coordinates","text","user.followers_count","entities.urls").show(false)
                   df.createOrReplaceTempView("tweets")
sqlcontext.sql("""SELECT place,max(favorite_count) as retweets
FROM tweets
GROUP BY  place
""").show
            }
          })
          ssc.start()
        ssc.awaitTermination()    

  }

 
}