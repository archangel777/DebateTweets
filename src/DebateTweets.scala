import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by ErickLima on 01/06/2017.
  */

object MyUtils {
	val fmt = DateTimeFormat forPattern "EEE MMM dd HH:mm:ss Z yyyy" withLocale Locale.ENGLISH
}

object DebateTweets {

	val k = 10

	def main(args: Array[String]): Unit = {

		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
		val spark = SparkSession
			.builder()
			.config(conf)
			.getOrCreate()

		val df = spark.sqlContext.read
			.format("com.databricks.spark.csv")
			.option("delimiter","\t")
			.load("Dataset/debate-tweets.tsv")

		//df.select("_c7").take(5).foreach(println)

		val toRemove = "\"".toSet

		import MyUtils._
		val hashTagsWithTime = df.rdd
			.filter(x => x.get(1) != null && x.get(7) != null && x.getString(7).split(" ").length > 1)
			.flatMap(x => x
				.getString(1).split(" ")
				.filter(s => s.startsWith("#"))
				.map(s => (fmt parseDateTime x.getString(7), s)))
			.map(s => (s._1, s._2.filterNot(toRemove)))

		hashTagsAlongTheDay(hashTagsWithTime)
	}

	def hashTagsAlongTheDay(hasTagsWithTime: RDD[(DateTime, String)]): Unit = {
		val morning = hasTagsWithTime
			.filter(x => x._1.getHourOfDay >= 6 && x._1.getHourOfDay <= 12)
			.map(x => (x._2, 1))
			.reduceByKey(_+_)
			.sortBy(x => x._2, ascending = false)
			.take(k)
		val afternoon = hasTagsWithTime
			.filter(x => x._1.getHourOfDay >= 12 && x._1.getHourOfDay <= 18)
			.map(x => (x._2, 1))
			.reduceByKey(_+_)
			.sortBy(x => x._2, ascending = false)
			.take(k)
		val evening = hasTagsWithTime
			.filter(x => x._1.getHourOfDay >= 18 && x._1.getHourOfDay <= 24)
			.map(x => (x._2, 1))
			.reduceByKey(_+_)
			.sortBy(x => x._2, ascending = false)
			.take(k)

		println("Morning: \n")
		morning.foreach(println)
		println("======================================")
		println("Afternoon: \n")
		afternoon.foreach(println)
		println("======================================")
		println("Evening: \n")
		evening.foreach(println)
		println("======================================")

	}
}
