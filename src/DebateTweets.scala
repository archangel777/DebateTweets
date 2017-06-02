import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.{DateTime, DateTimeConstants}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Created by ErickLima on 01/06/2017.
  */

object MyUtils {
	val fmt: DateTimeFormatter = DateTimeFormat forPattern "EEE MMM dd HH:mm:ss Z yyyy" withLocale Locale.ENGLISH
}

case class Summary(day: Int, hour:Int, freq:Int)

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

		//df.select("_c7").take(5).foreach
		val tweetsWithValidDates = df.rdd.filter(x => x.get(1) != null && x.get(7) != null && x.getString(7).split(" ").length > 1)

		val toRemove = "\"".toSet

		import MyUtils._
		val hashTagsWithTime = tweetsWithValidDates
			.flatMap(x => x
				.getString(1).split(" ")
				.filter(s => s.startsWith("#"))
				.map(s => (fmt parseDateTime x.getString(7), s)))
			.map(s => (s._1, s._2.filterNot(toRemove)))

		//hashTagsAlongTheDay(hashTagsWithTime)
		//hashTagsPerDay(hashTagsWithTime)
		//tweetsPerHour(tweetsWithValidDates, spark)
		topSentencesWithWord(tweetsWithValidDates, "Dilma")
		topSentencesWithWord(tweetsWithValidDates, "Aécio")
	}

	def topSentencesWithWord(tweets: RDD[Row], word: String): Unit = {
		val withDilma = tweets
			.map(x => x.getString(1))
			.filter(x => x.toLowerCase.contains(word.toLowerCase))
			.map(x => (x, 1))
			.reduceByKey(_+_)
			.sortBy(x => x._2, ascending = false)
			.take(k)
		println("Top sentences with " + word + ":")
		withDilma.foreach(x => println("\t" + x))
		println()
	}

	def tweetsPerHour(tweets: RDD[Row], sparkSession: SparkSession): Unit = {
		import MyUtils._
		val tweetsHourDay = tweets
			.map(x => fmt parseDateTime x.getString(7))
			.map(x => ((x.getDayOfMonth, x.getHourOfDay), 1))
			.reduceByKey(_+_)
			.map(x => Summary(x._1._1, x._1._2, x._2))
		import sparkSession.sqlContext.implicits._
		import org.apache.spark.sql.functions._
		val tweetsDatesDF = tweetsHourDay.toDF("day", "hour", "num")
		tweetsDatesDF
			.groupBy("day")
			.agg(avg("num") as "avgTweets")
			.drop("hour", "num")
			.withColumnRenamed("num", "avgTweets")
			.rdd.map(x => (x.getAs[Int]("day"), x.getAs[Double]("avgTweets")))
			.sortBy(x => x._1)
			.collect()
			.foreach(x => print("\n" + x._1 + "/10 -> Average Tweets per Hour: %.2f".format(x._2)))
		println()
	}

	def getPeriod(values: RDD[(DateTime, String)], period: String): Array[(String, Int)] = {
		val hourLimits = period match {
			case "morning" => (6, 12)
			case "afternoon" => (12, 18)
			case "evening" => (18, 24)
		}
		values.filter(row => row._1.getHourOfDay >= hourLimits._1 && row._1.getHourOfDay < hourLimits._2)
			.map(row => (row._2, 1))
			.reduceByKey(_+_)
			.sortBy(x => x._2, ascending = false)
			.take(k)
	}

	def hashTagsAlongTheDay(hasTagsWithTime: RDD[(DateTime, String)]): Unit = {

		val morning = getPeriod(hasTagsWithTime, "morning")
		val afternoon = getPeriod(hasTagsWithTime, "afternoon")
		val evening = getPeriod(hasTagsWithTime, "evening")

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

	def getDayOfWeekTops(values: RDD[((String, String), Int)], dayOfWeek: String): Array[((String, String), Int)] = {
		values.filter(x => x._1._1 == dayOfWeek)
			.sortBy(x => x._2, ascending = false)
			.take(k/2)
	}

	def hashTagsPerDay(hasTagsWithTime: RDD[(DateTime, String)]): Unit = {

		def mapping(x: Int) = x match {
			case DateTimeConstants.SUNDAY => "Sunday"
			case DateTimeConstants.MONDAY => "Monday"
			case DateTimeConstants.TUESDAY => "Tuesday"
			case DateTimeConstants.WEDNESDAY => "Wednesday"
			case DateTimeConstants.THURSDAY => "Thursday"
			case DateTimeConstants.FRIDAY => "Friday"
			case DateTimeConstants.SATURDAY => "Saturday"
		}

		val processed = hasTagsWithTime
			.map(x => ((mapping(x._1.getDayOfWeek), x._2), 1))
			.reduceByKey(_+_)

		val wednesday = getDayOfWeekTops(processed, "Wednesday")
		val thursday = getDayOfWeekTops(processed, "Thursday")
		val friday = getDayOfWeekTops(processed, "Friday")
		val saturday = getDayOfWeekTops(processed, "Saturday")
		val sunday = getDayOfWeekTops(processed, "Sunday")
		val monday = getDayOfWeekTops(processed, "Monday")

		println("Wednesday (15/10): \n")
		wednesday.foreach(x => println("\t" + x))
		println("======================================")
		println("Thursday (16/10): \n")
		thursday.foreach(x => println("\t" + x))
		println("======================================")
		println("Friday (17/10): \n")
		friday.foreach(x => println("\t" + x))
		println("======================================")
		println("Saturday (18/10): \n")
		saturday.foreach(x => println("\t" + x))
		println("======================================")
		println("Sunday (19/10): \n")
		sunday.foreach(x => println("\t" + x))
		println("======================================")
		println("Monday (20/10): \n")
		monday.foreach(x => println("\t" + x))
		println("======================================")
	}



}
