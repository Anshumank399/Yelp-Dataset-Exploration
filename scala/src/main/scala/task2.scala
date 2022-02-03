import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import java.io._
import scala.collection.mutable._


object task2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //val review_filepath = "../data/review.json"
    //val output_filepath = "abc.json"
    //val n_partition = 4

    val review_filepath = args(0)
    val output_filepath = args(1)
    val n_partition = args(2)

    val spark = new SparkConf().setAppName("task2").setMaster("local[*]")
    val sc = new SparkContext(spark)

    def get_id(x: JValue): (String, Int) = {
      val user_id = compact(render(x \ "business_id"))
      (user_id.substring(1, user_id.length - 1), 1)
    }

    val start_time_m = System.nanoTime
    val rdd_data = sc.textFile(path = review_filepath).map(line => parse(line))
    val m_time = (System.nanoTime - start_time_m) / 1e9d

    val start_time_m2 = System.nanoTime
    val temp = rdd_data.map(get_id).reduceByKey(_ + _)
    val top10_business_m1 = temp.takeOrdered(num = 10)(Ordering[(Int, String)].on(x => (-x._2, x._1)))
    val m1_time = (System.nanoTime - start_time_m2) / 1e9d
    //println("Time: ", m1_time)
    val n_partition_m1 = temp.getNumPartitions
    val n_items_m1 = temp.mapPartitions(iter => Iterator(iter.size), true).collect()


    val start_time = System.nanoTime
    val temp_m2 = rdd_data.map(get_id).partitionBy(new HashPartitioner(n_partition.toInt)).reduceByKey(_ + _)
    val top10_business_m2 = temp_m2.takeOrdered(num = 10)(Ordering[(Int, String)].on(x => (-x._2, x._1)))
    val m2_time = (System.nanoTime - start_time) / 1e9d
    //println("Time: ", m2_time)
    val n_partition_m2 = temp_m2.getNumPartitions
    val n_items_m2 = temp_m2.mapPartitions(iter => Iterator(iter.size), true).collect()

    val default = LinkedHashMap(
      "n_partition" -> n_partition_m1,
      "n_items" -> n_items_m1,
      "exe_time" -> (m1_time+m_time))
    //println(default)

    val parameter = LinkedHashMap(
      "n_partition" -> n_partition_m2,
      "n_items" -> n_items_m2,
      "exe_time" -> (m2_time+m_time))
    //println(parameter)

    val result = LinkedHashMap(
      "default" -> default,
      "customized" -> parameter)
    //println (result)

    implicit val formats = org.json4s.DefaultFormats
    val writer = new PrintWriter(new File(output_filepath))
    writer.write(Serialization.write(result))
    writer.close()
  }
}