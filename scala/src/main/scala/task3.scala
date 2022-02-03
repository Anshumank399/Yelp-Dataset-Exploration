import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import java.io._
import scala.collection.mutable._

object task3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val review_filepath = args(0)
    val business_filepath = args(1)
    val output_filepath_question_a = args(2)
    val output_filepath_question_b = args(3)

    //val review_filepath = "../data/review.json"
    //val business_filepath = "../data/business.json"
    //val output_filepath_question_a = "abc3_scala.txt"
    //val output_filepath_question_b = "xyz3_scala.json"

    val spark = new SparkConf().setAppName("task3").setMaster("local[*]")
    val sc = new SparkContext(spark)
    def extract_stars(line: String): Double = {
      val data = parse(line)
      val date = data \ "stars"
      val JDouble(res) = date
      res.toDouble
    }
    def extract_string(line: String, col_name : String): String = {
      val data = parse(line)
      val date = data \ col_name
      val JString(res) = date
      res
    }
    def reduce_func(first : (Int, Double), second: (Int, Double)):(Int, Double)={
      (first._1 + second._1, first._2 + second._2)
    }
    def map_func(x : (String, (Int, Double))): (Double, String)={
      (x._2._2/x._2._1, x._1)
    }
    val start_time_m = System.nanoTime
    val review_data = sc.textFile(path = review_filepath)
      .map(line => (extract_string(line, "business_id") , extract_stars(line)))
    val business_data = sc.textFile(path = business_filepath)
      .map(line => (extract_string(line, "business_id"), extract_string(line, "city")))
    val rdd_data = review_data.join(business_data)
    val city_rating_data = rdd_data.map(line => (line._2._2, (1, line._2._1)))
    //city_rating_data.foreach(println)
    val m_time = (System.nanoTime - start_time_m) / 1e9d

    val start_time_m1 = System.nanoTime
    val average_1 = city_rating_data.reduceByKey(reduce_func).map(x => map_func(x)).collect()
    val m1_data = average_1.sortBy(average_1 => (-average_1._1, average_1._2))
    val m1_data_top10 = m1_data.slice(0,10)
    val m1_time = (System.nanoTime - start_time_m1) / 1e9d
    //m1_data.foreach(println)

    val start_time_m2 = System.nanoTime
    def ftn_(line: (Double, List[String])): List[(String, Double)] = {
      val num = line._1
      val list_of_names = line._2
      var res: List[(String, Double)] = List()
      for (w <- 0 to list_of_names.size - 1) {
        res = res :+ ((list_of_names(w), num))
      }
      return res
    }
    val m2_data = city_rating_data
      .reduceByKey(reduce_func)
      .map(x => map_func(x))
      .takeOrdered(num=10)(Ordering[(Double, String)].on(x => (-x._1, x._2)))

    val m2_time = (System.nanoTime - start_time_m2) / 1e9d
    m2_data.foreach(println)
    //println("M1 Method: " + m1_time)
    //println("M2 Method: " + m2_time)
    val file = new File(output_filepath_question_a)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("city, stars"+"\n")
    for (w <- 0 to m1_data.size-1){
      bw.write(m1_data(w)._2.toString +','+ m1_data(w)._1.toString + "\n")
    }
    bw.close()

    val reason = ""
    val result = LinkedHashMap(
      "m1" -> (m1_time+m_time),
      "m2" -> (m2_time+m_time),
      "reason" -> reason)

    implicit val formats = org.json4s.DefaultFormats
    val writer = new PrintWriter(new File(output_filepath_question_b))
    writer.write(Serialization.write(result))
    writer.close()
  }
}
