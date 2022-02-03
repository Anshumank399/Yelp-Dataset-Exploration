import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger, Level}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io._
import org.json4s.jackson.Serialization
import scala.collection.mutable._

object task1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //val review_filepath = args(0)
    //val output_filepath = args(1)
    val review_filepath = "../data/review.json"
    val output_filepath = "abc.json"

    val spark = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(spark)
    val rdd_data = sc.textFile(path = review_filepath)
    val rdd_data_parsed = rdd_data.map(line => parse(line))
    // Task 1.1 Count Total records
    val n_review = rdd_data.count()
    //println("Total Review Count : " + n_review)

    val n_review_2018 = rdd_data_parsed.filter(review => compact(render(review \ "date"))regionMatches(false, 1, "2018", 0, 4)).count()
    //println("Total Review Count 2018 : " + n_review_2018)

    val distinct_users = rdd_data_parsed.map(review => compact(render(review \ "user_id"))).distinct().count()
    //println("Total distinct users : " + distinct_users)

    val distinct_business = rdd_data_parsed.map(review => compact(render(review \ "business_id"))).distinct().count()
    //println("Total distinct business : " + distinct_business)

    def get_id(x: JValue, by: String): (String, Int) = {
      if (by == "user"){
      val user_id = compact(render(x \ "user_id"))
      (user_id.substring(1, user_id.length - 1), 1)
      }
      else{
        val user_id = compact(render(x \ "business_id"))
        (user_id.substring(1, user_id.length - 1), 1)
      }
    }
    val top10_user = rdd_data_parsed.map(get_id(_,by = "user")).reduceByKey(_+_).takeOrdered(num=10)(Ordering[(Int, String)].on(x => (-x._2, x._1)))
    val top10_business = rdd_data_parsed.map(get_id(_,by = "business")).reduceByKey(_+_).takeOrdered(num=10)(Ordering[(Int, String)].on(x => (-x._2, x._1)))

    val top10_user_list:Array[List[Any]] = new Array(top10_user.length)
    for (i <- 0 until top10_user.length) {
      top10_user_list(i) = top10_user(i).productIterator.toList
    }
    val top10_business_list:Array[List[Any]] = new Array(top10_business.length)
    for (i <- 0 until top10_business.length) {
      top10_business_list(i) = top10_business(i).productIterator.toList
    }
    //println(top10_business_list)

    val final_ans = LinkedHashMap(
      "n_review" -> n_review,
      "n_review_2018" -> n_review_2018,
      "n_user" -> distinct_users,
      "top10_user" -> top10_user_list,
      "n_business" -> distinct_business,
      "top10_business" -> top10_business_list)
    //println(final_ans)

    implicit val formats = org.json4s.DefaultFormats
    val writer = new PrintWriter(new File(output_filepath))
    writer.write(Serialization.write(final_ans))
    writer.close()
  }
}