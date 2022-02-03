import sys
import json
from operator import add
from pyspark import SparkContext


class explore:
    def __init__(self, review_filepath, output_filepath):
        self.review_filepath = review_filepath
        self.output_filepath = output_filepath
        self.sc = SparkContext("local", "task1").getOrCreate()
        self.sc.setLogLevel("WARN")

    def read_data(self):
        rdd_data = self.sc.textFile(review_filepath)
        return rdd_data.map(lambda line: json.loads(line))

    def eda(self, rdd_data):
        # Task 1.1: Count number of Reviews
        number_of_reviews = rdd_data.map(lambda line: (line["review_id"], 1)).count()
        # Task 1.2: Count number of reviews in year 2018
        year = 2018
        number_of_reviews_2018 = (
            rdd_data.map(
                lambda line: (line["review_id"], int(line["date"].split("-")[0]))
            )
            .filter(lambda line: line[1] == year)
            .count()
        )
        # Task 1.3: Number of Distinct users who wrote reviews.
        distinct_users = (
            rdd_data.map(lambda line: (line["user_id"], 1)).distinct().count()
        )
        # Task 1.4: Top 10 users with count of reviews.
        reviews_by_user = rdd_data.map(lambda line: (line["user_id"], 1)).reduceByKey(
            add
        )
        top_10_users = reviews_by_user.sortBy(lambda x: (-x[1], x[0])).take(10)
        # Task 1.5: Count of distinct Business.
        distinct_business = (
            rdd_data.map(lambda line: (line["business_id"], 1)).distinct().count()
        )
        # Task 1.6: Top 10 Business with reviews.
        business_reviews = rdd_data.map(
            lambda line: (line["business_id"], 1)
        ).reduceByKey(add)
        top_10_business = business_reviews.sortBy(lambda x: (-x[1], x[0])).take(10)

        output_dict = {
            "n_review": number_of_reviews,
            "n_review_2018": number_of_reviews_2018,
            "n_user": distinct_users,
            "top10_user": top_10_users,
            "n_business": distinct_business,
            "top10_business": top_10_business,
        }
        return output_dict

    def write_output(self, result):
        f = open(self.output_filepath, "w")
        json.dump(result, f)
        f.close()


if __name__ == "__main__":
    review_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    obj = explore(review_filepath, output_filepath)
    rdd_data = obj.read_data()
    result = obj.eda(rdd_data)
    obj.write_output(result)
