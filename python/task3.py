import time
import json
import sys
from pyspark import SparkContext


class explore:
    def __init__(
        self,
        review_filepath,
        business_filepath,
        output_filepath_question_a,
        output_filepath_question_b,
    ):
        self.sc = SparkContext("local[*]", "task3")
        self.sc.setLogLevel("WARN")
        self.review_filepath = review_filepath
        self.business_filepath = business_filepath
        self.output_filepath_question_a = output_filepath_question_a
        self.output_filepath_question_b = output_filepath_question_b

    def data_preprocess(self):
        review_data = self.sc.textFile(review_filepath).map(
            lambda line: (json.loads(line)["business_id"], (json.loads(line)["stars"]))
        )
        business_data = self.sc.textFile(business_filepath).map(
            lambda line: (json.loads(line)["business_id"], (json.loads(line)["city"]))
        )
        data_joined = review_data.join(business_data)
        city_rating_data = data_joined.map(lambda line: (line[1][1], line[1][0]))
        return city_rating_data

    def avg_city_rating_spark_sort(self, city_rating_data):
        avg_by_key = (
            city_rating_data.mapValues(lambda v: (v, 1))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
            .mapValues(lambda v: v[0] / v[1])
            .sortBy(lambda x: (-x[1], x[0]))
            .collectAsMap()
        )
        # print(avg_by_key)
        return avg_by_key

    def avg_city_rating_python_sort(self, city_rating_data):
        avg_by_key = (
            city_rating_data.mapValues(lambda v: (v, 1))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
            .mapValues(lambda v: v[0] / v[1])
            .collectAsMap()
        )
        avg_by_key = sorted(avg_by_key.items(), key=lambda x: (-int(x[1]), x[0]))
        # print(avg_by_key)
        return avg_by_key

    def write_results(self, results, m1_time, m2_time, reason):
        f = open(self.output_filepath_question_a, "w")
        f.write("city,star\n")
        for key, value in results.items():
            f.write("{},{}\n".format(key, value))
        f.close
        answer = {"m1": m1_time, "m2": m2_time, "reason": reason}
        with open(self.output_filepath_question_b, "w") as json_file:
            json.dump(answer, json_file)


if __name__ == "__main__":
    review_filepath = sys.argv[1]
    business_filepath = sys.argv[2]
    output_filepath_question_a = sys.argv[3]
    output_filepath_question_b = sys.argv[4]
    obj = explore(
        review_filepath,
        business_filepath,
        output_filepath_question_a,
        output_filepath_question_b,
    )
    start_time = time.time()
    city_rating_data = obj.data_preprocess()
    m_time = time.time() - start_time
    start_time = time.time()
    result1 = obj.avg_city_rating_python_sort(city_rating_data)
    m1_time = time.time() - start_time
    start_time = time.time()
    result2 = obj.avg_city_rating_spark_sort(city_rating_data)
    m2_time = time.time() - start_time
    if dict(result1) == result2:
        reason = ""
        obj.write_results(result2, m1_time + m_time, m2_time + m_time, reason)

