import json
import time
import sys
from operator import add
from pyspark import SparkContext


class spark_timing_analysis:
    """
    Class to get timing and partition details between default method and explict mention of partition.
    """

    def __init__(self, review_filepath, output_file_path, partition):
        self.sc = SparkContext("local[*]", "task2")
        self.sc.setLogLevel("WARN")
        self.review_filepath = review_filepath
        self.output_file_path = output_file_path
        self.partition = partition

    def default_method(self):
        """
        Function to return Time taken to execute,number of partition by default and partition content.
        """
        start_time = time.time()
        rdd_data = self.sc.textFile(self.review_filepath).map(
            lambda line: json.loads(line)
        )
        business_reviews = rdd_data.map(
            lambda line: (line["business_id"], 1)
        ).reduceByKey(add)
        top_10_business = business_reviews.sortBy(lambda x: (-x[1], x[0]))
        default_method_time = time.time() - start_time
        # print("Time: " + str(default_method_time))
        n_partition = top_10_business.getNumPartitions()
        n_items = top_10_business.mapPartitions(lambda x: [sum(1 for i in x)]).collect()
        # print("Partition: " + str(n_partition))
        # print("Partition Items: " + str(n_items))
        default = {}
        default["n_partition"] = n_partition
        default["n_items"] = n_items
        default["exe_time"] = default_method_time
        return default

    def parameter_method(self):
        start_time = time.time()

        def custom_partitioner(key):
            return ord(key[:1])

        rdd_data = self.sc.textFile(self.review_filepath).map(
            lambda line: json.loads(line)
        )
        business_reviews = (
            rdd_data.map(lambda line: (line["business_id"], 1))
            .partitionBy(self.partition, custom_partitioner)
            .reduceByKey(add)
        )

        top_10_business = business_reviews.sortBy(lambda x: (-x[1], x[0]))
        opt_method_time = time.time() - start_time
        n_partition_opt = top_10_business.getNumPartitions()
        n_items_opt = top_10_business.mapPartitions(
            lambda x: [sum(1 for i in x)]
        ).collect()
        customized = {}
        customized["n_partition"] = n_partition_opt
        customized["n_items"] = n_items_opt
        customized["exe_time"] = opt_method_time
        return customized

    def write_results(self, default_result, parameter_result):
        result = {}
        result["default"] = default_result
        result["customized"] = parameter_result
        with open(self.output_file_path, "w") as json_file:
            json.dump(result, json_file)


if __name__ == "__main__":
    review_filepath = sys.argv[1]
    output_file_path = sys.argv[2]
    n_partition = int(sys.argv[3])
    obj = spark_timing_analysis(review_filepath, output_file_path, n_partition)
    default_result = obj.default_method()
    parameter_result = obj.parameter_method()
    obj.write_results(default_result, parameter_result)
