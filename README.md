# Yelp-Dataset-Exploration
Project to get started with spark using Python and Scala. 

## Dataset:
Yelp.com website and the Yelp mobile app, which publish crowd-sourced reviews about businesses.  
[Dataset](https://www.yelp.com/dataset) used in this project.

The goal of these tasks is to get you familiar with Spark operation types (e.g., transformations and actions).

## Tasks:  
__Task 1: Data Exploration__.   
* The total number of reviews.  
* The number of reviews in 2018.  
* The number of distinct users who wrote reviews.  
* The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote.  
* The number of distinct businesses that have been reviewed.  
* The top 10 businesses that had the largest numbers of reviews and the number of reviews they had.  

__Task 2: Partition__.   
* In this task, show the number of partitions for the RDD used for Task 1 Question F and the number of items per partition.  
* Use a customized partition function to improve the performance of map and reduce tasks. A time duration (for executing Task 1 Question F) comparison between the default partition and the customized partition (RDD built using the partition function).  

__Task 3: Exploration on Multiple Datasets__.  
* Explore two datasets together containing review information (test_review.json) and business information (business.json). What are the average stars for each city?   
* Compare the execution time of using two methods to print top 10 cities with highest stars.   
   Method 1: Collect all the data, sort in python/scala, and then print the first 10 cities.  
   Method 2: Sort in Spark, take the first 10 cities, and then print these 10 cities.  

## Fork the repo and practice more !
