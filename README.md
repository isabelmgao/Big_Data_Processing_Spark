# Big_Data_Processing_Spark
Big data processing on Hadoop: MapReduce and Spark


In this project, I processed Yelp's public dataset using Spark and Fladoop. Filtering only Yelp's business object, this program reads in Yelp's JSON objects to be processed using map reduce. The program computes the number of businesses, total review count, and average star rating for each neighborhood in each city within Yelp's Academic Dataset hosted in HDFS on the Fladoop cluster. Review count and stars get attributed to all neighborhoods in the case of businesses with multiple neighborhood listings. If the neighborhoods list is empty, then it the name of the neighborhood is defaulted to 'Unknown'.
