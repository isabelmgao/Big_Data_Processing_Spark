# Big_Data_Processing_Spark
Processing Yelp's Public Dataset Using Spark and Fladoop


I computed the number of businesses, total review count, and average star rating for each neighborhood in each city within 
Yelp's Academic Dataset hosted in HDFS on the Fladoop cluster. If a business had multiple neighborhoods, its review count and 
stars were attributed to all of the neighborhoods. If the neighborhoods list was empty, then it defaulted to 'Unknown' as the 
name of the neighborhood.
