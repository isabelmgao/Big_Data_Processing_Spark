'''
To run on Fladoop cluster

spark-submit --master yarn-client --queue si601w16 --num-executors 2 --executor-memory 1g --executor-cores 2 si601_w16_hw5_imgao.py si601w16hw5_output_imgao
'''
import sys, re
import simplejson as json
from pyspark import SparkContext
sc = SparkContext(appName="AvgStars")

input_file = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset.json")

def organize_data(data):
  business_list = []
  business_dict = {}
  star_rating = data.get('stars', None)
  business_type = data.get('type', None)
  review_count = data.get('review_count', None)
  neighborhoods = data.get('neighborhoods', None)
  city = data.get('city', None)
  if business_type == 'business':
      if neighborhoods:
          for n in neighborhoods:
              neighborhood = n
              business_list.append(((city, neighborhood), (review_count, star_rating)))
      else:
          neighborhood = 'Unknown'
          business_list.append(((city, neighborhood), (review_count, star_rating)))
  return business_list

outputdir = sys.argv[1]
output_data = input_file.map(lambda line: json.loads(line)) \
                .flatMap(organize_data) \
                .mapValues(lambda x: (x[0], x[1], 1)) \
                .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
                .map(lambda x: (x[0], x[1][2], x[1][0], x[1][1]/x[1][2]))

            #COMMENTS----------------------------------------------------------------------------------------
            # 1. One line of input_file is: : {u'city': u'Phoenix', u'review_count': 9, u'name': u'Eric Goldberg, MD', u'neighborhoods': [], u'open': True, u'business_id': u'vcNAWiLM4dR7D2nwwJ7nCA', u'full_address': u'4840 E Indian School Rd\nSte 101\nPhoenix, AZ 85018', u'hours': {u'Tuesday': {u'close': u'17:00', u'open': u'08:00'}, u'Friday': {u'close': u'17:00', u'open': u'08:00'}, u'Monday': {u'close': u'17:00', u'open': u'08:00'}, u'Wednesday': {u'close': u'17:00', u'open': u'08:00'}, u'Thursday': {u'close': u'17:00', u'open': u'08:00'}}, u'state': u'AZ', u'longitude': -111.98375799999999, u'stars': 3.5, u'latitude': 33.499313000000001, u'attributes': {u'By Appointment Only': True}, u'type': u'business', u'categories': [u'Doctors', u'Health & Medical']}


            #2. Output of .flatMap() is: ((u'Phoenix', 'Unknown'), (9, 3.5))
            # .flatMap(organize_data) \


            # 3. MapValues applies lambda x function to each value of a pair RDD without changing the key.
            # In practice, for every dict from business_list, it is adding the count element to the value
            # Output of .mapValues is: ((u'Phoenix', 'Unknown'), (9, 3.5, 1))
            # .mapValues(lambda x: (x[0], x[1], 1)) \
            #
            # 3. reduceByKey combines values with the same key.
            # In practice, for every value, this is summing the review_count, star_rating, and count (:from .mapValues function)
            # Output of reduceByKey is: ((u'Madison', u'Willy St'), (1070, 99.5, 26))
            # .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
            #
            # 4. The difference between .map and .mapValues is that .map takes a tuple as parameter, whereas .mapValues takes a list
            #  x[0] is a key, x[1] is the tuple
            #  Output of .map is ((u'Madison', u'Willy St'), 26, 1070, 3.8269230769230771)
            # .map(lambda x: (x[0], x[1][2], x[1][0], x[1][1]/x[1][2]))

            # END COMMENTS----------------------------------------------------------------------------------------
output_data_sorted = output_data.sortBy(lambda x: (x[0][0], -x[1], -x[2]))
output_data_sorted.map(lambda t : t[0][0] + '\t' + t[0][1] + '\t' + str(t[1]) + '\t' + str(t[2]) + '\t' + str(t[3])).saveAsTextFile(outputdir)
#if you are running this in the terminal interpreter using pyspark, then to see the output file, check hadoop: hadoop fs -ls
