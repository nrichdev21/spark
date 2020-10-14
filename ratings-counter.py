from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setAppName('ratings-counter.py')
sc = SparkContext(conf=conf)

movie_ratings = sc.textFile('C:/BigData/ml-100k/u.data')

def get_rating(line):
    return line.split()[2]

ratingsCount = movie_ratings.map(get_rating)
result = ratingsCount.countByValue()
sorted_results = collections.OrderedDict(sorted(result.items()))
for k, v in sorted_results.items():
    print(k, v)