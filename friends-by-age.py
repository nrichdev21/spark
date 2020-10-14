""" Friends by age - find total and average"""
from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setAppName('friends_by_age.py')
sc = SparkContext(conf=conf)

friends_by_age = sc.textFile('data/fakefriends.csv')

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

friend = friends_by_age.map(parseLine)
totals_by_age = friend.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] +y[0], x[1] + y[1]))
average_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])

for i in average_by_age.collect():
    print(i)