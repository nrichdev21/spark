from pyspark.sql import *
from pyspark.sql import functions as f
spark = SparkSession.builder.appName('LowestMovieWithDataFrames').getOrCreate()

def movieNames():
    movieNames = {}
    with open('C:/BigData/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[fields[0]]= fields[1]
    return movieNames

def parseLines(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))


movieNames = movieNames()
lines = spark.sparkContext.textFile('C:/BigData/ml-100k/u.data')
movieRatings = lines.map(parseLines) #This is an RDD

#Convert movieRatings from RDD to a DataFrame
df = spark.createDataFrame(movieRatings)

averageRatings = df.groupBy('movieID').agg(f.avg('rating').alias('avgRating'))
numRatings = df.groupBy('movieID').agg(f.count('rating').alias('numRatings'))

badMovies = averageRatings.join(numRatings,'movieID').show()



