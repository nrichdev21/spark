from pyspark import SparkContext, SparkConf

sc = SparkContext(conf=conf)

def loadMovieNames():
    movieNames = {}
    with open('C:/SparkCourse/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]),1.0))


# Create the movieID --> movieName lookup table
movieNames = loadMovieNames()

lines = sc.textFile('C:/SparkCourse/ml-100k/u.data')  #Creates a RDD

# (movieID, (rating, 1.0))
movieRatings = lines.map(parseInput)
print(movieRatings.collect())

ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1]))
print(ratingTotalsAndCount.collect())

averageRatings =ratingTotalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0]/totalAndCount[1])
print(averageRatings.collect())

sortedMovies = averageRatings.sortBy(lambda x: x[1]).collect()

for movie in sortedMovies:
    print(movie)