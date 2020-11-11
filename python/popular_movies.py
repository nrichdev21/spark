from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('popular_movies.py')
sc = SparkContext(conf= conf)

def loadMovieNames():
    movieNames = {}
    with open('data/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

namesDict = sc.broadcast(loadMovieNames())

input = sc.textFile('data/ml-100k/u.data')
movie_data = input.map(lambda x: (x.split()[1],1))
movie_ratings_count = movie_data.reduceByKey(lambda x, y: x + y)
flipped = movie_ratings_count.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()


movie_names = sortedMovies.map(lambda x: (namesDict.value[x[1]], x[0]))
results = movie_names.collect()
for result in results:
    print(result)