from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('word-count.py')
sc = SparkContext(conf=conf)

book_file = sc.textFile('data/book.txt')
word = book_file.flatMap(lambda x: x.split())
word_count = word.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
word_count_agg = word_count.reduceByKey(lambda x, y: x + y).sortByKey()
sorted_results = word_count_agg.sortBy(lambda x: x[1])

for i in sorted_results.collect():
    print(i)
