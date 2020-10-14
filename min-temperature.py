from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName('min-temperature.py')
sc = SparkContext(conf=conf)

temp_data = sc.textFile('data/1800.csv')

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    typeValue = fields[3]
    return (stationID, entryType, typeValue)

rdd = temp_data.map(parseLine)
min_temps = rdd.filter(lambda x: 'TMIN' in x[1])
station_min_temps = min_temps.map(lambda x: (x[0], x[2]))
coldest_temp = station_min_temps.reduceByKey(lambda x, y: min(x,y))

for i in coldest_temp.collect():
    print(i)