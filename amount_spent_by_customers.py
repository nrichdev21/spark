''' sort by biggest spenders'''
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('amount_spent_by_customers.py')
sc = SparkContext(conf= conf)

infile = sc.textFile('data/customer-orders.csv')

def parseLine(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    product_id = int(fields[1])
    product_cost = float(fields[2])
    return (customer_id, product_cost)

data = infile.map(parseLine)
customer_spend = data.reduceByKey(lambda x,y: x + y)
sorted_customer_spend = customer_spend.sortBy(lambda x: x[1])

for i in sorted_customer_spend.collect():
    print(i)