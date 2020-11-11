from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local[4]').setAppName('the_rdd')
sc = SparkContext(conf = conf)

# Create sample rdd to use for examples.
rdd1 = sc.parallelize([('a', 7), ('a', 2), ('b', 2)])
rdd2 = sc.parallelize([('a', 2), ('d', 1), ('b', 1)])
rdd3 = sc.parallelize(range(100))
rdd4 = sc.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])



"""----------------------------------------------------------------------------------------------------------------
Retrieving RDD Information
----------------------------------------------------------------------------------------------------------------"""
def basic_information():
    """ Basic Information"""
    # List number of partitions
    print('Number of Partitions:')
    print(rdd1.getNumPartitions())  # 4
    print()

    # Count RDD instances
    print('Count RDD instances:')
    print(rdd2.count())  # 3
    print(rdd3.count())  # 100
    print(rdd4.count())  # 2
    print()

    # Count RDD instances by Key
    print('Count RDD instances by Key:')
    print(rdd1.countByKey())  # defaultdict(<class 'int'>, {'a': 2, 'b': 1})
    print(rdd2.countByKey())  # defaultdict(<class 'int'>, {'a': 1, 'd': 1, 'b': 1})
    print(rdd4.countByKey())  # defaultdict(<class 'int'>, {'a': 1, 'b': 1})
    print()

    # Count RDD instances by Value
    print('Count RDD instances by Value:')
    print(rdd1.countByValue())  # {('a', 7): 1, ('a', 2): 1, ('b', 2): 1})
    print(rdd2.countByValue())  # {('a', 2): 1, ('d', 1): 1, ('b', 1): 1})
    # print(rdd4.countByValue()) <---- Throws an Error.  This RDD does not contain integers
    print()

    # Return (key, value) pairs as a dictionary
    print('Return (key, value) pairs as dictionaries')
    print(rdd1.collectAsMap())      # {'a': 2, 'b': 2}  Note how the second 2 key value overides the 1st 2 key value.
    print(rdd2.collectAsMap())      # {'a': 2, 'd': 1, 'b': 1}
    # print(rdd3.collectAsMap()) <---- Throws an error.
    print(rdd4.collectAsMap())      # {'a': ['x', 'y', 'z'], 'b': ['p', 'r']}
    print()

    # Sum of RDD elements
    print('Sum of an RDD elements:')
    print(rdd3.sum())       # 4950
    print()

    # Check whether an RDD is empty
    print('Check whether an RDD is empty:')
    print((sc.parallelize({}).isEmpty()))
    print()

def summary():
    """Display summary information for an rdd"""
    print(rdd1.max())   # <-- ('b',2)
    print(rdd2.max())   # <-- ('d',1)
    print(rdd3.min())   # <-- 0

def apply_functions():
    """
    rdd1 = sc.parallelize([('a', 7), ('a', 2), ('b', 2)])
    rdd2 = sc.parallelize([('a', 2), ('d', 1), ('b', 1)])
    rdd3 = sc.parallelize(range(100))
    rdd4 = sc.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])
    """
    """ Applying functions"""
    # MAP - apply a function to each RDD element
    func = rdd1.map(lambda x: x+(x[1], x[0])).collect()
    print(func) # <---[('a', 7, 7, 'a'), ('a', 2, 2, 'a'), ('b', 2, 2, 'b')]

    # FLATMAP - apply a function to each RDD element and flatten the result
    func = rdd1.flatMap(lambda x: x+(x[1], x[0])).collect()
    print(func) # <--- ['a', 7, 7, 'a', 'a', 2, 2, 'a', 'b', 2, 2, 'b']

    #FLATMAPVALUES - apply a flatmap to each (key, value) pair without chaning the keys.
    func = rdd4.flatMapValues(lambda x: x).collect()
    print(func) # <--- [('a', 'x'), ('a', 'y'), ('a', 'z'), ('b', 'p'), ('b', 'r')]

if __name__ == '__main__':
    #basic_information()
    #summary()
    apply_functions()
