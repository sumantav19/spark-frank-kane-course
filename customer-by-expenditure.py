from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerByExpenditure")
sc = SparkContext(conf = conf)

def parseLine(line):
  x = line.split(",")
  customerId = int(x[0])
  cost = float(x[2])
  return (customerId,cost)


input = sc.textFile("customer-orders.csv")
expenseByCustomerId = input.map(parseLine).reduceByKey(lambda accum,current:accum+current).map(lambda x: (x[1],x[0])).sortByKey()

for result in expenseByCustomerId.collect():
  print result
