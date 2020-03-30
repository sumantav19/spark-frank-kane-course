from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("fakefriends.csv")
rdd = lines.map(parseLine)
temprdd = rdd.mapValues(lambda x: (x, 1))
# tempResults = temprdd.collect()
# for result in tempResults:
#     print(result)

# accumalates by key
def aggregateFriendsByAge(accumByKey,currentBykey):
    # print("agg",accumByKey,currentBykey)
    return (accumByKey[0] + currentBykey[0], accumByKey[1] + currentBykey[1])

totalsByAge= temprdd.reduceByKey(aggregateFriendsByAge)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
