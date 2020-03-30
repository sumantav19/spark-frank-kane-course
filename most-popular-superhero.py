from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("marvel-names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("marvel-graph.txt")

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda accum, current : accum + current)
flipped = totalFriendsByCharacter.map(lambda (heroId,occurences) : (occurences,heroId))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(mostPopularName + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
