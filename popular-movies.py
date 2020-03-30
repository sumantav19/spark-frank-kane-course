from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-1m/ratings.dat")
movies = lines.map(lambda x: (int(x.split("::")[1]), 1))
movieCounts = movies.reduceByKey(lambda accum, current: accum + current)

flipped = movieCounts.map( lambda (movieId, occurence) : (occurence, movieId) )
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print result
