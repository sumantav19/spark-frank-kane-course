from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("ml-1m/movies.dat") as f:
        for line in f:
            fields = line.split('::')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("ml-1m/ratings.dat")
movies = lines.map(lambda x: (int(x.split("::")[1]), 1))
movieCounts = movies.reduceByKey(lambda accum, current: accum + current)

flipped = movieCounts.map( lambda (movieId, count) : (count, movieId))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda (count, movieId) : (nameDict.value[movieId], count))

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)
