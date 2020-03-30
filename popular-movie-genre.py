import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MovieGenreSimilarities")
sc = SparkContext(conf = conf)
movieData = sc.textFile("ml-100k/u.item")
ratingData = sc.textFile("ml-100k/u.data")


def loadGenre():
  genre = {}
  with open("ml-100k/u.genre") as f:
    for line in f:
      fields = line.split('|')
      genre[int(fields[1])] = fields[0].decode('ascii', 'ignore')
  return genre

genres =loadGenre()

def loadMovieIdwithGenre(text):
  fields = text.split('|')
  movieWithGenre = []
  for i in range(len(fields[5:])-1):
    if int(fields[5+i]) == 1:
      movieWithGenre.append((int(fields[0]),genres[i]))
  return movieWithGenre

# movie id and rating - user id not required
def loadMovieRating(text):
  fields = text.split()
  return (int(fields[1]),int(fields[2]))

def avgRating(ratings):
  total = 0
  for rating in ratings:
    total += rating
  return (total / len(ratings),len(ratings))

movieIdGenreRdd = movieData.flatMap(loadMovieIdwithGenre)

movieRating = ratingData.map(loadMovieRating)
groupedByMovieId = movieRating.groupByKey()
movieRatingWithTotalRatersGenre = groupedByMovieId.mapValues(avgRating).join(movieIdGenreRdd)

# movieRatingWithTotalRatersGenre.collect()

for result in movieRatingWithTotalRatersGenre.collect():
  print result
