package io.github.aelesbao.spark.apps.movielens

import java.time.LocalDate

import com.typesafe.scalalogging.Logger
import io.github.aelesbao.spark.data.MovieLensDataSource
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.math.sqrt
import scala.reflect.io.Path
import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

object MovieSimilarities {

  implicit lazy val sc = new SparkContext("local[*]", getClass.getName)

  type Movie = (Int, String)

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  type MoviePair = (Int, Int)
  type RatingSimilarity = (Double, Int)
  type MoviePairRatingSimilarity = (MoviePair, RatingSimilarity)

  private val log = Logger(getClass)

  def main(args: Array[String]): Unit = {
    val movies = loadMovies()
    val moviePairSimilarities = loadMoviePairSimilarities()

    // Extract similarities for the movie we care about that are "good".
    if (args.length > 0 || true) {
      val scoreThreshold = 0.97
      val coOccurrenceThreshold = 50.0

      val movieID:Int = 1 //args(0).toInt

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above
      val filteredResults = moviePairSimilarities.filter(x => {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurrenceThreshold
      })

      // Sort by quality score.
      val results = filteredResults.map(x => (x._2, x._1)).sortByKey(false).take(10)

      def movieTitle(id: Int): String = movies.lookup(movieID)(0)

      println("\nTop 10 similar movies for " + movieTitle(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        val similarMovieID = if (pair._1 == movieID) pair._2 else pair._1
        println(movieTitle(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
  }

  def loadMovies(): RDD[Movie] =
    MovieLensDataSource("movies")
      .map(row => (row("movieId").toInt, row("title")))

  def loadRatingsPerUser(): RDD[(Int, MovieRating)] =
    MovieLensDataSource("ratings")
      .map(row => (row("userId").toInt, (row("movieId").toInt, row("rating").toDouble)))

  def loadMoviePairSimilarities(): RDD[MoviePairRatingSimilarity] = {
//    val cacheFile = "movie-similarities"
//    if (Path(cacheFile).exists)
//      loadCachedMoviePairSimilarities(cacheFile)
//    else {
      val moviePairSimilarities = analyseMoviePairSimilarities()
//      moviePairSimilarities.saveAsTextFile(cacheFile)
      moviePairSimilarities
//    }
  }

  private def loadCachedMoviePairSimilarities(cacheFile: String) = {
    log.info("Loading cached movie similarities")
//    sc.textFile(cacheFile)
//      .map(MoviePairRatingSimilarityParser.parse(MoviePairRatingSimilarityParser.data, _))
  }

  private def analyseMoviePairSimilarities() = {
    log.info("Analysing movie similarities")

    val ratings = loadRatingsPerUser()
    val moviePairSimilarities = ratings.join(ratings)
      .filter(filterDuplicates)
      .map(makePairs)
      .groupByKey()
      .mapValues(computeCosineSimilarity)
      .sortByKey()

    log.debug(moviePairSimilarities.toDebugString)

    moviePairSimilarities.cache()
  }

  def filterDuplicates(userRatingPair: UserRatingPair) = {
    val movieRating1 = userRatingPair._2._1
    val movieRating2 = userRatingPair._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    movie1 < movie2
  }

  def makePairs(userRatingPair: UserRatingPair) = {
    val movieRating1 = userRatingPair._2._1
    val movieRating2 = userRatingPair._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    (movie1 -> movie2, rating1 -> rating2)
  }

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    val sum = ratingPairs.foldLeft((0.0, 0.0, 0.0)) { (sum, pair) =>
      val ratingX = pair._1
      val ratingY = pair._2

      val sum_xx = sum._1 + (ratingX * ratingX)
      val sum_yy = sum._2 + (ratingY * ratingY)
      val sum_xy = sum._3 + (ratingX * ratingY)

      (sum_xx, sum_yy, sum_xy)
    }

    val numerator = sum._3
    val denominator = sqrt(sum._1) * sqrt(sum._2)
    val score: Double = Try(numerator / denominator) getOrElse 0.0
    val numPairs: Int = ratingPairs.size

    score -> numPairs
  }

  object MoviePairRatingSimilarityParser extends JavaTokenParsers {
    override val skipWhitespace = false

    def int = wholeNumber ^^ { _.toInt }
    def decimal = decimalNumber ^^ { _.toDouble }

//    def moviePairTuple: Parser[MoviePair] = "(" ~> repsep(int ~ int, ",") <~ ")" ^^ { case a =>  }
//    def ratingSimilarityTuple: Parser[RatingSimilarity] = "(" ~> repsep(decimal ~ int, ",") <~ ")" ^^ { case r ~ s => r -> s }
//    def data: Parser[MoviePairRatingSimilarity] = "(" ~> repsep(moviePairTuple ~ ratingSimilarityTuple, ",") <~ ")" ^^ { case mp ~ rs => mp -> rs }
  }
}
