package io.github.aelesbao.spark.apps.movielens

import com.typesafe.scalalogging.Logger
import io.github.aelesbao.spark.data.MovieLensDataSource
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
    def movieTitle = movies.lookup(_: Int)(0)

    val moviePairSimilarities = loadMoviePairSimilarities()

    // Extract similarities for the movie we care about that are "good".
    if (args.length > 0) {
      val movieID = args.lift(0).map(_.toInt).getOrElse(1)
      val mainMovieTitle = movieTitle(movieID)

      log.info(s"Calculating similarities for movie ${mainMovieTitle}")

      val scoreThreshold = args.lift(1).map(_.toDouble).getOrElse(0.97)
      val coOccurrenceThreshold = args.lift(2).map(_.toDouble).getOrElse(50.0)

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above
      val filteredResults = moviePairSimilarities.filter(x => {
        val (pair, (score, strength)) = x
        pair.productIterator.contains(movieID) &&
          score > scoreThreshold && strength > coOccurrenceThreshold
      })

      // Sort by quality score.
      val results = filteredResults.map(_.swap).sortByKey(false).take(10)

      println(s"\nTop 10 similar movies for ${mainMovieTitle}")
      for (result <- results) {
        val ((score, strength), pair) = result
        // Display the similarity result that isn't the movie we're looking at
        val similarMovieID = if (pair._1 == movieID) pair._2 else pair._1
        println(f"${movieTitle(similarMovieID)}%-30s\tscore: ${score}\tstrength: ${strength}")
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
    val cacheFile = ".rdd-cache/movie-similarities"
    if (Path(cacheFile).exists)
      loadCachedMoviePairSimilarities(cacheFile)
    else {
      analyseMoviePairSimilarities(cacheFile)
    }
  }

  private def loadCachedMoviePairSimilarities(cacheFile: String): RDD[MoviePairRatingSimilarity] = {
    log.info("Loading cached movie similarities")
    sc.textFile(cacheFile).map(MoviePairRatingSimilarity.apply)
  }

  private def analyseMoviePairSimilarities(cacheFile: String): RDD[MoviePairRatingSimilarity] = {
    log.info("Analysing movie similarities")

    val ratings = loadRatingsPerUser()
    val moviePairSimilarities = ratings.join(ratings)
      .filter(filterDuplicates)
      .map(makePairs)
      .groupByKey()
      .mapValues(computeCosineSimilarity)
      .sortByKey()

    log.debug(s"Caching movie similarities\n${moviePairSimilarities.toDebugString}")
    moviePairSimilarities.saveAsTextFile(cacheFile)

    moviePairSimilarities.cache()
  }

  def filterDuplicates(userRatingPair: UserRatingPair) = {
    val ((movie1, _), (movie2, _)) = userRatingPair._2
    movie1 < movie2
  }

  def makePairs(userRatingPair: UserRatingPair) = {
    val ((movie1, rating1), (movie2, rating2)) = userRatingPair._2
    (movie1 -> movie2, rating1 -> rating2)
  }

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    val sum = ratingPairs.foldLeft((0.0, 0.0, 0.0)) { (sum, pair) =>
      val (ratingX, ratingY) = pair

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

  object MoviePairRatingSimilarity {
    def apply(line: String): MoviePairRatingSimilarity =
      Parser.parse(Parser.data, line) match {
        case Parser.Success(matched, _) => matched
        case Parser.NoSuccess(msg, _) => scala.sys.error(msg)
      }

    object Parser extends JavaTokenParsers {
      override val skipWhitespace = false

      def int = wholeNumber ^^ { _.toInt }
      def decimal = decimalNumber ^^ { _.toDouble }

      def moviePairTuple: Parser[MoviePair] =
        "(" ~> int ~ "," ~ int <~ ")" ^^ { case a ~ "," ~ b => a -> b }

      def ratingSimilarityTuple: Parser[RatingSimilarity] =
        "(" ~> decimal ~ "," ~ int <~ ")" ^^ { case r ~ "," ~ s => r -> s }

      def data: Parser[MoviePairRatingSimilarity] =
        phrase("(" ~> moviePairTuple ~ "," ~ ratingSimilarityTuple <~ ")") ^^ { case mp ~ "," ~ rs => mp -> rs }
    }
  }
}
