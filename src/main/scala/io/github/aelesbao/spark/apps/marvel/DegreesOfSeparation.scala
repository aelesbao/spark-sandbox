package io.github.aelesbao.spark.apps.marvel

import com.typesafe.scalalogging.Logger
import io.github.aelesbao.spark.data.MarvelDataSource
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

object DegreesOfSeparation {
  // Some custom data types
  // BFSData contains an array of hero ID connections, the distance, and color.
  type BFSData = (Array[Int], Int, Color.EnumVal)
  // A BFSNode has a heroID and the BFSData associated with it.
  type BFSNode = (Int, BFSData)

  private val log = Logger(getClass)

  implicit lazy val sc = new SparkContext("local[*]", getClass.getName)

  def main(args: Array[String]): Unit = {
    // The characters we want to find the separation between.
    val (startCharacterID, targetCharacterID) = if (args.length == 2) (args(0).toInt, args(1).toInt) else (5306, 173)
    val (startCharacter, targetCharacter) = characterNames(startCharacterID, targetCharacterID)
    val degrees = calculateDegreesOfDistance(startCharacterID, targetCharacterID)

    println(s"There are $degrees degrees of separation between $startCharacter and $targetCharacter")
  }

  def characterNames(startCharacterID: Int, targetCharacterID: Int): (String, String) = {
    val names = MarvelDataSource("marvel-names")
      .flatMap(parseNames)

    val startCharacter = names.lookup(startCharacterID)(0)
    val targetCharacter = names.lookup(targetCharacterID)(0)

    (startCharacter, targetCharacter)
  }

  def parseNames(row: Array[String]): Option[(Int, String)] = {
    if (row.length > 1) Some(row(0).trim().toInt, row(1)) else None
  }

  def calculateDegreesOfDistance(startCharacterID: Int, targetCharacterID: Int): Int = {
    // Our accumulator, used to signal when we find the target
    // character in our BFS traversal.
    val hitCounter = sc.longAccumulator("Hit Counter")

    def loop(iteration: Int, iterationRdd: RDD[BFSNode]): Int = {
      log.info(s"Running BFS Iteration# $iteration")

      // Create new vertices as needed to darken or reduce distances in the
      // reduce stage. If we encounter the node we're looking for as a GRAY
      // node, increment our accumulator to signal that we're done.
      val mapped = iterationRdd.flatMap(bfsMap(targetCharacterID, hitCounter))

      // Note that mapped.count() action here forces the RDD to be evaluated, and
      // that's the only reason our accumulator is actually updated.
      log.info(s"Processing ${mapped.count()} values.")

      if (hitCounter.value > 0) {
        log.info(s"Hit the target character! From ${hitCounter.value} different direction(s).")
        iteration
      } else if (iteration < 10) {
        // Reducer combines data for each character ID, preserving the darkest
        // color and shortest path.
        loop(iteration + 1, mapped.reduceByKey(bfsReduce))
      } else {
        Int.MaxValue
      }
    }

    val iterationRdd = MarvelDataSource("marvel-graph")
      .map(convertToBFS(startCharacterID))

    loop(1, iterationRdd)
  }

  /** Converts a line of raw input into a BFSNode */
  def convertToBFS(startCharacterID: Int)(fields: Array[String]): BFSNode = {
    // Extract this hero ID from the first field
    val heroID = fields(0).toInt

    // Extract subsequent hero ID's into the connections array
    val connections = (1 to (fields.length - 1))
      .map(fields(_).toInt)

    // Unless this is the character we're starting from,
    // use default distance and color is Int.MaxValue and white
    val (color, distance) = if (heroID == startCharacterID) (Color.Gray, 0) else (Color.White, Int.MaxValue)

    (heroID, (connections.toArray, distance, color))
  }

  /** Expands a BFSNode into this node and its children */
  def bfsMap(targetCharacterID: Int, hitCounter: LongAccumulator)(node: BFSNode): Array[BFSNode] = {
    // Extract data from the BFSNode
    val characterID: Int = node._1
    val data: BFSData = node._2

    val connections: Array[Int] = data._1
    val distance: Int = data._2
    val color: Color.EnumVal = data._3

    // Gray nodes are flagged for expansion, and create new
    // gray nodes for each connection
    if (color == Color.Gray) {
      // Color this node as black, indicating it has been processed already.
      val thisEntry: BFSNode = (characterID, (connections, distance, Color.Black))

      // This is called from flatMap, so we return an array
      // of potentially many BFSNodes to add to our new RDD
      val results: Array[BFSNode] = connections
        .map(connection => {
          val newCharacterID = connection
          val newDistance = distance + 1
          val newColor = Color.Gray

          // Have we stumbled across the character we're looking for?
          // If so increment our accumulator so the driver script knows.
          if (targetCharacterID == connection) {
            hitCounter.add(1)
          }

          // Create our new Gray node for this connection and add it to the results
          (newCharacterID, (Array[Int](), newDistance, newColor))
        })

      // Add the original node back in, so its connections can get merged with
      // the gray nodes in the reducer.
      results :+ thisEntry
    }
    else {
      val thisEntry: BFSNode = (characterID, (connections, distance, color))
      Array(thisEntry)
    }
  }

  /** Combine nodes for the same heroID, preserving the shortest length and darkest color. */
  def bfsReduce(data1: BFSData, data2: BFSData): BFSData = {
    // Extract data that we are combining
    val edges1: Array[Int] = data1._1
    val edges2: Array[Int] = data2._1
    val distance1: Int = data1._2
    val distance2: Int = data2._2
    val color1: Color.EnumVal = data1._3
    val color2: Color.EnumVal = data2._3

    // Default node values
    var distance: Int = Int.MaxValue
    var color: Color.EnumVal = Color.White
    var edges: ArrayBuffer[Int] = ArrayBuffer()

    // See if one is the original node with its connections.
    // If so preserve them.
    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }

    // Preserve minimum distance
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }

    // Preserve darkest color
    if (color1 == Color.White && (color2 == Color.Gray || color2 == Color.Black)) {
      color = color2
    }
    if (color1 == Color.Gray && color2 == Color.Black) {
      color = color2
    }
    if (color2 == Color.White && (color1 == Color.Gray || color1 == Color.Black)) {
      color = color1
    }
    if (color2 == Color.Gray && color1 == Color.Black) {
      color = color1
    }

    return (edges.toArray, distance, color)
  }

  object Color {

    sealed trait EnumVal

    case object White extends EnumVal

    case object Gray extends EnumVal

    case object Black extends EnumVal

  }

}
