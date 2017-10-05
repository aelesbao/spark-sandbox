package io.github.aelesbao.spark.apps.marvel

import com.typesafe.scalalogging.Logger
import io.github.aelesbao.spark.data.MarvelDataSource
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

object DegreesOfSeparation {
  object Color {
    sealed trait EnumVal
    case object White extends EnumVal
    case object Gray extends EnumVal
    case object Black extends EnumVal
  }

  // Some custom data types
  // BFSData contains an array of hero ID connections, the distance, and color.
  type BFSData = (Array[Int], Int, Color.EnumVal)
  // A BFSNode has a heroID and the BFSData associated with it.
  type BFSNode = (Int, BFSData)

  // The characters we want to find the separation between.
  val startCharacterID = 5013
  val targetCharacterID = 14

  // We make our accumulator a "global" Option so we can reference it in a mapper later.
  var hitCounter: Option[LongAccumulator] = None

  implicit lazy val sc = new SparkContext("local[*]", getClass.getName)

  private val log = Logger(getClass)

  def main(args: Array[String]): Unit = {
    // Our accumulator, used to signal when we find the target
    // character in our BFS traversal.
    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    var iterationRdd = MarvelDataSource("marvel-graph").map(convertToBFS)

    for (iteration <- 1 to 10) {
      log.info(s"Running BFS Iteration# $iteration")

      // Create new vertices as needed to darken or reduce distances in the
      // reduce stage. If we encounter the node we're looking for as a GRAY
      // node, increment our accumulator to signal that we're done.
      val mapped = iterationRdd.flatMap(bfsMap)

      // Note that mapped.count() action here forces the RDD to be evaluated, and
      // that's the only reason our accumulator is actually updated.
      log.info(s"Processing ${mapped.count()} values.")

      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println(s"Hit the target character! From ${hitCount} different direction(s).")
          return
        }
      }

      // Reducer combines data for each character ID, preserving the darkest
      // color and shortest path.
      iterationRdd = mapped.reduceByKey(bfsReduce)
    }
  }

  /** Converts a line of raw input into a BFSNode */
  def convertToBFS(fields: Array[String]): BFSNode = {
    // Extract this hero ID from the first field
    val heroID = fields(0).toInt

    // Extract subsequent hero ID's into the connections array
    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for (connection <- 1 to (fields.length - 1)) {
      connections += fields(connection).toInt
    }

    // Default distance and color is 9999 and white
    var color: Color.EnumVal = Color.White
    var distance: Int = 9999

    // Unless this is the character we're starting from
    if (heroID == startCharacterID) {
      color = Color.Gray
      distance = 0
    }

    return (heroID, (connections.toArray, distance, color))
  }

  /** Expands a BFSNode into this node and its children */
  def bfsMap(node: BFSNode): Array[BFSNode] = {
    // Extract data from the BFSNode
    val characterID: Int = node._1
    val data: BFSData = node._2

    val connections: Array[Int] = data._1
    val distance: Int = data._2
    var color: Color.EnumVal = data._3

    // This is called from flatMap, so we return an array
    // of potentially many BFSNodes to add to our new RDD
    var results: ArrayBuffer[BFSNode] = ArrayBuffer()

    // Gray nodes are flagged for expansion, and create new
    // gray nodes for each connection
    if (color == Color.Gray) {
      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor = Color.Gray

        // Have we stumbled across the character we're looking for?
        // If so increment our accumulator so the driver script knows.
        if (targetCharacterID == connection) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }

        // Create our new Gray node for this connection and add it to the results
        val newEntry: BFSNode = (newCharacterID, (Array(), newDistance, newColor))
        results += newEntry
      }

      // Color this node as black, indicating it has been processed already.
      color = Color.Black
    }

    // Add the original node back in, so its connections can get merged with
    // the gray nodes in the reducer.
    val thisEntry: BFSNode = (characterID, (connections, distance, color))
    results += thisEntry

    return results.toArray
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
    var distance: Int = 9999
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
}
