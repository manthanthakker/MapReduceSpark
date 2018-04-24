package pagerrankhw4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import package1.{NodeIdNeighbors, ParserImpl}

import scala.collection.JavaConversions._


object PageRankDriver {

  /**
    * Invokes the Java parser
    *
    * @param input : A single Line from the input file
    *              returns (nodeId,Neighbors)
    */
  def parseInput(input: String, parserImpl: ParserImpl): (String, Set[String]) = {
    // call to java method
    var nodeString: NodeIdNeighbors = parserImpl.parse(input)

    // in case null values ignore
    if (nodeString == null) {
      return ("INVALIDPAGES", Set())
    }

    var neighbors: Set[String] = nodeString.getNeighbors.toSet

    return (nodeString.getId, neighbors)
  }

  /**
    * Reducer for building the graph
    *
    * @param neighbors1 : neighbors1 of a node from different machines
    * @param neighbors2 : neighbors2 of the same node from different machines
    * @return union of the two sets
    */
  def reducerForGraph(neighbors1: Set[String], neighbors2: Set[String]): (Set[java.lang.String]) = {
    return neighbors1.union(neighbors2)
  }

  /**
    * Emits the node and all the neigbors for the first time to normalize the graph
    * Normalize meaning: For eg input is Graph: A->{B,C} and B->{A}
    * Normalized Graph : A->{B,C} and B->{A} and C->{}
    *
    * @param nodeId    : NodeId which is the pageName
    * @param neighbors : Neighbors of the given node
    * @return List of (nodeId,neigbors)
    */
  def normalizeGraph(nodeId: String, neighbors: Set[String]): List[(String, Set[String])] = {

    var graphList: List[(String, Set[String])] = List()
    // Add the node
    graphList = graphList :+ (nodeId, neighbors)

    neighbors.foreach(x =>
      graphList = graphList :+ (x, Set[String]())
    )
    graphList
  }

  /**
    * Initailizes the pageRank for all nodes to 1/pageCount
    *
    * @param nodeId         : NodeId of the Node
    * @param neighbors      : Neigbors of the Node
    * @param totalPageCount : PageCount in the entire graph
    * @return (nodeId, defaultPageRank)
    */
  def initializePageRank(nodeId: String, neighbors: Set[String], totalPageCount: Long): (String, Double) = {
    (nodeId, (1.0 / totalPageCount))
  }


  /**
    * Mapper for pageRank as discussed in the slides
    *
    * @param nodeId                   : pageName
    * @param pageRank                 : current pageRank
    * @param neighbors                : neighbors
    * @param contributionFromDangling : contributionFromDangling
    * @return List(nodeId,Pagerank)
    */
  def pageRankMapper(nodeId: String, pageRank: Double, neighbors: Set[String], contributionFromDangling: Double): List[(String, Double)] = {
    var pageRanklist: List[(String, Double)] = List()

    // Add the contributionFromDangling precomputed for this iteration
    var newPageRank: Double = pageRank + contributionFromDangling

    if (!neighbors.isEmpty) {

      neighbors.foreach(x => {
        pageRanklist = pageRanklist :+ (x, newPageRank / neighbors.size)
      })
    }
    else {
      // Increment delta by adding a dummy key delta
      pageRanklist = pageRanklist :+ ("delta", newPageRank)
    }
    pageRanklist
  }

  /**
    *
    * @param nodeId           : pageName
    * @param pageRank         : pageRank accumulation for this iteration
    * @param totalUniquePages : totalUniquePages
    * @param Alpha            : Alpha wich is a constant
    * @return :  (nodeId, (pageRank * (1 - Alpha)) + (Alpha / totalUniquePages))
    */
  def applyPageRankFormula(nodeId: String, pageRank: Double, totalUniquePages: Long, Alpha: Double): (String, Double) = {
    if (nodeId.equals("delta"))
      return (nodeId, pageRank)

    (nodeId, (pageRank * (1 - Alpha)) + (Alpha / totalUniquePages))
  }


  /**
    * Driver program
    *
    * param args (0): Input Path
    * param args (2):Output Path
    */
  def main(args: Array[String]): Unit = {

    val iterations = 10
    val alpha = 0.15
    val K=10
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Set PageRank context
    val sc = new SparkContext(new SparkConf().setAppName("PageRankName"))

    // Load Input file to HDFS
    val inputData = sc.textFile(args(0))


    val graphData = inputData.mapPartitions(iter => {
      val parser: ParserImpl = new ParserImpl()
      iter.map(x => parseInput(x, parser))
        .flatMap(x => normalizeGraph(x._1, x._2))
    }, true).reduceByKey(reducerForGraph)


    // Compute the total count of unique pagess
    val totalPageCount = graphData.count()

    // initialize pageRanks to 1/pageCounts
    val pageRankData = graphData.map(x => initializePageRank(x._1, x._2, totalPageCount))

    // Intialize all delta counters
    var contributionFromDangling: Double = 0.0
    var delta: Double = 0.0
    // for the first iteration the pageRank Data would be default
    var pageRankExecution = pageRankData


    // Iterations for pageRank
    for (a <- 1 to iterations) {

      pageRankExecution = graphData.join(pageRankExecution)
        .flatMap(x => pageRankMapper(x._1, x._2._2, (x._2)._1, contributionFromDangling))
        .reduceByKey(_ + _)
        .map(x => applyPageRankFormula(x._1, x._2, totalPageCount, alpha))

      // Get the delta increment in this iteration
      delta = pageRankExecution.filter(x => x._1.equals("delta")).map(x => x._2).reduce(_ + _)

      // Recompute Contribution for next iteration
      contributionFromDangling = (1 - alpha) * (delta / totalPageCount)

    }


    // Apply sorting to get top K results
    val sortedPageRanks = pageRankExecution.takeOrdered(K)(Ordering[Double].reverse.on(x => x._2))

    sc.parallelize(sortedPageRanks).coalesce(1, true).saveAsTextFile(args(1))

  }
}
