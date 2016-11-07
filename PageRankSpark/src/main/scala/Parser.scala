import java.io.StringReader

import org.apache.spark.rdd.RDD._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.util
import java.util.regex.{Matcher, Pattern}
import javax.xml.parsers.{SAXParser, SAXParserFactory}

import org.apache.spark.util.DoubleAccumulator
import org.xml.sax.InputSource

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by Darshan on 11/1/16.
  */

object Parser {


  //This function checkes wether the given string is good page name or not.
  def isGoodName(line : String): Boolean ={

    val namePattern = Pattern.compile("^([^~]+)$")
    val questionPattern = Pattern.compile("^[? ]*$")

    val delimLoc: Int = line.indexOf(':')
    val pageName: String = line.substring(0, delimLoc)
    val matcher: Matcher = namePattern.matcher(pageName)
    val questionMatcher: Matcher = questionPattern.matcher(pageName)

    if (!matcher.find || questionMatcher.find) {
    //if (!matcher.find) {
      // Skip this html file, name contains (~).
      return false
    }
    return true
  }

  // This funtion parse the input Iterator of the html page string and converts then into Iterator
  // of tuple contains page name and its adjancency page list.
  def parseInputAndConvertDeadIntoDangling(pages: Iterator[String]) : Iterator[(String, util.List[String])] ={

    val pageWithAdj = pages.filter(line => isGoodName(line)).
      map( line => {
      val node = bz2WikiParser(line)
      (node.pageName, node.adjPages)
    })

    val adjPageWithDummyAdj = pageWithAdj.flatMap(line => makeAdjPageWithDummyAdj(line))
    pageWithAdj ++ adjPageWithDummyAdj

  }

  // This function convert the given the given page adjacency list into the Iterator of the tuple containing
  // adjacency page and empty adjacency list
  def makeAdjPageWithDummyAdj(page : (String, util.List[String])): Iterator[(String, util.List[String])] ={

    val adjPages = page._2
    val emptyAdjPages =  List[String]().asJava
    val newArray = for (value <- adjPages) yield {
      (value, emptyAdjPages)
    }
    newArray.iterator

  }

  // This function parse the input file and convert it into the pairwise RDD in which key is page name and value is (page name, adjacency list)
  def initialParser(input : String, fixPartitioner : Boolean, nbrOfPartitioner : Int, sc : SparkContext) : RDD[(String, (String, util.List[String]))] = {

    // If we want to fix the partitioner number while reading the input file
    if(fixPartitioner){
      val allPages = sc.textFile(input, nbrOfPartitioner).mapPartitions(lines => parseInputAndConvertDeadIntoDangling(lines)).keyBy(line => line._1)
      // As dead node are converted into the dandling node, there would be multiple adjacency list for each page.
      // Below code will reduce them to one adjacency list the preference would be given to non empty list.
      val reducedAllPages = allPages.reduceByKey({(a, b) =>
        if(a._2.length == 0){
          b
        }else{
          a
        }
      })
      reducedAllPages.persist()
    }else {
      val allPages = sc.textFile(input).mapPartitions(lines => parseInputAndConvertDeadIntoDangling(lines)).keyBy(line => line._1)
      // As dead node are converted into the dandling node, there would be multiple adjacency list for each page.
      // Below code will reduce them to one adjacency list the preference would be given to non empty list.
      val reducedAllPages = allPages.reduceByKey({ (a, b) =>
        if (a._2.length == 0) {
          b
        } else {
          a
        }
      })
      reducedAllPages.persist()
    }
  }

  // This function applies the BZ2 parser to the given string and returns the node containing page name
  // and its adjacency list.
  def bz2WikiParser(line : String): Node ={
    // Configure parser
    var linkPageNames: util.List[String] = new util.LinkedList[String]


    val spf: SAXParserFactory = SAXParserFactory.newInstance
    spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
    val saxParser: SAXParser = spf.newSAXParser

    val xmlReader = saxParser.getXMLReader
    linkPageNames = new util.LinkedList[String]
    xmlReader.setContentHandler(new WikiParser(linkPageNames))

    val delimLoc: Int = line.indexOf(':')
    val pageName: String = line.substring(0, delimLoc)
    var html: String = line.substring(delimLoc + 1)

    // Parse page and fill list of linked pages.
    try {
      html = html.replace("&", "&amp;")
      xmlReader.parse(new InputSource(new StringReader(html)))
    } catch {
      case e: Exception => {
        // Discard ill-formatted pages.
        linkPageNames.clear()
      }
    }
    val pageNamesSet: util.Set[String] = new util.HashSet[String](linkPageNames)
    // Remove source page name from its adjacency list if exists.
    pageNamesSet.remove(pageName)
    linkPageNames = new util.LinkedList[String](pageNamesSet)

    val node = Node(pageName, linkPageNames)
    node
  }

  // This function performs the task to distribute the page rank of the page to its adjacency pages
  def distributePageRank(pageInfo : RDD[(String, (util.List[String], Double))]): RDD[(String, (String, Double))]={

    val prDistribution = pageInfo.flatMap( value => {
      val adjPages = value._2._1
      val pageRank = value._2._2
      val adjPageCount = adjPages.length
      // Convert adjacency page list into tuple containing adjacency page name and page rank contribution to it
      val pageRankDist = for (adjPage <- adjPages) yield {
        (adjPage, pageRank/adjPageCount)
      }
      pageRankDist
    }).keyBy{line => line._1} // Page Name is used as Key

    // Apply reduceBy to some the page rank contribution for the page
    val reducedPRDistribution = prDistribution.reduceByKey({(a, b) => (a._1, a._2+b._2)})
    reducedPRDistribution
  }

  // This function calculates the new page rank for the all pages from the page rank contribution and sum of
  // dangling node page rank.
  def calculateNewPageRank(distAdjPagePR : RDD[(String, (String, Double))],
                           allPages : RDD[(String, (util.List[String], Double))],
                           pageCount : Long,
                           delta: Double,
                           sc : SparkContext,
                           dangPRSum : DoubleAccumulator,
                           allPRSum : DoubleAccumulator) : RDD[(String, (util.List[String], Double))] ={

    // Join all page RDD with the RDD which contains page rank contribution for the each page
    val pagesWithContributionPR = allPages.leftOuterJoin(distAdjPagePR)
    val alpha = 0.15d

    // Apply map function on RDD generated by join operation to calculate new page rank.
    val newPagePR = pagesWithContributionPR.mapValues( value => {

      val adjPages = value._1._1
      val curretnPageRank = value._1._2
      val prContribSum = value._2
      var newPageRank = 0.0
      if(prContribSum != None){
        // New Page Rank equation
        newPageRank = (alpha/pageCount) + (1-alpha)*(prContribSum.get._2 + (delta/pageCount))
      }else{
        // If the contribution of this page is zero.
        newPageRank = (alpha/pageCount) + (1-alpha)*(delta/pageCount)
      }
      // Increment the same of dangling node page rank sum.
      if(adjPages.size() == 0){
        dangPRSum.add(newPageRank)
      }
      allPRSum.add(newPageRank)
      (adjPages, newPageRank)
    })
    newPagePR
  }

  def evaluate[T](rdd:RDD[T]) = {
    rdd.sparkContext.runJob(rdd,(iter: Iterator[T]) => {
      while(iter.hasNext) iter.next()
    })
  }

  // This function sort the given Iterator and returns only top 100 values in the form of Iterator.
  def sortPageLocally(pages: Iterator[(String, (util.List[String], Double))]) : Iterator[(String, Double)] = {
    val list = pages.map(line => (line._1, line._2._2)).toList
    //Sort by Double (Page Rank Value)
    list.sortBy(-_._2).take(100).iterator
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: SparkPageRank <file/folder>")
      System.exit(1)
    }

    var fixPartitioner = false
    var nbrOfPartitioner = 0
    var input = args(0)
    var output = args(1)
    if(args.length >= 3){
      fixPartitioner = true
      nbrOfPartitioner = args(2).toInt
    }

    val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    val sc = new SparkContext(conf)

    // Parse the input and generate pairwise RDD
    val allPages = initialParser(input, fixPartitioner, nbrOfPartitioner, sc)

    println("Number of partitions :" + allPages.partitions.length)
    // Calculate total number of pages in the source data to give initial page rank.
    val nbrOfPages = sc.longAccumulator("Total Number of pages")
    allPages.foreach(x => nbrOfPages.add(1))
    val pageCount = nbrOfPages.value

    // Assign initial page rank to all the pages.
    var allPagesWithPR = allPages.mapValues(value => (value._2, 1.0/pageCount))

    // At First Iteration sum of dangling node page rank is zero
    var delta : Double = 0.0

    // Page Rank Iteration
    for(i <- 1 to 10) {

      // Accumulators
      val dangPRSum = sc.doubleAccumulator("Sum Of Dangling Node Page Rank")
      val allPRSum = sc.doubleAccumulator("Sum of All Page Rank")

      // First Distribute the page rank.
      val distAdjPagePR = distributePageRank(allPagesWithPR)
      // Calculate New Page Rank
      allPagesWithPR = calculateNewPageRank(distAdjPagePR, allPagesWithPR, pageCount, delta, sc, dangPRSum, allPRSum)
      // Force the evalution to make accumulator work
      evaluate(allPagesWithPR)
      delta = dangPRSum.value

      println("Iteration Number :" + i)
      println("Danging node page rank sum : "+delta)
      println("All Page Rank Sum : "+allPRSum.value)

    }

    // Find the top 100 records.
    val top100 = allPagesWithPR.mapPartitions(lines => sortPageLocally(lines), preservesPartitioning = true).
      takeOrdered(100)(Ordering[Double].reverse.on(line => line._2))
    sc.parallelize(top100, 1).saveAsTextFile(output)

  }

}
