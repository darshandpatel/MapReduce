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

  def initialParser(input : String, sc : SparkContext) : RDD[(String, (String, util.List[String]))] = {

    //Read the input files and parse them.
    val lines = sc.textFile(input).filter(line => isGoodName(line)).
    map(line => {
    val node = bz2WikiParser(line)
    (node.pageName, node.adjPages)
    }).keyBy{line => line._1}

    //lines.saveAsTextFile("./parseoutput")

    // Convert the Dead nodes into Dangling nodes
    val dummyLines = lines.flatMap(line => {
    val adjPages = line._2._2
    //adjPages.asScala.toList.foreach(page => (page, emptyAdjPages))
    val newArray = for (value <- adjPages) yield {
    val emptyAdjPages =  List[String]().asJava
    (value, emptyAdjPages)
    }
    newArray
    }).keyBy{ line => line._1}

    //dummyLines.saveAsTextFile("./dummyLines")

    // Convert the multiple empty list as value to single empty list
    val reducedDummyLines = dummyLines.reduceByKey((a, b) => a)

    // Union the all the pages with initially parsed pages.
    val allPages = lines.union(reducedDummyLines)

    val reducedAllPages = allPages.reduceByKey({(a, b) =>
    if(a._2.length == 0){
    b
    }else{
      a
    }
    })
    reducedAllPages
  }

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

  def distributePageRank(pageInfo : RDD[(String, (util.List[String], Double))]): RDD[(String, (String, Double))]={

    val prDistribution = pageInfo.flatMap( value => {
      val adjPages = value._2._1
      val pageRank = value._2._2
      val adjPageCount = adjPages.length
      val pageRankDist = for (adjPage <- adjPages) yield {
        (adjPage, pageRank/adjPageCount)
      }
      pageRankDist
    }).keyBy{line => line._1}

    val reducedPRDistribution = prDistribution.reduceByKey({(a, b) => (a._1, a._2+b._2)})
    reducedPRDistribution
  }

  def calculateNewPageRank(distAdjPagePR : RDD[(String, (String, Double))],
                           allPages : RDD[(String, (util.List[String], Double))],
                           pageCount : Long,
                           delta: Double,
                           sc : SparkContext,
                           dangPRSum : DoubleAccumulator,
                           allPRSum : DoubleAccumulator) : RDD[(String, (util.List[String], Double))] ={

    val pagesWithContributionPR = allPages.leftOuterJoin(distAdjPagePR)
    val alpha = 0.15d

    val newPagePR = pagesWithContributionPR.mapValues( value => {

      val adjPages = value._1._1
      val curretnPageRank = value._1._2
      val prContribSum = value._2
      var newPageRank = 0.0
      if(prContribSum != None){
        newPageRank = (alpha/pageCount) + (1-alpha)*(prContribSum.get._2 + (delta/pageCount))
      }else{
        newPageRank = (alpha/pageCount) + (1-alpha)*(delta/pageCount)
      }
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

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file/folder>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    val sc = new SparkContext(conf)

    val allPages = initialParser(args(0), sc)

    val nbrOfPages = sc.longAccumulator("Total Number of pages")
    allPages.foreach(x => nbrOfPages.add(1))
    val pageCount = nbrOfPages.value

    var allPagesWithPR = allPages.mapValues(value => (value._2, 1.0/pageCount))

    //println(distAdjPagePR.count())
    //allPagesWithPR.mapValues(value => value._2).saveAsTextFile("./InitialPageRank")

    // At First Iteration sum of dangling node page rank is zero
    var delta : Double = 0.0

    for(i <- 1 to 10) {

      val dangPRSum = sc.doubleAccumulator("Sum Of Dangling Node Page Rank")
      val allPRSum = sc.doubleAccumulator("Sum of All Page Rank")

      println("Iteration Number :" + i)
      val distAdjPagePR = distributePageRank(allPagesWithPR)
      //distAdjPagePR.saveAsTextFile("./DistributedPageRank")
      allPagesWithPR = calculateNewPageRank(distAdjPagePR, allPagesWithPR, pageCount, delta, sc, dangPRSum, allPRSum)
      evaluate(allPagesWithPR)
      delta = dangPRSum.value
      println("Danging node page rank sum : "+delta)
      println("All Page Rank Sum : "+allPRSum.value)
    }

    val localTop100 = allPagesWithPR.mapValues(line=>line._2).takeOrdered(100)(Ordering[Double].reverse.on(line => line._2))
    sc.parallelize(localTop100).saveAsTextFile("./Top100")

    //val sorted = allPagesWithPR.sortByKey(ascending = false, numPartitions = 10)
    //sorted.mapValues(value => value._2).saveAsTextFile("./FinalOutput")
    //sc.parallelize(sorted.mapValues(value => value._2).take(100)).saveAsTextFile("./Top100")
    sys.exit(0)
  }

}

case class Node(pageName : String, adjPages : util.List[String])