import org.apache.spark.rdd.RDD._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by Darshan on 11/1/16.
  */
object Parser {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file/folder>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0)).filter(line => Bz2WikiParser.goodPageName(line)).
      map(line => {
        val node = Bz2WikiParser.parseHTMLPage(line)
        (node.getPageName, node.getAdjPages)
      }).keyBy{line => line._1}

    //lines.saveAsTextFile("./parseoutput")

    /*
    val upates = lines.flatMap(line => {
      val length = line._2.size()
      val values = for(i <- 0 until 1) yield {
        if(i==0){
          (line._1, line._2)
        }else{
          val emptyAdjPages =  List[String]().asJava
          for(j <- 0 until length) yield {
            (line._2.get(j), emptyAdjPages)
          }
        }
      }
      values.flatten
    })
    upates.saveAsTextFile("./updates")
    */


    val dummyLines = lines.flatMap(line => {
      //val pageName = line._2._1
      val adjPages = line._2._2
      //adjPages.asScala.toList.foreach(page => (page, emptyAdjPages))
      val newArray = for (value <- adjPages) yield {
        val emptyAdjPages =  List[String]().asJava
        (value, emptyAdjPages)
      }
      newArray
    }).keyBy{ line => line._1}

    //dummyLines.saveAsTextFile("./dummyLines")

    val reducedDummyLines = dummyLines.reduceByKey((a, b) => a)

    val allPages = lines.union(reducedDummyLines)

    val reducedAllPages = allPages.reduceByKey({(a, b) =>
        if(a._2.length == 0){
          b
        }else{
          a
        }
    })

    val nbrOfPages = sc.longAccumulator("Total Number of pages")
    reducedAllPages.foreach(x => nbrOfPages.add(1))
    val pages = nbrOfPages.value

    //reducedAllPages.saveAsTextFile("./reducedAllPages")

    val pageWithPRContrib = reducedAllPages.flatMap( line => {
      val adjPages = line._2._2
      //var isDangling = false
      //if(adjPages.length == 0){
      //  isDangling = true
      //}
      val newArray = for (value <- adjPages) yield {
        (value, 1.0d/pages)
      }
      newArray
    }).keyBy{line => line._1}


    val reducedPagePR = pageWithPRContrib.reduceByKey({(a,b) =>
      (a._1, a._2 + b._2)
    })

    println("dummyLines : " + dummyLines.count())
    println("nbr of pages " + pages)
    println("pageWithPRContrib : " + pageWithPRContrib.count())
    println("reducedPagePR : " + reducedPagePR.count())
    reducedPagePR.saveAsTextFile("./reducedPagePR")
    // First Iteration of Page Rank

    //val linesWithPR = reducedAllPages.join(reducedPagePR)
    //reductedunionLines.saveAsTextFile("./reductedunionLines")

    // val finalResults = reduceDummyLines ++ lines
    //finalResults.saveAsTextFile("./firstoutput")


    /*

     val results = lines.flatMap(line => {

      val parts = line.split(":")
      val key = parts(0)
      val value = ""
      val emptyString = ""

      if(parts.length > 1){
        val value = parts(1).toString
        val argParts = value.split(",")
        val doubleSeq = for(j <- 0 until argParts.length) yield {
          val key = argParts(j).toString
          (key, emptyString)
        }
        doubleSeq.flatten
      }else{
        (key, emptyString)
      }
      //doubleSeq.flatten
      //(key.toString, value.toString)
    })
        results.keyBy((a, b) => {a}).reduceByKey((a, b) => {
          if(a._2.length > 1){
            a._2
          }else if(b._2.length > 1){
            b._2
          }else{
            a._2
          }
        })


        val data = sc.parallelize(List("1,2,3", "4,5,6", "7,8,9"))
        val result = data.flatMap(line=>{
          val arr = line.split(",")
          val doubleSeq = for(i <- 0 until arr.length) yield {
            val x = arr(i).toDouble
            for(j <- (i+1) until arr.length) yield {
              val y = arr(j).toDouble
              val k = Index(i,j)
              val v = Val(x,y)
              (k,v)
            }
          }
          doubleSeq.flatten
        })
        */

    //val keyedLines = linesU.keyBy((key, value) => key)

    //keyedLines.saveAsTextFile("./firstoutput")
    //results.saveAsTextFile("./firstoutput")
  }

}
case class Index(i:Integer, j:Integer)
case class Val(x:Double, y:Double)
