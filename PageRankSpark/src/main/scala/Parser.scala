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
      })

    lines.saveAsTextFile("./parseoutput")

    val results = lines.flatMap(line => {
      val pageName = line._1
      val adjPages = line._2
      val emptyAdjPages =  List[String]().asJava
      List(adjPages).map(page => (pageName, emptyAdjPages))
      List((pageName, adjPages)).map(x=>x)
    })

    //val keyedResults = results.keyBy(_ => _1)

    /*
    val finalResults = results.reduceByKey({ (a, b) =>
      if(a.length == 0){
        if(b.length == 0){
          a
        }else{
          b
        }
      }
      a
    })
    */
    results.saveAsTextFile("./firstoutput")
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
