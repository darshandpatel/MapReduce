import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.util.SizeEstimator

object MmulSparse {
    val on_csv = """\s*,\s*""".r

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("mmul").setMaster("local")
        val sc   = new SparkContext(conf)

        val textA = sc.textFile("_test/dataA.csv")
        val textB = sc.textFile("_test/dataB.csv")

        println("textA Size : " + SizeEstimator.estimate(textA))
        println("textB Size : " + SizeEstimator.estimate(textB))

        val rowsA = textA.map(line => on_csv.split(line)).
          map(line => (line(1).toLong, (line(0).toLong, line(2).toDouble)))

        val calB = textB.map(line => on_csv.split(line)).
          map(line => (line(0).toLong, (line(1).toLong, line(2).toDouble)))

        val groupedCalB = calB.groupByKey()

        // TODO: calculate C = A * B
        val C = rowsA.join(groupedCalB).flatMap(line => {
            val multipyRow = for(value <- line._2._2) yield {
                ((line._2._1._1, value._1),(line._2._1._2 * value._2))
            }
            multipyRow
        })

        val textC = C.reduceByKey( (a,b) => a+b).map(line => line._1._1.toString+", "+line._1._2.toString+", "+line._2.toString)
        textC.saveAsTextFile("output")
        
        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
