import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MmulSparse {
    val on_csv = """\s*,\s*""".r

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("mmul").setMaster("local")
        val sc   = new SparkContext(conf)

        val textA = sc.textFile("_test/dataA.csv")
        val textB = sc.textFile("_test/dataB.csv")

        val rowsA = textA.map(line => on_csv.split(line))

        // TODO: calculate C = A * B

        val textC = textB
        textC.saveAsTextFile("output")
        
        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
