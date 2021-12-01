
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer

object crossfeatures {
  def main(args: Array[String]): Unit = {
    val spark = AppUtils.createCluterContext(s"randomCandidates")
    val parser //读入参数

    //配置spark数据库参数

    run(spark, parser)
  }

  def run(spark: SparkSession, parser: OptionParser): Unit = {
    //读取参数
    val DataTable = parser.getString("data_input").split(",")(0)
    val client = new Client()

    val UserTable = Op //读取数据
      .makeProvider(spark.sparkContext, DataTable.split("::")(0))
      .table(DataTable.split("::")(1))
      .persist(StorageLevel.MEMORY_AND_DISK)
      .setName("DataTable")

    //对数据进行交叉处理
    val resultRdd = UserTable
      .map(
        r => {
          var i = 0
          var j = 0
          var arr = ArrayBuffer[String]()
          for (i <- 0 to r.length - 2) {
            for (j <- i + 1 to r.length-1) {
              var a = r(i).toDouble * r(j).toDouble
              arr += a.toString
            }
          }
          Array(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12),arr(13),arr(14))
        }
      )


  }
}
