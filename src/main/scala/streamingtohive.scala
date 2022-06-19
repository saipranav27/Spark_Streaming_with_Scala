import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object streamingtohive {
    def main(args: Array[String]): Unit = {
      println("Structured Streaming Demo")
      val conf = new SparkConf().setAppName("spark structured streaming").setMaster("local[*]")
      val spark = SparkSession.builder().config(conf).getOrCreate()
      println("Spark Session Created")
      val schema = StructType(Array(StructField("empId",StringType),StructField("empName",StringType)))
      val streamdf = spark.readStream.option("header","true").schema(schema).csv("D:\\streaming")
      val query = streamdf.writeStream.outputMode(OutputMode.Append()).format("csv").option("path","hiveloc").option("checkpointLocation","location").start()
      query.awaitTermination()

    }
}
