import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object filterstreamdata {
  def main(args: Array[String]): Unit = {
    println("Structured Streaming Demo")
    val conf = new SparkConf().setAppName("spark structured streaming").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    println("Spark Session Created")
    val schema = StructType(Array(StructField("phone", StringType), StructField("sale_volume", IntegerType)))
    val streamdf = spark.readStream.option("header", "true").schema(schema).csv("D:\\streaming").groupBy("phone").sum("sale_volume")
    streamdf.createOrReplaceTempView("temp")
    val query1 = spark.sql("Select * from temp where phone='iPhone'")
    val query = query1.writeStream.format("console").outputMode(OutputMode.Complete()).start()
    query.awaitTermination()
  }


}
