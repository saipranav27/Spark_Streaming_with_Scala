import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, from_unixtime, unix_timestamp, window}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object sparkstreamingwindowaggregation {
  def main(args: Array[String]): Unit = {
    println("Structured Streaming Demo")
    val conf = new SparkConf().setAppName("spark structured streaming").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    println("Spark Session Created")
    val schema = StructType(Array(StructField("phone", StringType), StructField("sale_volume", IntegerType)))
    val streamdf = spark.readStream.option("header", "true").schema(schema).csv("D:\\streaming")
    import spark.implicits._
    val streamstamp = streamdf.withColumn("datetime",from_unixtime(unix_timestamp(current_timestamp(), "yyyyMMdd'T'HHmmss:SSSSSS")))
    val windowstream = streamstamp.groupBy(window($"datetime","20 seconds")).sum("sale_volume")
    val query = windowstream.writeStream.format("console").outputMode(OutputMode.Complete()).start()
    query.awaitTermination()
  }
}
