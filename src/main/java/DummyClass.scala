
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

class DummyClass {

}

object DummyClass {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.types._
    val sche = StructType(
      StructField("color", StringType, nullable = false) ::
        StructField("category", StringType, nullable = false) ::
        StructField("type", StringType, nullable = false) :: Nil)

    //{"color": "black",  "category": "hue1",  "type": "primary"}

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount").master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val a = spark.read.schema(sche).format("json").load("/Users/arpan/Raw_data/json/").withColumn("current_timestamp", current_timestamp)

    a.write.json("/Users/arpan/Raw_data/outpath")

  }
}