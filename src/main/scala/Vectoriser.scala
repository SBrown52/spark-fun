import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/*
* Code used convert trade or any data from row based format to a "vectorised" format, or a format where the data is now in an array in a single column
* In the original, we had every value on a single line.
* eg:
*  Trade, Index, Value
*	  1		  0		  10
*	  1		  1		  25
*	  1		  2		  30 etc etc
*	
*	Running this code, the output will be:
*	1, 10;25;30
*
*/

object Vectoriser {

  val ARRAY_TYPE = "array";
  val ARRAY_TYPE_STRING = "array<string>";

  val TRADE_COL = "Trade";
  val VALUE_COL = "Value";

  // We have to stringify the array as Spark does not support writing arrays to CSV ?!?!?
  def stringifyArray(df: DataFrame): DataFrame = {
    val colsToStringify = df.schema.filter(r => r.dataType.typeName.equals(ARRAY_TYPE)).map(r => r.name)

    colsToStringify.foldLeft(df)((df, c) => {
      df.withColumn(c, concat_ws(";", col(c).cast(ARRAY_TYPE_STRING)))
    })
  }


  def main (args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("local")
      .setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    // Load original file
    val df = spark.read.option("header", "true")
            .option( "inferSchema", "true")
            .csv("some_file.csv")
            .toDF();

    // Modify to vector structure
    val mod1 = df.groupBy(TRADE_COL).agg(collect_list(VALUE_COL))

    // Stringify the array
    val mod2 = stringifyArray(mod1)
    // We coalesce to get a single file, in the directory below. We might want to tidy the name afterwards, or not.
    mod2.coalesce(1).write.csv("some_file_vec")
  }
}
