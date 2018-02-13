import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable

/**
  * An aggregation function for summing arrays in Spark
  *
  * Created by SBrown52 on 12/02/2018.
  */

case class Trades(trade: String, Desk: String, MtM: Array[Double])

object VectorSum {

  private class VectorSumAggregateFunction extends UserDefinedAggregateFunction {
    def inputSchema: StructType =     //
      new StructType().add("MtM", ArrayType(DoubleType))
    def bufferSchema: StructType =
      new StructType().add("MtM.Sum", ArrayType(DoubleType))
    def dataType: DataType = ArrayType(DoubleType)
    def deterministic: Boolean = true

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = new Array[Double](10)
    }

    def sumFunction1(aggregationBuffer: MutableAggregationBuffer, input: Row): Unit = {
      val buffer = aggregationBuffer.get(0).asInstanceOf[mutable.WrappedArray[Double]]
      val update = input.get(0).asInstanceOf[mutable.WrappedArray[Double]]

      val arraz = new Array[Double](buffer.length)
      // should probably check for nulls
      for (i <- 0 to buffer.length - 1) {
        arraz(i) = buffer(i) + update(i);
      }
      aggregationBuffer.update(0, arraz)
    }

    def sumFunction2(aggregationBuffer: MutableAggregationBuffer, input: Row): Unit = {
      val a = aggregationBuffer.get(0).asInstanceOf[mutable.WrappedArray[Double]]
      val b = input.get(0).asInstanceOf[mutable.WrappedArray[Double]]
      val arraz = (a, b).zipped.map(_ + _)
      aggregationBuffer.update(0, arraz)
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      sumFunction1(buffer, input);

    }
    // Merge intermediate result sums by adding them
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      sumFunction1(buffer1, buffer2)
    }
    // THe final result will be contained in 'buffer'
    def evaluate(buffer: Row): Any = {
      buffer.get(0)
    }
  }

  def main (args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("UDAF Example")
      .setMaster("local[*]")
      .set("spark.driver.memory", "2g")
      .set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // Random but needed...
    import spark.implicits._

    // Load the trades
    val trades = spark.read
      .option("header", "true")
      .csv("Trades_100.csv");

    val trades_vector = trades.withColumn("MtM", split(trades("MtM"), ";").cast("Array<Double>")).as[Trades]


    trades_vector.show();
    // create a table and cache it
    trades_vector.createOrReplaceTempView("trades")
    spark.sqlContext.cacheTable("trades")

    // Register the UDF
    spark.udf.register("vectorArraySum", new VectorSumAggregateFunction)

    val before = System.currentTimeMillis()
    spark.sql("SELECT vectorArraySum(MtM) from trades").show()
    val after = System.currentTimeMillis()

    println((after-before)/1000d);
  }

}
