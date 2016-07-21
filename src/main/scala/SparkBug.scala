import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.JavaConverters._

object SparkBug {

  def main(args: Array[String]): Unit = {

    val sparkContext = new SparkContext("local", "SparkBug", new SparkConf())
    val sparkSession = SparkSession.builder().getOrCreate()

    val throws = (x: Integer) =>
      if (x == 2) 2
      else throw new Exception

    val ds = sparkSession.createDataFrame(Seq(Row(1)).toList.asJava, StructType(Seq(StructField("a", IntegerType))))
    val e = ds.filter(column("a").equalTo(2))
      .select(udf(throws, IntegerType)(column("a")).as("foo"))
      .filter(column("foo").leq(2))
    e.explain(true)
    e.show()

  }
}
