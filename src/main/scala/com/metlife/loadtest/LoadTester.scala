package com.metlife.loadtest

import com.metlife.mask.{Mask, MaskType}
import org.apache.spark.sql.SparkSession



object LoadTester {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SQLWorkFlow").enableHiveSupport().getOrCreate()
    val globalName = if (args.length == 1) args(0) else "SSN1"
    val start = if (args.length == 2) args(1).toInt else 1
    val end = if (args.length == 3) args(2).toInt else 1000000
    val numDigits = if (args.length == 3) args(2).toInt else 9


    createDF(spark, globalName, start, end, numDigits)
  }

  def createDF(spark: SparkSession, globalName: String, begin: Int, end: Int, numDigits: Int): Int = {

    import org.apache.spark.sql.types._

    val df = spark.range(begin, end).toDF("ssn1")
    val df1 = df.withColumn("name", df("ssn1").cast(StringType))
    println(df1.count())
    df1.count().toInt
    val beginT = System.currentTimeMillis()
    Mask.maskColumnVal(df1, "ssn1", globalName, true, numDigits, true, MaskType.REAL_NUMBER)
    val endT = System.currentTimeMillis()
    println("Total time taken is " + (endT - beginT) / 1000 + " secs")
    10
  }
}