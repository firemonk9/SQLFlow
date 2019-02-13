package com.metlife.datagen

import com.metlife.RandomNameGenerator
import org.apache.spark.sql.SparkSession
case class Name(id:Long,name:String )

object NameGen {
  def main(spark:SparkSession, outputPath:String): Unit = {

    import spark.implicits._

    val names = spark.range(100000).rdd.mapPartitions(parts=> {
      val r = scala.util.Random
      val rng = new RandomNameGenerator(r.nextInt())
        rng.load(r.nextInt())
      parts.map(id=> Name(id,rng.nextFullName))
    }).toDF()

    val nnames = names.distinct()
      nnames.write.mode("overwrite").parquet(outputPath)
    println("written : "+nnames.count())
    nnames.show()
  }
}
