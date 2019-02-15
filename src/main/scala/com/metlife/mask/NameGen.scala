package com.metlife.mask

import org.apache.spark.sql.SparkSession

case class Name(id: Long, name: String)

object NameGen {
  def main(spark: SparkSession, outputPath: Option[String] = None): Unit = {

    import spark.implicits._

    val names = spark.range(100000).rdd.mapPartitions(parts => {
      val r = scala.util.Random
      val rng = new RandomNameGenerator(r.nextInt())
      rng.load(r.nextInt())
      parts.map(id => Name(id, rng.nextFullName))
    }).toDF()

    val nnames = names.distinct()
    if (outputPath.isDefined)
      nnames.write.mode("overwrite").parquet(outputPath.get)
    else {
      println("written : " + nnames.count())
      nnames.show()
    }
  }
}
