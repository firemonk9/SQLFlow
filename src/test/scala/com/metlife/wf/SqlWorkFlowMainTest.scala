package com.metlife.wf

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import com.metlife.datagen.NameGenTest
import org.apache.spark.sql.SQLContext
import org.common.model.CSV_INPUT_JOB
import org.scalatest.FunSuite

class SqlWorkFlowMainTest extends FunSuite with SharedSparkContext{

  test("testMain") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession

    spark.sql("create database mask_db")
    spark.sql("use mask_db")
    val url = classOf[NameGenTest].getClassLoader.getResource("flow2.csv")
    val f = new File(url.toURI().getPath)

    val initPath = url.toURI().getPath
    val csvFile = initPath.replace("flow2.csv","input-one.csv")

    val ds = sqlContext.read.option("header", "true").option("delimiter", ",").csv(csvFile)//.as[CSV_INPUT_JOB] // //.save(outputDir)
    ds.registerTempTable("input_test_1")

    SqlWorkFlowMain.processFlow(f.getAbsolutePath().toString,sqlContext,false,true )
  }

}
