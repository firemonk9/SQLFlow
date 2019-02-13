package com.metlife.wf

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import com.metlife.datagen.NameGenTest
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

class SqlWorkFlowMainTest extends FunSuite with SharedSparkContext{

  test("testMain") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession

    val url = classOf[NameGenTest].getClassLoader.getResource("flow2.csv")
    val f = new File(url.toURI().getPath+"/flow2.csv")

    SqlWorkFlowMain.processFlow(f.getAbsolutePath().toString,sqlContext,false,true )
  }

}
