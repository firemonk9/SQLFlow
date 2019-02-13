package com.metlife.datagen

import java.io.File
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

class NameGenTest extends FunSuite with SharedSparkContext {

  test("test NameGenTest") {
    val sqlContext = new SQLContext(sc)
    val url = classOf[NameGenTest].getClassLoader.getResource("flow2.csv")
    val f = new File(url.toURI().getPath+"/../names.parquet")
    println(" absolute path : "+f.getAbsolutePath)
    NameGen.main(sqlContext.sparkSession,f.getAbsolutePath)
    //TODO ensure names are generated.
  }
}
