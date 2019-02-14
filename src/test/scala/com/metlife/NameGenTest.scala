package com.metlife.datagen

import java.io.File
import java.nio.file.Paths

import com.holdenkarau.spark.testing.SharedSparkContext
import com.metlife.mask.NameGen
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

class NameGenTest extends FunSuite with SharedSparkContext {

  test("test NameGenTest") {
    val sqlContext = new SQLContext(sc)
    import java.nio.file.Paths
    val platformIndependentPath = Paths.get(classOf[NameGenTest].getClassLoader.getResource("flow2.csv").toURI).toString

    val parent = Paths.get(platformIndependentPath).getParent
    val f = new File(parent.getParent+"/names.parquet")
    println(" absolute path : "+f.getAbsolutePath)
    NameGen.main(sqlContext.sparkSession,f.getAbsolutePath)
    //TODO ensure names are generated.
  }
}
