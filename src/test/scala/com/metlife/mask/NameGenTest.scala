package com.metlife.mask

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

class NameGenTest extends FunSuite with SharedSparkContext {

  test("test NameGenTest") {
    val sqlContext = new SQLContext(sc)
    import java.nio.file.Paths
    val platformIndependentPath = Paths.get(classOf[NameGenTest].getClassLoader.getResource("flow2.csv").toURI).toString

    val parent = Paths.get(platformIndependentPath).getParent
//    val f = new File("C:\\Users\\dpeechara\\Desktop"+"/names.parquet")
//    println(" absolute path : "+f.getAbsolutePath)
    NameGen.main(sqlContext.sparkSession,None)
    //TODO ensure names are generated.
  }
}
