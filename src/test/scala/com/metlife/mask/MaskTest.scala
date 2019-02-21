package com.metlife.mask

import com.holdenkarau.spark.testing.SharedSparkContext
import com.metlife.loadtest.LoadTester
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql._


case class Person(name:String, ssn:Int, addr:String)

class MaskTest extends FunSuite with SharedSparkContext  {


  val sampleData = List(Person("John",123456,"abc Cary, nc"),Person("Rob",123457,"abc Cary, nc"),Person("Mary",123458,"abc Cary, nc"))
  val sampleDataNew = List(Person("John",123456,"abc Cary, nc"),Person("Harry",123434,"abc Cary, nc"))

  val sampleDataNeg = List(Person("John",123456,"abc Cary, nc"),Person("Rob",123457,"abc Cary, nc"),Person("Mary",123458,"abc Cary, nc"),Person("GREEN",-123458,"abc Cary, nc"))
  val sampleDataNewNeg = List(Person("John",123456,"abc Cary, nc"),Person("Harry",123434,"abc Cary, nc"))


  test("loadtest 1"){
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession
    assert(LoadTester.createDF(spark,10,100) == 90)
  }

  test("testMaskNumber") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession
    spark.sql("create database mask_db")
    val sampleF = spark.sparkContext.parallelize(sampleData).toDF
    val sampleFNew = spark.sparkContext.parallelize(sampleDataNew).toDF
    val edf = Mask.maskColumn(sampleF,"ssn","SSN",Mask.REAL_NUMBER,true,9,true)
    edf.show()

    val ndf = Mask.maskColumn(sampleFNew,"ssn","SSN",Mask.REAL_NUMBER,true,9,true)
    ndf.show()
    assert(edf.filter(edf("name") === "John").head() == ndf.filter(ndf("name") === "John").head())
    assert(edf.count() == sampleF.count())
    assert(ndf.count() == sampleFNew.count())

  }

//TODO yet to implement
//  test("testMaskNumber with negative values") {
//    val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//    val spark = sqlContext.sparkSession
//    spark.sql("create database mask_db")
//    val sampleF = spark.sparkContext.parallelize(sampleDataNeg).toDF
//    val sampleFNew = spark.sparkContext.parallelize(sampleDataNewNeg).toDF
//    val edf = Mask.maskColumn(sampleF,"ssn","SSN",Mask.REAL_NUMBER,true,9,true)
//    edf.show()
//
//    val ndf = Mask.maskColumn(sampleFNew,"ssn","SSN",Mask.REAL_NUMBER,true,9,true)
//    ndf.show()
//    assert(edf.filter(edf("name") === "John").head() == ndf.filter(ndf("name") === "John").head())
//    assert(edf.filter(edf("name") === "GREEN").head() == sampleF.filter(ndf("name") === "GREEN").head())
//
//    assert(edf.count() == sampleF.count())
//    assert(ndf.count() == sampleFNew.count())
//
//  }


}
