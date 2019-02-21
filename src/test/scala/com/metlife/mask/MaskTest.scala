package com.metlife.mask

import com.holdenkarau.spark.testing.SharedSparkContext
import com.metlife.loadtest.LoadTester
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql._


case class Person(name:String, ssn:Int, addr:String)
case class ID(name:String, ssn: String)


class MaskTest extends FunSuite with SharedSparkContext  {


  val sampleData = List(Person("John",123456,"abc Cary, nc"),Person("Rob",123457,"abc Cary, nc"),Person("Mary",123458,"abc Cary, nc"))
  val sampleDataNew = List(Person("John",123456,"abc Cary, nc"),Person("Harry",123434,"abc Cary, nc"))

  val sampleDataNeg = List(Person("John",123456,"abc Cary, nc"),Person("Rob",123457,"abc Cary, nc"),Person("Mary",123458,"abc Cary, nc"),Person("GREEN",-123458,"abc Cary, nc"))
  val sampleDataNewNeg = List(Person("John",123456,"abc Cary, nc"),Person("Harry",123434,"abc Cary, nc"))

  val testData1 = List(ID("John","53A263248"),ID("Rob","457&1@190"),ID("Mary","-100012903"))
  val testData2= List(ID("Mike", "367-90-3452"),ID("Jeff","123"),ID("Larry","111111111"), ID("Jose", "456-09-2347"),ID("John","123456789"),ID("Naruto", "789340987"))
  val testData3= List(ID("Sasuke", "667234890"), ID("Sakura", "43"), ID("Lenny", "1ul345*l3"), ID("Naruto", "789340987"))
  val testData4= List(ID("Mike", "367-90-3452"), ID("Sasuke", "667234890"),ID("Naruto", "789340987"), ID("Boruto", "762-02-9876"), ID("Soma", "1123yes89"))


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

  test("testMaskString SSN") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession
    spark.sql("create database mask_db")
    val test1 = spark.sparkContext.parallelize(testData1).toDF
    val test2 = spark.sparkContext.parallelize(testData2).toDF
    val test3 = spark.sparkContext.parallelize(testData3).toDF
    val test4 = spark.sparkContext.parallelize(testData4).toDF


    val tbl1 = Mask.isItMasked(test1, "ssn", "ssnStr", true, 9, true)
    val tbl2 = Mask.isItMasked(test2, "ssn", "ssnStr", true, 9, true)
    val tbl2Cnt = tbl2.count()
    val tbl2Head = tbl2.filter(tbl2("name") === "Mike").head()

    val tbl3 = Mask.isItMasked(test3, "ssn", "ssnStr", true, 9, true)
    assert(tbl3.count() == 2)

    val tbl4 = Mask.isItMasked(test4, "ssn", "ssnStr", true, 9, true)
    assert(tbl2Cnt == tbl4.count())


    val lookup1 = sqlContext.sql("select * from ssnStr")
    println("testing lkup count")
    assert(lookup1.count() == 6)
    assert(tbl2Head == tbl4.filter(tbl4("name") === "Mike").head())
    println("testing tbl1 count")
    assert(tbl1.count() == 0)
    println("testing tbl3 count")

  }

 /* test("loadtest 1"){
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession
    assert(LoadTester.createDF(spark,10,100) == 90)
  }*/

  /*test("testMaskNumber") {
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

  }*/

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
