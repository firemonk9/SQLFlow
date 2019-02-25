package com.metlife.mask

import com.holdenkarau.spark.testing.SharedSparkContext
import com.metlife.loadtest.LoadTester
import com.metlife.mask.Mask.{maskKeyColumn, maskValueColumn}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql._



case class Person3(name:String, ssn:Int, addr:String)
case class ID(name:String, ssn: String)
case class LKUP(id:String, n_name:String)
case class testme(ssn:String, name:String)

class MaskTest extends FunSuite with SharedSparkContext  {


  val sampleData = List(Person3("John",123456,"abc Cary, nc"),Person3("Rob",123457,"abc Cary, nc"),Person3("Mary",123458,"abc Cary, nc"))
  val sampleDataNew = List(Person3("John",123456,"abc Cary, nc"),Person3("Harry",123434,"abc Cary, nc"))

  val sampleDataNeg = List(Person3("John",123456,"abc Cary, nc"),Person3("Rob",123457,"abc Cary, nc"),Person3("Mary",123458,"abc Cary, nc"),Person3("GREEN",-123458,"abc Cary, nc"))
  val sampleDataNewNeg = List(Person3("John",123456,"abc Cary, nc"),Person3("Harry",123434,"abc Cary, nc"))

  //test for masking snn
  val testData1 = List(ID("John","53A263248"),ID("Rob","457&1@190"),ID("Mary","-100012903"))
  val testData2= List(ID("Mike", "367-90-3452"),ID("Jeff","123"),ID("Larry","111111111"), ID("Jose", "456-09-2347"),ID("John","123456789"),ID("Naruto", "789340987"))
  val testData3= List(ID("Sasuke", "667234890"), ID("Sakura", "43"), ID("Lenny", "1ul345*l3"), ID("Naruto", "789340987"))
  val testData4= List(ID("Mike", "367-90-3452"), ID("Sasuke", "667234890"),ID("Naruto", "789340987"), ID("Boruto", "762-02-9876"), ID("Soma", "1123yes89"))

  //test for masking names
  val name_lkup_sample = List(LKUP("1000000000", "HARRY POTTER"),LKUP("1000000001", "DR.STEPHEN STRANGE"),LKUP("1000000002", "MR.RONALD WEASLY"),LKUP("1000000003", "MS.MARRY POPPINS"),LKUP("1000000004", "NARUTO UZUMAKI"),LKUP("1000000005", "SASUKE UCHIA"),LKUP("1000000006", "MIKE JONES"),LKUP("1000000007", "PROF DWAYNE CARTER"),LKUP("1000000008", "SISTER MARY ELLIS"),LKUP("1000000009", "TOM BRADY"),LKUP("1000000010", "DREW BREES"),LKUP("1000000011", "CAPT DARTH VADER"),LKUP("1000000012", "MRS.MARGRET MAY"),LKUP("1000000013", "DR MARGARET MAY"),LKUP("1000000014", "LILLY ALDRIDGE"),LKUP("1000000015", "MS RACHEL GREEN"),LKUP("1000000016", "PROF TED MOSBY"),LKUP("1000000017", "WILLIE JONES IV"),LKUP("1000000018", "JOHN ADDAMS JR"),LKUP("1000000019", "LUKE SKYWALKER"))
  val testName1 = List(Person3("John",123456,"abc Cary, nc"),Person3("Rob",123457,"abc Cary, nc"),Person3("Mary",123458,"abc Cary, nc"))
  val testName2 = List(Person3("John",123456,"abc Cary, nc"),Person3("Harry",123434,"abc Cary, nc"), Person3("Kerry", 87639, "abc Cary, nc"))
  val testName3 = List(Person3("John",123456,"abc Cary, nc"),Person3("     ",123454,"abc Carry, nc"), Person3(null, 98765, "abc Cary, nc") , Person3("TBD", 456789, "abc Cary, nc") , Person3("NA", 25467, "abc Cary, nc"))


  def createIfNotExists(spark:SparkSession):Unit={
    if(spark.sql("show databases").filter("databaseName = 'mask_db'").count() == 0)
      spark.sql("create database mask_db")

    //.filter("")
  }

  test("testMaskNumber") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession
    createIfNotExists(spark)
    //    spark.sql("create database mask_db")
    val sampleF = spark.sparkContext.parallelize(sampleData).toDF
    val sampleFNew = spark.sparkContext.parallelize(sampleDataNew).toDF
    val edf = Mask.maskColumnVal(sampleF,"ssn","SSN",true,9,true,MaskType.REAL_NUMBER)
    edf.show()

    val ndf = Mask.maskColumnVal(sampleFNew,"ssn","SSN",true,9,true,MaskType.REAL_NUMBER)
    ndf.show()
    assert(edf.filter(edf("name") === "John").head() == ndf.filter(ndf("name") === "John").head())
    assert(edf.count() == sampleF.count())
    assert(ndf.count() == sampleFNew.count())

  }

  test("testMaskString SSN") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession
    //    spark.sql("create database mask_db")
    createIfNotExists(spark)
    val test1 = spark.sparkContext.parallelize(testData1).toDF
    val test2 = spark.sparkContext.parallelize(testData2).toDF
    val test3 = spark.sparkContext.parallelize(testData3).toDF
    val test4 = spark.sparkContext.parallelize(testData4).toDF


    val tbl1 = Mask.maskColumnVal(test1, "ssn", "ssnStr", true, 9, true,MaskType.SSN)
    tbl1.show()
    val tbl2 = Mask.maskColumnVal(test2, "ssn", "ssnStr", true, 9, true,MaskType.SSN)
    //val tbl2Cnt = tbl2.count()
    tbl2.show()
    val tbl2Head = tbl2.filter(tbl2("name") === "Mike").head()


    val tbl3 = Mask.maskColumnVal(test3, "ssn", "ssnStr", true, 9, true,MaskType.SSN)
    tbl3.show()
    //assert(tbl3.count() == 2)

    val tbl4 = Mask.maskColumnVal(test4, "ssn", "ssnStr", true, 9, true,MaskType.SSN)
    tbl4.show()
    //assert(tbl2Cnt == tbl4.count())

    /*
        val lookup1 = sqlContext.sql("select * from ssnStr")
        println("testing lkup count")
        assert(lookup1.count() == 6)
        assert(tbl2Head == tbl4.filter(tbl4("name") === "Mike").head())
        println("testing tbl1 count")
        assert(tbl1.count() == 0)
        println("testing tbl3 count")*/

  }
  test("testMaskString Name") {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._
    val spark = sqlContext.sparkSession
    createIfNotExists(spark)
    //    spark.sql("create database mask_db")
    val lkup = spark.sparkContext.parallelize(name_lkup_sample).toDF
    lkup.createOrReplaceTempView("name_lookup")
    //spark.sql("select * from name_lookup").show()
    val test1 = spark.sparkContext.parallelize(testName1).toDF
    val test2 = spark.sparkContext.parallelize(testName2).toDF
    val test3 = spark.sparkContext.parallelize(testName3).toDF
    /*val tbl1 = Mask.maskColumnVal(test1, "name", "nameStr", true, 9, true)
    println("this is tbl1")
    tbl1.show()
    println("Tbl1 test columns are same name")
    assert(tbl1.columns.toList.sorted ==test1.columns.toList.sorted)
    println("Tbl1 should equal test1 count")
    assert(tbl1.count() ==test1.count())


    val tbl2 = Mask.maskColumnVal(test2, "name", "nameStr", true, 9, true)
    val harry = tbl2.filter(tbl2("name") === "HARRY POTTER").head()
    println("this is tbl2")
    tbl2.show()
    println("Tbl2 should equal test2 count")
    assert(tbl2.count() ==test2.count())
    println("Tbl2 has a duplicate from Tbl1 check names")
    assert(tbl2.filter(tbl2("name") === "HARRY POTTER").head() == tbl1.filter(tbl1("name") === "HARRY POTTER").head())
    */
    val tbl3 = Mask.maskColumnVal(test3, "name", "nameStr", true, 9, true,MaskType.FULL_NAME)
    println("this is tbl3")
    tbl3.show()
    println("This table should be empty")
    assert(tbl3.count == 5)
    //println("Tbl3 has a duplicate from Tbl2 check names")
    //assert(tbl3.filter(tbl3("name") === "HARRY POTTER").head() == harry)





    //jdf.filter(jdf(maskKeyColumn).isNotNull).drop(columnName, maskKeyColumn).withColumnRenamed(maskValueColumn, columnName)
    // dfToMaskEnc.join(cacheTableGlobal, dfToMaskEnc(columnName) === cacheTableGlobal(maskKeyColumn),"left_outer")



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

