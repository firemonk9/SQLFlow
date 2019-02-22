package com.metlife.mask


import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql._

object Mask {

  val maskDatabase = "mask_db"
  val REAL_NUMBER = "REAL_NUMBER"
  val FULL_NAMES = "FULL_NAMES"

  val needMaskingColumn = "maskColumnVal?"
  val maskKeyColumn = "mask_key_secret_sp19"
  val maskValueColumn = "mask_value_sp19"
  val BASE_PATH_TEMP = "/tmp/"

  val ENCRYPT_PASSWD = "abcdefghijklmno1" // TODO need to pull from vault


  def main(args: Array[String]): Unit = {
    //create spark context
    //read properties
  }

  def maskFullNames(df: DataFrame, columnName: String, globalName: String, createIfNotExists: Boolean): DataFrame = {

    df
  }

  def mask(df: DataFrame, columnName: String, globalName: String, maskType: String, createIfNotExists: Boolean = true, numberOfDigits: Int = -1, localTesting:Boolean=false): DataFrame = {
    if (maskType == REAL_NUMBER)
      //maskNumber(df, columnName, globalName, createIfNotExists, numberOfDigits,localTesting)
      maskColumnVal(df, columnName, globalName, createIfNotExists, numberOfDigits,localTesting)
    else if (maskType == FULL_NAMES)
      maskFullNames(df, columnName, globalName, createIfNotExists)
    else
      df
  }


  import org.apache.spark.sql.{DataFrame, Row}
  import org.apache.spark.sql.types.{LongType, StructField, StructType}


  def dfZipWithIndex(
                      df: DataFrame,
                      offset: Int = 1,
                      colName: String = "id",
                      inFront: Boolean = true
                    ): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName, LongType, false)))
      )
    )
  }


  /**
    * This function encrypts the column and makes SHA-2 of the encrypted column.
    *
    * @param df
    * @param colName
    * @return
    */
  def getEncryptedColumn(df: DataFrame, colName: String): DataFrame = {
    val spark = df.sparkSession
    import org.apache.spark.sql.functions._


    val encryptStrUdf = udf[String,String](encryptStr)
    val edf = df.withColumn(colName, encryptStrUdf(df(colName)))
    edf.withColumn(colName, sha2(edf(colName), 512))
  }


  def encryptStr(col: String): String = {

    if(col == null)
      return col
    val password = ENCRYPT_PASSWD.getBytes()
    val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    val keySpec = new SecretKeySpec(password, "AES")
    cipher.init(Cipher.ENCRYPT_MODE, keySpec)
    val output = Base64.encodeBase64String(cipher.doFinal(col.getBytes()))
    output.trim

  }

  def maskedString(tin: String): String = {
    val special_c = """[a-zA-Z~!@#$^%&*\\(\\)_+=\\{\\}\\[\\]|;:\"'<,>.?`/\\\\]""".r
    val ones = "111111111"
    val nines = "999999999"
    val zeros = "000000000"
    val rm_dash = tin.replaceAll("-", "")

    if(tin.trim.isEmpty || tin.trim.length() < 4 || tin.trim.take(1) == "-" || rm_dash.trim.matches(ones) || rm_dash.trim.matches(nines) || rm_dash.trim.matches(zeros) || tin.trim.takeRight(4) == "0000"|| special_c.findFirstIn(tin.trim).toString != "None")
      "T"
    else
      "F"
  }

  def maskedName(name1: String): String = {
    val numbers = """[0-9]""".r
    val tbd = "TBD"
    val unknown = "UNKNOWN"
    val null_strin = "NULL"
    val na = "NA"
    val n_a = "N/A"
    val not_applic = "NOT APPLICABLE"

    if(name1 == null || name1.trim.isEmpty() || numbers.findFirstIn(name1).toString() != "None" || name1 == name1.toUpperCase()  || name1 == tbd || name1 == unknown || name1 == null_strin || name1 == na || name1 == n_a || name1 == not_applic)
      "T"
    else
      "F"

  }

  def getColumnStatus(df: DataFrame, colName: String): DataFrame = {
    val spark = df.sparkSession
    import org.apache.spark.sql.functions._

    if(colName == "ssn") {
      val maskedStrUdf = udf[String, String](maskedString)
      val edf = df.withColumn(needMaskingColumn, maskedStrUdf(df(colName)))
      //edf.withColumn(colName, sha2(edf(colName), 512))
      edf
    }
    else if(colName == "name"){
      val maskedStrUdf = udf[String, String](maskedName)
      val edf = df.withColumn(needMaskingColumn, maskedStrUdf(df(colName)))
      //edf.withColumn(colName, sha2(edf(colName), 512))
      edf
    }else{
      throw new Exception("This is not a column to mask")
    }

  }


  def maskColumnVal(dfToMask: DataFrame, columnName: String, tempGlobalName: String, createIfNotExists: Boolean = true, numberOfDigits: Int, localTesting:Boolean): DataFrame = {
    import org.apache.spark.sql.functions.max

    val globalName = (tempGlobalName).toLowerCase()
    println(" Global mask table  " + globalName)
    println("colName: " + columnName)
    println("dfToMask")
    dfToMask.show()
    dfToMask.sparkSession.sql("use " + maskDatabase)
    println(" Looking for : " + globalName + "   and found status : " + dfToMask.sparkSession.catalog.tableExists(globalName))

    val columnType = dfToMask.select(columnName).schema.head.dataType
    val frame123: DataFrame = dfToMask.withColumn(columnName, dfToMask(columnName).cast(StringType))
    val dfToCheck = getColumnStatus(frame123, columnName)
    val needMasking = dfToCheck.filter(dfToCheck(needMaskingColumn).like("F"))
    val noMasking = dfToCheck.filter(dfToCheck(needMaskingColumn).like("T")).drop(needMaskingColumn)
    val dfToMaskEnc = getEncryptedColumn(needMasking, columnName).drop(needMaskingColumn)




    if (!dfToMask.sparkSession.catalog.tableExists(globalName) && createIfNotExists == false) {
      throw new Exception("Check the name " + globalName + " or set createIfNotExists to true if new table should be created.")
    }

    val (begin: Int, toAppend: Option[DataFrame], newKeysDf: DataFrame) = if (!dfToMask.sparkSession.catalog.tableExists(globalName) && createIfNotExists) {
      val begin = ("1" + "0" * numberOfDigits).toInt
      (begin, None, dfToMaskEnc)
    }
    else {

      val cacheTableGlobal = dfToMaskEnc.sparkSession.sql("select * from " + globalName)
      var maxValue = 0.0
      var begin = 0

      if (cacheTableGlobal.count() == 0) {
        begin = ("1" + "0" * numberOfDigits).toInt
      } else {
        maxValue = cacheTableGlobal.agg(max(cacheTableGlobal(maskValueColumn))).head.getLong(0)
        begin = (maxValue + 1).toInt
      }

      val jdf = dfToMaskEnc.join(cacheTableGlobal, dfToMaskEnc(columnName) === cacheTableGlobal(maskKeyColumn), "left_outer")


      val newKeysFrame = jdf.filter(jdf(maskKeyColumn).isNull).drop(maskKeyColumn, maskValueColumn) //.localCheckpoint(true)


      val foundKeysFrame = jdf.filter(jdf(maskKeyColumn).isNotNull).drop(columnName, maskKeyColumn).withColumnRenamed(maskValueColumn, columnName) //.localCheckpoint(true)

      //TODO Temp hack to run in local.
      val (nnnew, nnfound) = if (localTesting) {
        newKeysFrame.write.mode(SaveMode.Overwrite).parquet(BASE_PATH_TEMP + "new_keys")
        val newKeysDf = jdf.sparkSession.read.parquet(BASE_PATH_TEMP + "new_keys")
        foundKeysFrame.write.mode(SaveMode.Overwrite).parquet(BASE_PATH_TEMP + "found_keys")
        val foundKeys = jdf.sparkSession.read.parquet(BASE_PATH_TEMP + "found_keys")
        newKeysDf.count()
        foundKeys.count()

        (newKeysDf, foundKeys)
      } else (newKeysFrame, foundKeysFrame)

      (begin, Some(nnfound), nnnew)
    }

    val newColumn = columnName + "_" + "SAM_QRE_321_455"
    val t2 = dfZipWithIndex(newKeysDf, begin, newColumn, false)
    val frame = t2.select(columnName, newColumn).toDF(maskKeyColumn, maskValueColumn)

    frame.write.mode("append").saveAsTable(globalName)

    val t3 = t2.drop(columnName).withColumnRenamed(newColumn, columnName)

    val finalRes = if (toAppend.isDefined) {
      toAppend.get.show()
      toAppend.get.union(t3)
    } else t3

    if (columnName == "ssn") {
    println("This is finalRes")
    finalRes.show()
    val finalUnion = finalRes.union(noMasking)
    finalUnion
   //finalUnion.withColumn(columnName, finalRes(columnName).cast(columnType))
    }else if (columnName == "name"){
      val master_name = finalRes.sparkSession.sql("select * from name_lookup" )
      val name_change = finalRes.join(master_name, finalRes("name") === master_name("id"), "left_outer").drop("name","id").withColumnRenamed("n_name", columnName)
      val cols = name_change.columns
      val finalUnion = name_change.union(noMasking.select(cols.head, cols.tail: _*))
      finalUnion

    }else{
      throw new Exception("This is not a Masked column." + columnName)
    }
  }


}
