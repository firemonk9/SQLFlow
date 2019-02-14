package com.metlife.mask


import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql._

object Mask {

  val maskDatabase = "MASK_DB"
  val REAL_NUMBER = "REAL_NUMBER"
  val maskKeyColumn = "mask_key_secret_sp19"
  val maskValueColumn = "mask_value_sp19"
  val BASE_PATH_TEMP = "c:/Users/dpeechara/Desktop/"

  val ENCRYPT_PASSWD = "abcdefghijklmno1" // TODO need to pull from vault


  def main(args: Array[String]): Unit = {
    //create spark context
    //read properties
  }

  def maskColumn(df: DataFrame, columnName: String, globalName: String, maskType: String, createIfNotExists: Boolean = true, numberOfDigits: Int = -1): DataFrame = {
    if (maskType == REAL_NUMBER)
      maskNumber(df, columnName, globalName, createIfNotExists, numberOfDigits)
    else
      df
  }


  def maskNumber(dfToMask: DataFrame, columnName: String, tempGlobalName: String, createIfNotExists: Boolean = false, numberOfDigits: Int): DataFrame = {
    import org.apache.spark.sql.functions.max

    val globalName = maskDatabase+"."+tempGlobalName

    // step 1 : mask the column
    val columnType = dfToMask.select(columnName).schema.head.dataType
    val frame123:DataFrame = dfToMask.withColumn(columnName, dfToMask(columnName).cast(StringType))
    val dfToMaskEnc = getEncryptedColumn(frame123, columnName)

    if (!dfToMask.sparkSession.catalog.tableExists(globalName) && createIfNotExists == false) {
      throw new Exception("Check the name " + globalName + " or set createIfNotExists to true if new table should be created.")
    }

    val (begin: Int, toAppend: Option[DataFrame], newKeysDf: DataFrame) = if (!dfToMask.sparkSession.catalog.tableExists(globalName) && createIfNotExists) {
      val begin = ("1" + "0" * numberOfDigits).toInt
      (begin, None, dfToMaskEnc)
    }
    else  {

      val cacheTableGlobal = dfToMaskEnc.sparkSession.sql("select * from " + globalName)
      cacheTableGlobal.show(false)
      val maxValue = cacheTableGlobal.agg(max(cacheTableGlobal(maskValueColumn))).head.getLong(0)
      val begin = (maxValue + 1).toInt

      val jdf = dfToMaskEnc.join(cacheTableGlobal, dfToMaskEnc(columnName) === cacheTableGlobal(maskKeyColumn),"left_outer")

      val newKeysFrame = jdf.filter(jdf(maskKeyColumn).isNull).drop(maskKeyColumn, maskValueColumn)//.localCheckpoint(true)
      val foundKeysFrame = jdf.filter(jdf(maskKeyColumn).isNotNull).drop(columnName, maskKeyColumn).withColumnRenamed(maskValueColumn, columnName)//.localCheckpoint(true)

      //TODO Temp hack to run in local.
      newKeysFrame.write.mode(SaveMode.Overwrite).parquet(BASE_PATH_TEMP+"new_keys")
      val newKeysDf = jdf.sparkSession.read.parquet(BASE_PATH_TEMP+"new_keys")
      foundKeysFrame.write.mode(SaveMode.Overwrite).parquet(BASE_PATH_TEMP+"found_keys")
      val foundKeys =jdf.sparkSession.read.parquet(BASE_PATH_TEMP+"found_keys")


     (begin, Some(foundKeys), newKeysDf)
    }


    val newColumn = columnName + "_" + "SAM_QRE_321_455"
    //val t1 = newKeysDf.withColumn(newColumn, monotonically_increasing_id)
    val t2 = dfZipWithIndex(newKeysDf, begin, newColumn, false)
    val frame = t2.select(columnName, newColumn).toDF(maskKeyColumn, maskValueColumn)



    val toSaveAsTable = frame.write.mode("append").saveAsTable(globalName)
    val t3 = t2.drop(columnName).withColumnRenamed(newColumn, columnName)

    t3.show()
    val finalRes = if (toAppend.isDefined) {
      toAppend.get.show()
      toAppend.get
      toAppend.get.union(t3)
    } else t3

    finalRes.show()
    finalRes.withColumn(columnName, finalRes(columnName).cast(columnType))



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


}
