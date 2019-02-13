package com.metlife.wf

import javax.xml.bind.DatatypeConverter
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.common.model._

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * Created by dhiraj
  */


object WorkFlowUtil {


  val driverMap = Map("mysql" -> "com.mysql.jdbc.Driver", "oracle" -> "oracle.jdbc.driver.OracleDriver", "db2" -> "com.ibm.db2.jdbc.app.DB2Driver", "sybase" -> "com.sybase.jdbc.SybDriver", "teradata" -> "com.teradata.jdbc.TeraDriver", "sqlserver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver", "postgresql" -> "org.postgresql.Driver")
  val basicTypes = List("short", "int", "integer", "float", "long", "double", "decimal", "boolean")
  val basicNumericTypes = List("short", "int", "integer", "float", "long", "double", "decimal")
  val numPattern: Regex = ".[0]+$".r
  val DELIMITER = "__"
  var machineConsumable = true


  /**
    * Create Dataframe from input source.
    *
    * @param fileSource
    * @param sqlContext
    * @return
    */
  def createDataFrame(fileSource: FileSource, sqlContext: SQLContext): DataFrame = {

    val df: DataFrame = if (fileSource.datasetFormat == FileFormats.CSV) {
      val inferSchema = if (fileSource.inferSchema.isDefined) fileSource.inferSchema.get else false
      sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", inferSchema.toString).option("header", "" + fileSource.header.get).option("delimiter", getDelimiter(fileSource.datasetDelimiter.get)).load(fileSource.datasetPath) //.save(outputDir)

    } else if (fileSource.datasetFormat == "CSV-SNAPPY") {
      val inferSchema = if (fileSource.inferSchema.isDefined) fileSource.inferSchema.get else false
      sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", inferSchema.toString).option("header", "" + fileSource.header.get).option("codec", "snappy").option("delimiter", getDelimiter(fileSource.datasetDelimiter.get)).load(fileSource.datasetPath) //.save(outputDir)
    }
    else if (fileSource.datasetFormat == FileFormats.JSON) {
      sqlContext.read.json(fileSource.datasetPath) //format("com.databricks.spark.csv").option("header", "" + fileSource.header).option("delimiter", fileSource.srcDelimiter).load(fileSource.srcPath) //.save(outputDir)
    }
    else if (fileSource.datasetFormat == (FileFormats.PARQUET) || fileSource.datasetFormat == (FileFormats.DATAQ)) {
      sqlContext.read.parquet(fileSource.datasetPath)
    } else if (fileSource.datasetFormat == FileFormats.ORC) {
      sqlContext.read.format(FileFormats.ORC).load(fileSource.datasetPath)
    } else if (fileSource.datasetFormat == FileFormats.HIVE) {

      //val hc = if(sqlContext.isInstanceOf[HiveContext]) sqlContext.asInstanceOf[HiveContext] else sqlContext

      val hc = sqlContext
      if (fileSource.transformationSQL.isDefined && fileSource.transformationSQL.get) {
        hc.sql(fileSource.datasetPath)
      } else hc.sql("select * from " + fileSource.datasetPath)

    } else if (fileSource.datasetFormat == FileFormats.JDBC) {

      val dbProperties = new java.util.Properties()
      dbProperties.setProperty("user", fileSource.jdbcData.get.jdbcUser)
      dbProperties.setProperty("password", fileSource.jdbcData.get.jdbcPassword)


      val driverClass: String = getDriverClass(fileSource)

      if (driverClass == "") throw new Exception("Driver class not found for " + fileSource.jdbcData.get.jdbcUrl)
      val df = if (fileSource.jdbcData.get.min.isDefined && fileSource.jdbcData.get.max.isDefined && fileSource.jdbcData.get.partitionColumn.isDefined && fileSource.jdbcData.get.numPartitions.isDefined) {
        //TODO test this
        sqlContext.read.format(FileFormats.JDBC).option("url", fileSource.jdbcData.get.jdbcUrl).option("driver", driverClass).option("dbtable", getTableName(fileSource.datasetPath)).option("user", fileSource.jdbcData.get.jdbcUser).option("password", fileSource.jdbcData.get.jdbcPassword).load()
      } else
        sqlContext.read.format(FileFormats.JDBC).option("url", fileSource.jdbcData.get.jdbcUrl).option("driver", driverClass).option("dbtable", getTableName(fileSource.datasetPath)).option("user", fileSource.jdbcData.get.jdbcUser).option("password", fileSource.jdbcData.get.jdbcPassword).load()


      df
    } else if (fileSource.datasetFormat == "NETEZZA") {
      val opts = Map("url" -> fileSource.jdbcData.get.jdbcUrl,
        "user" -> fileSource.jdbcData.get.jdbcUser,
        "password" -> fileSource.jdbcData.get.jdbcPassword,
        "dbtable" -> fileSource.datasetPath,
        "numPartitions" -> fileSource.jdbcData.get.numPartitions.get.toString)
      val df = sqlContext.read.format("com.ibm.spark.netezza")
        .options(opts).load()
      df
    } else {
      null
    }
    if (fileSource.columns.isDefined) {
      val l = List("A", "B")
      df.toDF(fileSource.columns.get.toList: _*)
    } else
      df
  }


  def writeDataframe(dataFrameTemp: DataFrame, fileSource: FileSource, output: String, sqlContext: SQLContext): Boolean = {
    val dataFrame = if (fileSource.repartition.isDefined) dataFrameTemp.repartition(fileSource.repartition.get) else if (fileSource.coalesce.isDefined) dataFrameTemp.coalesce(fileSource.coalesce.get) else dataFrameTemp
    val mode: Option[SaveMode] = if (fileSource.mode.isDefined) fileSource.mode.get.toLowerCase match {
      case "append" => Some(SaveMode.Append)
      case "overwrite" => Some(SaveMode.Overwrite)
      case "ignore" => Some(SaveMode.Ignore)
      case "errorifexists" => Some(SaveMode.ErrorIfExists)
      //TODO this is a hack. Need to fix
      case _ => Some(SaveMode.Append); //throw new Exception("Mode not found "+mode+" accepted modes are "+SaveMode.Append+", "+SaveMode.Overwrite+", "+SaveMode.Ignore+" ,"+SaveMode.ErrorIfExists+"")

    } else Some(SaveMode.Overwrite)
    if (fileSource.datasetFormat == FileFormats.CSV) {

      //val inferSchema = if (fileSource.inferSchema.isDefined) fileSource.inferSchema.get else false
      dataFrame.write.format("csv").mode(mode.get).option("header", "" + fileSource.header.get).option("delimiter", getDelimiter(fileSource.datasetDelimiter.get)).save(output) //.save(outputDir)

    }
    else if (fileSource.datasetFormat == FileFormats.JSON) {
      dataFrame.write.mode(mode.get).json(output)
    }
    else if (fileSource.datasetFormat == (FileFormats.PARQUET) || fileSource.datasetFormat == (FileFormats.DATAQ)) {
      dataFrame.write.mode(mode.get).parquet(output)
    } else if (fileSource.datasetFormat == FileFormats.HIVE) {

      dataFrame.write.mode(mode.get).saveAsTable(output)
    } else if (fileSource.datasetFormat == FileFormats.JDBC) {
      val dbProperties = new java.util.Properties()
      dbProperties.setProperty("user", fileSource.jdbcData.get.jdbcUser)
      dbProperties.setProperty("password", fileSource.jdbcData.get.jdbcPassword)
      val driverClass: String = getDriverClass(fileSource)
      if (driverClass == "") throw new Exception("Driver class not found for " + fileSource.jdbcData.get.jdbcUrl)
      dataFrame.write.mode(mode.get).format(FileFormats.JDBC).option("url", fileSource.jdbcData.get.jdbcUrl).option("driver", driverClass).option("dbtable", output).option("user", fileSource.jdbcData.get.jdbcUser).option("password", fileSource.jdbcData.get.jdbcPassword).save()

    }
    true
  }

  def getDriverClass(fileSource: FileSource): String = {
    val driverClass: String = if (fileSource.jdbcData.get.jdbcDriverClass.isDefined) fileSource.jdbcData.get.jdbcDriverClass.get else {
      val driver = driverMap.keys.map(key => if (fileSource.jdbcData.get.jdbcUrl.toLowerCase.contains(key)) driverMap.get(key) else None).filter(a => a.isDefined)
      if (!fileSource.jdbcData.get.jdbcDriverClass.isDefined && driver.size > 0) driver.head.get else ""
    }
    driverClass
  }

  protected def getTableName(userTableInfo: String): String = {
    val userTableInfol = userTableInfo.trim
    val tableName = if (userTableInfol.trim.indexOf(" ") > 0) {
      if (userTableInfol.toLowerCase().startsWith("select")) {
        "( " + userTableInfol + " ) user_sql_table "
      }
      else userTableInfol
    } else userTableInfol

    println(" :: tableName ++++++ " + tableName)
    tableName
  }

  //TODO this is temp hack.
  private[wf] def getDelimiter(delimiter: String): String = {
    delimiter match {
      case "001" => "\001"
      case _ => delimiter
    }
  }


  /**
    * Yet to test for non String types.
    *
    * @param df1t1
    * @param filterData
    * @return
    */
  def filterDF(df1t1: DataFrame, filterData: Option[List[RecordFilter]] = None, filterSql: Option[String] = None, sqlContext: SQLContext = null): DataFrame = {

    if (filterData.isDefined) {
      val filterQueries: String = filterData.get.map(a => a.columns.map(b => " " + b.column + " = \"" + b.value + "\" and").reduce(_ + _).replaceAll("^", "(").replaceAll("and$", ")")).reduce((a1, a2) => a1 + " or " + a2)
      //.replaceAll("and$", "")
      val conditionExpr: String = "( " + filterQueries + " )"
      println("  ####  " + conditionExpr)
      df1t1.filter(conditionExpr)
    } else if (filterSql.isDefined) {
      val tableName = getTableNameFromSql(filterSql.get)
      df1t1.registerTempTable(tableName)
      println(" the filter sql " + filterSql.get + "  and executing table name " + tableName)
      sqlContext.sql(filterSql.get)
    } else
      df1t1
  }


  def getTableNameFromSql(filterSql: String): String = {
    val arr = filterSql.replaceAll(" +", " ").split(" ").map(a => a.toLowerCase)
    val index = arr.indexOf("from")
    val tableName = arr(index + 1)
    tableName
  }


  /**
    * Limitations : Only works for strings,Int,Float,Double. TODO add other data types.
    * Transform the input data (src & destination) by user provided partial functions.
    *
    * @param srcDF
    * @param dataTransformRule Column Name -> the code to be executed to transformt this column.
    * @return
    */
  def transformInputData(srcDF: DataFrame, dataTransformRule: Option[List[DataTransformRule]], colMap: Map[String, String]): DataFrame = {
    val myFunc = "value"
    val columns = srcDF.columns
    val sqlContext = srcDF.sqlContext
    var sdf: DataFrame = srcDF

    println(" begin transformInputData :: ")
    //println("the col map is " + colMap)

    dataTransformRule.get.foreach(a => {

      if (a.columnCasts.isDefined) {
        val stringToString = Map[String, String]()
        sdf = columnCasts(sdf, Some(a.columnCasts.get), stringToString)
      }

      if (a.transformSQL.isDefined) {
        val tableName = getTableNameFromSql(a.transformSQL.get)
        srcDF.createOrReplaceTempView(tableName)
        sdf = sqlContext.sql(a.transformSQL.get)

      }
      else if (a.columnExpressionSqlStatement.isDefined) {


        val filterdCols: List[String] = if (a.column.isDefined) a.column.get.filter(col => colMap.contains(col) || columns.toSeq.contains(col)) else List()
        println("filterdCols  " + filterdCols)
        if (a.columnExpressionSqlStatement.isDefined) {


          val funct = if (a.base64.isDefined && a.base64.get) new String(DatatypeConverter.parseBase64Binary(a.columnExpressionSqlStatement.get), "utf-8").replace("\n", " ") else a.columnExpressionSqlStatement.get
          println("the rule is " + funct)
          //a.columnExpressionSqlStatement.get

          val whereInSql = " where "
          val funct1 = if (funct.indexOf(whereInSql) > 0) Tuple2(funct.substring(0, funct.indexOf(whereInSql)), Some(funct.substring(funct.indexOf(whereInSql), funct.length))) else Tuple2(funct, None)
          val strings = splitSQLString(funct1._1)
          //.split(",").toList
          val sqlCols = strings.map(a => {
            val indx = a.toLowerCase().indexOf("as") + 2
            val col = a.substring(indx, a.length).trim
            val coln = if (col.endsWith(",")) col.substring(0, col.length - 1) else col
            // val ll = colMap.keys.filter(ac=> col.equalsIgnoreCase(ac))
            val l = sdf.columns.filter(ac => coln.equalsIgnoreCase(ac)).toList
            l.headOption

          }).filter(a => a.isDefined).map(a => a.get) //.flatten

          println("sqlCols :  " + sqlCols.toList)

          val arr = sdf.columns.toList diff sqlCols
          val strings1 = splitSQLString(funct1._1)
          val strings2 = strings1.map(a => {
            val indx = a.toLowerCase().indexOf("as") + 2
            val col = a.substring(indx, a.length).trim
            val coln = if (col.endsWith(",")) col.substring(0, col.length - 1) else col
            //val ll = colMap.keys.filter(ac=> col.equalsIgnoreCase(ac))
            val l = sdf.columns.filter(sdfCol => sdfCol.equalsIgnoreCase(coln)).toList
            if (l.nonEmpty) Some(a.replace(" " + coln, " " + l.head)) else None

          }).filter(a => a.isDefined).map(a => a.get)
          //.split(",").toList
          val afunct = strings2.mkString("")

          // println("afunct "+afunct)

          val selectCols = arr.mkString(",") + "," + afunct
          //println("the select for query is " + selectCols)
          sdf.registerTempTable("my_tab_321zd")
          sdf = if (funct1._2.isDefined) {

            val columns = sdf.columns.mkString(",")
            val wheredfTrans = sdf.sqlContext.sql("select " + selectCols + " from my_tab_321zd " + funct1._2.get)
            val wheredf = sdf.sqlContext.sql("select " + columns + " from my_tab_321zd " + funct1._2.get)
            val fullDf = sdf.sqlContext.sql("select " + columns + " from my_tab_321zd ")
            fullDf.except(wheredf).unionAll(wheredfTrans)

          } else {
            sdf.sqlContext.sql("select " + selectCols + " from my_tab_321zd ")
          }

        }


      }

    })
    sdf
  }



  /**
    * casts columns to user defined types.
    * Does not handle exceptions
    *
    * @param df
    * @param columnCasts
    * @return
    */
  private[wf] def columnCasts(df: DataFrame, columnCasts: Option[Array[Cast]], colMap: Map[String, String]): DataFrame = {

    if (columnCasts.isDefined) {
      val arr = columnCasts.get
      var ndf: DataFrame = df
      println("array size is " + arr)
      arr.foreach(a => {
        val newcol: Option[String] = Some(colMap.getOrElse(a.columnName, a.columnName))
        println(" a " + a + "  newcol " + newcol)
        if (newcol.isDefined) {
          val cols = ndf.columns.filter(b => b != newcol.get)
          ndf = ndf.withColumn(newcol.get, ndf(newcol.get).cast(a.columnType))
        }
        else {
          println(" column to cast " + a.columnName + "  not found in the map " + colMap)
        }
      })
      ndf
    } else
      df
  }


  def joinDF(joinJob: JoinJob, sqlContext: SQLContext): Tuple2[DataFrame, Long] = {

    val ndf = sqlContext.sql(joinJob.joinSql.get).cache()
    val count = ndf.count()
    (ndf, count)
  }


  def checkRepartition(df: DataFrame): DataFrame = {
    val colsCount = df.columns.toList.length
    import df.sparkSession.implicits._

    val partCount = df.rdd.mapPartitionsWithIndex { case (i, rows) => Iterator((i, rows.size)) }.toDF("partition_number", "number_of_records").select("number_of_records").collect().map(row => row.getInt(0))
    val factor: Int = ((partCount.max * colsCount) / 1000000).toInt
    if (factor > 1) {
      println(" Need to repartition the data to " + factor * df.rdd.partitions.length + " from partition existing size : " + df.rdd.partitions.length + "");
      df.repartition(factor * df.rdd.partitions.length)
    } else df

  }


  private[wf] def splitSQLString(sql: String): List[String] = {

    def lsplit(pos: List[Int], str: String): List[String] = {
      val (rest, result) = pos.foldRight((str, List[String]())) {
        case (curr, (s, res)) =>
          val (rest, split) = s.splitAt(curr)
          (rest, split :: res)
      }
      rest :: result
    }

    var index = 0
    var loop = true
    var indexes = new ListBuffer[Int]()
    while (loop) {
      val l = sql.indexOf(" as ", index) + 4
      val markIndex = sql.indexOf(",", l)
      if (markIndex <= 0) {
        loop = false

      } else {
        index = markIndex + 1
        indexes += index
      }
    }
    lsplit(indexes.toList, sql)
  }

}
