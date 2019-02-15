package com.metlife.wf

import java.nio.file.{Files, Paths}

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.common.model._
import org.wf.util.FileUtil

/**
  * Created by dhiraj
  */
object SqlWorkFlowMain extends SparkInit {


  var submittedTime: Option[Long] = None
  var LICENSED: Boolean = false
  var jobId: String = null
  var execId: String = null
  var projectId: String = null
  //  var base64enCoded: Boolean = false
  var exceptionOccured: Boolean = false

  def setSparkContext(lsparkContext: SparkSession): Unit = {
    sparkContextLivy = lsparkContext
  }

  var sparkContextLivy: SparkSession = null

  def main(args: Array[String]) {

    System.out.println("args = " + args.toList)
    val argsMap = parseArgs(args)

    val inputDiffPropsFile = argsMap.getOrElse("INPUT_FILE", "")

    val debug = if (argsMap.getOrElse("DEBUG", "false") == "true") true else false
    val local = if (argsMap.getOrElse("local", "false") == "true") true else false
    val spark = if (sparkContextLivy != null) {
      sparkContextLivy
    } else if (local == false) {
      SparkSession.builder().appName("SQLWorkFlow").enableHiveSupport().getOrCreate()
    }
    else {
      SparkSession.builder().appName("SQLWorkFlow").master("local").getOrCreate()
    }

    val sQLContext = spark.sqlContext

    import java.util.Properties

    import org.apache.log4j.PropertyConfigurator

    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("log4j.properties"))
    PropertyConfigurator.configure(props)
    val hdfsOutput: Boolean = if (argsMap.getOrElse("HDFS_OUTPUT", "true") == "true") true else false

    try {
      processFlow(inputDiffPropsFile, sQLContext, hdfsOutput, debug)
    } catch {
      case e: Exception => e.printStackTrace(); throw e;
    } finally {
      spark.stop()
    }
  }


  def processFlow(inputFile: String, sqlContext: SQLContext, hdfsOutput: Boolean = true, debug: Boolean): Unit = {
    val filesCompare: InputFlow = csvToWF(inputFile, sqlContext, true) //if (jsonContent.isDefined) convertCSVtoInputFlow(jsonContent.get) else throw new IllegalArgumentException("unable to read input JSON " + inputFile)
    new JobsExecutor(filesCompare, sqlContext, debug).processChains()
  }

  def writeToFile(jsonStr: String, filePath: Option[String], hdfsOutput: Boolean, sqlContext: SQLContext): Unit = {
    if (filePath.isDefined) {
      if (hdfsOutput) FileUtil.writeToFile(filePath, jsonStr, sqlContext.sparkContext.hadoopConfiguration) else FileUtil.writeToTextFile(filePath.get, jsonStr)
    }
  }

  def compress(input: String): Array[Byte] = {

    import java.util.zip.{ZipEntry, ZipOutputStream}

    val path = System.getProperty("java.io.tmpdir") + "/" + "flow_result.json"
    val baos = new ByteArrayOutputStream()
    val zos = new ZipOutputStream(baos)
    /* File is not on the disk, test.txt indicates
        only the file name to be put into the zip */
    val entry = new ZipEntry("flow_result.json")
    zos.putNextEntry(entry)
    zos.write(input.getBytes)
    zos.closeEntry()
    baos.toByteArray

  }


  def parseArgsJava(args: Array[String]): java.util.Map[String, String] = {
    import scala.collection.JavaConverters._
    parseArgs(args).asJava
  }

  def parseArgs(args: Array[String]): scala.collection.Map[String, String] = {
    val v: scala.collection.Map[String, String] = args.filter(a => a.contains("=")).map(a => {
      val ar = a.splitAt(a.indexOf("="))
      val res = ar._1 -> ar._2.substring(1, ar._2.length)

      res
    }).toMap
    v
  }

  def csvToWF(filePath: String, sqlContext: SQLContext, local: Boolean): InputFlow = {

    import sqlContext.implicits._
    val ds = sqlContext.read.option("header", "true").option("delimiter", ",").csv(filePath).as[CSV_INPUT_JOB] // //.save(outputDir)
    ds.show()

    def getSqlText(filePath: String, actSql: String) = {
      val sql: Option[String] = if (filePath != null && filePath.length > 0) {
        if (local) Some(new String(Files.readAllBytes(Paths.get(filePath)))) else FileUtil.getFileContent(filePath, sqlContext.sparkContext.hadoopConfiguration)
      } else Some(actSql)
      sql
    }

    val jobsMap: Array[(String, Job)] = ds.collect().map(record => {
      val depends_on: Option[List[String]] = if (record == null || record.depends_on == null || record.depends_on.length == 0) None else Some(record.depends_on.split(":").toList)
      val outputName: Option[String] = if (record.output_tbl_name != null) Some(record.job_name) else None
      println("Job Type: " + record.job_type + " depends_on " + depends_on)

      val tuple: Tuple2[String, Job] = record.job_type match {
        case SqlJobTypes.INPUT_SOURCE => {
          if (record.hive != null && record.hive.length > 0) {
            val fs = FileSource(record.hive, FileFormats.HIVE)
            record.job_name -> Job(record.job_name, Some(true), depends_on, Some(fs), jobOutputTableName = outputName)
          } else {
            val header = if (record.csv_header != null && record.csv_header == "TRUE") true else false
            val fs = FileSource(record.csv_file_path, FileFormats.CSV, Some(record.csv_delimiter), Some(header))
            record.job_name -> Job(record.job_name, Some(true), depends_on, Some(fs), jobOutputTableName = outputName)
          }
        }
        case SqlJobTypes.OUTPUT_SOURCE => {
          if (record.hive != null && record.hive.length > 0) {
            val fs = FileSource(record.hive, FileFormats.HIVE)

            record.job_name -> Job(record.job_name, Some(true), depends_on, output = Some(fs), jobOutputTableName = outputName)
          } else {
            val header = if (record.csv_header != null && record.csv_header == "TRUE") true else false
            val fs = FileSource(record.csv_file_path, FileFormats.CSV, Some(record.csv_delimiter), Some(header), mode = Some(record.output_mode))
            record.job_name -> Job(record.job_name, Some(true), depends_on, output = Some(fs), jobOutputTableName = outputName)
          }
        }
        case SqlJobTypes.TRANSFORMATION => {
          val sql: Option[String] = getSqlText(record.transformation_file, record.transformation_sql)
          record.job_name -> Job(record.job_name, Some(true), depends_on, None, None, Some(List(DataTransformRule(transformSQL = sql))), jobOutputTableName = outputName)
        }
        case SqlJobTypes.FILTER => {
          val sql: Option[String] = getSqlText(record.filter_file, record.filter_sql)
          record.job_name -> Job(record.job_name, Some(true), depends_on, None, filterData = Some(FilterData(sql)), None, None, jobOutputTableName = outputName)
        }
        case SqlJobTypes.JOIN => {
          val sql: Option[String] = getSqlText(record.join_file, record.join_sql)
          record.job_name -> Job(record.job_name, Some(true), depends_on, None, joins = Some(JoinJob(sql)), jobOutputTableName = outputName)
        }
        case SqlJobTypes.MASK => {
          val sql: Option[String] = getSqlText(record.join_file, record.join_sql)
          val maskNumDig = if (record.mask_num_digits != null && record.mask_column_type == COLUMN_MASKING_TYPE.REAL_NUMBER) Some(record.mask_num_digits.toInt) else None

          val dataMask = ColumnMask(record.mask_column_name, record.mask_column_global_type, record.mask_column_type, maskNumDig)
          record.job_name -> Job(record.job_name, Some(true), depends_on, None, columnMask = Some(dataMask), jobOutputTableName = outputName)
        }
        case _ => throw new IllegalArgumentException

      }
      tuple
    })

    println(" root name :" + ds.collect().last.job_name)

    InputFlow(ds.collect().last.job_name, jobsMap.toMap, None, ds.collect().last.job_name)
  }

}
