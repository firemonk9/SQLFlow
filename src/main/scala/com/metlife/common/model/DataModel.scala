package org.common.model

import java.util

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.plans.logical.Repartition

/**
  * Created by dhiraj
  */

class DataModel {}

case class CSV_INPUT_JOB(job_name:String,depends_on:String,job_type:String,output_mode:String,output_tbl_name:String,hive:String,csv_file_path:String,csv_delimiter:String,csv_header:String,filter_file:String,filter_sql:String,transformation_file:String,transformation_sql:String,join_file:String,join_sql:String,mask_column_name:String, mask_column_global_type : String, mask_column_type:String, mask_num_digits: String)

case class InputFlow(root: String, jobMap: Map[String, Job], callBackUrl: Option[String] = None, flowName: String, livy:Option[Boolean]=None, hadoop:Option[Boolean]=None)

case class Job(id: String, dependentJob: Option[Boolean] = None, dependsOn: Option[List[String]] = None,
               sourceData: Option[FileSource] = None, filterData: Option[FilterData] = None, dataTransformRule: Option[List[DataTransformRule]] = None,
               command: Option[List[String]] = None,  restCallBack: Option[String] = None, sqlUpdate: Option[List[String]] = None, jobDescription: Option[String] = None,
               validationStatements: Option[List[ValidationStatement]] = None,
               joins: Option[JoinJob] = None,  jobOutputTableName:Option[String]=None,output:Option[FileSource]=None,columnMask:Option[ColumnMask]=None)

case class ColumnMask(mask_column_name:String, mask_column_global_type : String, mask_column_type:String, mask_num_digits: Option[Int]=None)

case class JoinJob(joinSql: Option[String] = None)

case class FilterData(filterSql: Option[String] = None)

case class FileSource(datasetPath: String, datasetFormat: String, datasetDelimiter: Option[String] = Some(","), header: Option[Boolean] = Some(false), transformations: Option[List[DataTransformRule]] = Some(List()), filterData: Option[List[RecordFilter]] = None, filterSql: Option[String] = None,
                      columns: Option[Array[String]] = None,  jdbcData: Option[JDBCData] = None, transformationSQL: Option[Boolean] = None, tableName: Option[String] = None, tableNameMap:Option[Map[String,String]]=None,inferSchema:Option[Boolean]=None,repartition: Option[Int]=None,coalesce: Option[Int]=None, mode:Option[String]=None)


case class JDBCData(jdbcUrl: String, jdbcUser: String, jdbcPassword: String, jdbcDriverPath: String, jdbcDriverClass: Option[String], min: Option[Long]=None, max: Option[Long]=None, partitionColumn: Option[String]=None, numPartitions: Option[Int]=None, schema: Option[String]=None, saveMode: Option[String] = Some("Append"))

case class FileSourceException(fileSource: FileSource, message: String)

case class DataTransformRule(column: Option[List[String]] = None,  base64: Option[Boolean] = Some(false), columnExpressionSqlStatement: Option[String] = None, transformSQL: Option[String] = None, columnCasts: Option[Array[Cast]] = None)

case class ColumnFilter(column: String, value: String)

case class RecordFilter(columns: List[ColumnFilter])

case class ValidationStatement(sql: Option[String] = None, expectedResult: Option[Long] = None, validRangeMin: Option[Long] = None, validRangeMax: Option[Long] = None, continueIfFail: Boolean = false, columnName: Option[String] = None, notNull: Option[Boolean] = None, notEmpty: Option[Boolean] = None, notEqualStr: Option[List[String]] = None, notEqualNum: Option[List[Long]] = None, rangeMin: Option[Long] = None, rangeMax: Option[Long] = None, enum: Option[List[String]] = None, uniqueColumns: Option[List[String]] = None)

case class ValidationSQL(sql: String, source: FileSource, validExpected: Option[Long] = None, validRangeMin: Option[Long] = None, validRangeMax: Option[Long] = None, validActual: Option[Long] = None, validationPass: Option[Boolean] = None, exception: Option[String] = None, submittedTime: Option[Long] = Some(0l), beginTime: Option[Long] = Some(0l), endTime: Option[Long] = Some(0l))

case class ColumnType(columnName: String, columnType: String, columnLength: Option[Int], nullable: Option[Boolean], primaryKey: Option[Boolean], foreignKey: Option[Boolean])


case class Status(id: String, status: Option[String] = None)


case class Cast(columnName: String, columnType: String)



object JobData {
  val SAMPLE_DATA: String = "SAMPLEDATA"
  val SCHEMA: String = "SCHEMA"
  val DIFF: String = "DIFF"
  val LIST_TABLES: String = "LIST_TABLES"
  val TABLE_COUNTS: String = "TABLE_COUNTS"
  val SCHEMA_COMPARE: String = "SCHEMA_COMPARE"
  val COPY_DATA: String = "COPY_DATA"
  val SQL_TRANSACTIONS: String = "SQL_TRANSACTIONS"
  var xs = new util.HashMap[String, Tuple2[String, String]]()
}

object DiffConstants {
  val COMPLETED: String = "SUCCEEDED"
  val WAITING: String = "WAITING"
  val RUNNING: String = "RUNNING"
  val EXCEPTION: String = "FAILED"
  val CANCELLED: String = "CANCELLED"
}


object FileFormats {
  val CSV: String = "CSV"
  val JSON: String = "JSON"
  val PARQUET: String = "PARQUET"
  val JDBC: String = "JDBC"
  val HIVE: String = "HIVE"
  val AVRO: String = "AVRO"
  val ORC: String = "ORC"
  val MONGO: String = "MONGO"
  val DATAQ:String = "DATAQ"

}

object StatusMsg{
  val STARTED:String = "STARTED"
  val EXCEPTION:String = "EXCEPTION"
  val COMPLETED:String = "COMPLETED"
  val PROCESSING:String = "PROCESSING"
  val TABLE_COUNT:String = "TABLE_COUNT"

}


object SqlJobTypes{
  val FILTER:String = "FILTER"
  val TRANSFORMATION:String = "TRANSFORMATION"
  val INPUT_SOURCE:String = "INPUT_SOURCE"
  val OUTPUT_SOURCE:String = "OUTPUT_SOURCE"
  val JOIN:String = "JOIN"
  val MASK:String = "COLUMN_MASKING"
}

object COLUMN_MASKING_TYPE{
  val REAL_NUMBER:String = "REAL_NUMBER"
  val FULL_NAME:String = "FULL_NAME"
}