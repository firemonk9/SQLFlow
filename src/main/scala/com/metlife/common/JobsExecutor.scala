package com.metlife.common

import java.util
import java.util.Date

import com.metlife.mask.Mask
import com.metlife.wf.{StatusMessage, StatusUpdate, WorkFlowUtil}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.common.model.{SqlJobTypes, _}


case class DFName(df: Option[DataFrame], name: String)

class JobsExecutor(inputFlow: InputFlow, sqlContext: SQLContext, debug: Boolean = false) {

  //TODO change var to val
  var exception: Boolean = false
  val ljobs: Map[String, Job] = inputFlow.jobMap
  val flowName: String = inputFlow.flowName
  val dfMap: java.util.Map[Job, DFName] = new util.HashMap[Job, DFName]()

  def processChains(): Unit = {

    StatusUpdate.addStatus(StatusMessage(flowName, jobName = None, jobType = None, msg = StatusMsg.STARTED, None, new Date(), tableName = None, None))
    postOrderTraversal(ljobs.get(inputFlow.root))
    StatusUpdate.addStatus(StatusMessage(flowName, jobName = None, jobType = None, msg = StatusMsg.COMPLETED, None, new Date(), tableName = None, None))
  }


  def postOrderTraversal(node: Option[Job]): Unit = {

    if(node.isDefined)
      println("traversal : "+node.get.id)

    if (node.isEmpty)
      return

    if (node.get.dependsOn.isDefined) {
      node.get.dependsOn.get.foreach(b => {
        postOrderTraversal(ljobs.get(b))
      })
    }

    val resException: Boolean = processJob(node.get)
    if (resException == true) throw new Exception(" exception occurred in processing the workflow for the step : " + node.get)
  }

  def extractFileName(datasetPath: String): String = {
    if (datasetPath.indexOf("/") > 0) datasetPath.substring(datasetPath.lastIndexOf("/"), datasetPath.lastIndexOf(".") + 1) else datasetPath
  }

  def getJobType(cJob: Job): Option[String] = {
    if (cJob.dataTransformRule.isDefined) Some(SqlJobTypes.TRANSFORMATION)
    else if (cJob.filterData.isDefined) Some(SqlJobTypes.FILTER)
    else if (cJob.sourceData.isDefined) Some(SqlJobTypes.INPUT_SOURCE)
    else if (cJob.output.isDefined) Some(SqlJobTypes.OUTPUT_SOURCE)
    else if (cJob.joins.isDefined) Some(SqlJobTypes.JOIN)
    else None
  }

  def processJob(cJob: Job): Boolean = {

    val a: Job = cJob
    val startTime: Some[Long] = Some(System.nanoTime())

    def exeJob(e: Exception): Unit = {
      //      val count: Long = df.get.df.get.count()
      StatusUpdate.addStatus(StatusMessage(flowName, jobName = Some(cJob.id), jobType = getJobType(cJob), msg = StatusMsg.EXCEPTION, Some(e.getMessage), new Date(), Some(""), None))
      e.printStackTrace()
      exception = true
    }


    println("processing : " + cJob)
    val temp: Job = cJob

    var df: Option[DFName] = try {
      val sourceName: Option[String] = if (cJob.sourceData.isDefined) None else if (cJob.dependsOn.isDefined && cJob.dependsOn.get.length == 1) Some(getDFName(cJob.dependsOn.get.head)) else None
      if (sourceName.isDefined) Some(DFName(Some(sqlContext.sql("select * from " + sourceName.get)), sourceName.get)) else None
    } catch {
      case e: Exception => exeJob(e); None
    }

    if (a.dataTransformRule.isDefined) {
      try {

        println("in transform ::")
        val tdf: org.apache.spark.sql.DataFrame = WorkFlowUtil.transformInputData(df.get.df.get, a.dataTransformRule, Map()).cache()
        val sourceName: String = if (a.jobOutputTableName.isDefined) a.jobOutputTableName.get else df.get.name
        df = Some(DFName(Some(tdf), sourceName))

        df.get.df.get.show()

      } catch {
        case e: Exception => exeJob(e)
      }
    }
    else if (a.filterData.isDefined) {
      try {
        println("in filter ::")
        df.get.df.get.show()
        val tdf: org.apache.spark.sql.DataFrame = WorkFlowUtil.filterDF(df.get.df.get, None, a.filterData.get.filterSql, sqlContext)
        val sourceName: String = if (a.jobOutputTableName.isDefined) a.jobOutputTableName.get else df.get.name
        df = Some(DFName(Some(tdf), sourceName))

      } catch {
        case e: Exception => exeJob(e)

      }
    }
    else if (a.sourceData.isDefined) {
      try {
        println("in source data ::")
        val kValue: String = if (a.sourceData.get.tableName.isDefined) a.sourceData.get.tableName.get else a.sourceData.get.datasetPath
        val mapTableName: Map[String, String] = if (a.sourceData.get.tableNameMap.isDefined) a.sourceData.get.tableNameMap.get else Map(a.sourceData.get.datasetPath -> kValue)

        mapTableName.keys.foreach(key => {
          val tableName: String = if (mapTableName.get(key).get != null && mapTableName.get(key).get.length() > 0) mapTableName.get(key).get else key
          val tdf: org.apache.spark.sql.DataFrame = WorkFlowUtil.createDataFrame(a.sourceData.get, sqlContext).cache()
          df = Some(DFName(Some(tdf), tableName))
          df.get.df.get.show()
        })

      } catch {
        case e: Exception => exeJob(e)
      }
    }
    else if (a.joins.isDefined) {
      try {
        val (resultDF, joinCount) = WorkFlowUtil.joinDF(a.joins.get, sqlContext)
        df = Some(DFName(Some(resultDF), a.id))

      } catch {
        case e: Exception => exeJob(e)
      }
    } else if (a.output.isDefined && df.isDefined) {
      try {
        println("in output data ::")
        val tdf: Boolean = WorkFlowUtil.writeDataframe(df.get.df.get, a.output.get, a.output.get.datasetPath, sqlContext)

      } catch {
        case e: Exception => exeJob(e)
      }
    } else if (a.columnMask.isDefined && df.isDefined) {
      try {
        println("in column mask job ::")
        val resultDF = Mask.maskColumn(df.get.df.get, a.columnMask.get.mask_column_name, a.columnMask.get.mask_column_global_type, a.columnMask.get.mask_column_type, true, a.columnMask.get.mask_num_digits.getOrElse(-1))
        df = Some(DFName(Some(resultDF), a.id))
      } catch {
        case e: Exception => exeJob(e)
      }
    }


    if (df.isDefined && df.get.df.isDefined) {
      try {
        if (debug) {
          val count: Long = df.get.df.get.count()
          StatusUpdate.addStatus(StatusMessage(flowName, jobName = Some(cJob.id), jobType = getJobType(cJob), msg = StatusMsg.COMPLETED, None, new Date(), Some(df.get.name), Some(count)))
        } else {
          //Note : we have set the status to processing as we do not know when this job will actually be executed bca of the laxy execution model of the spark.
          StatusUpdate.addStatus(StatusMessage(flowName, jobName = Some(cJob.id), jobType = getJobType(cJob), msg = StatusMsg.PROCESSING, None, new Date(), Some(df.get.name), None))
        }
        val str: String = if (cJob.jobOutputTableName.isDefined) cJob.jobOutputTableName.get else cJob.id
        df.get.df.get.createOrReplaceTempView(str)
        println("setting the name : " + str + " for below sample dataframe")
        sqlContext.sql("select * from " + str).show()
      } catch {
        case e: Exception => exeJob(e)
      }
    }
    return exception
  }


  private def getDFName(headDependency: String): String = {

    println(" headDependency " + headDependency + "   and jobResultMap.get(headDependency) : " + ljobs.get(headDependency).get + "  jobResultMap.get(headDependency).job.jobOutputTableName.get : ")
    val tableName: String = if (ljobs.get(headDependency) != null && ljobs.get(headDependency).get.jobOutputTableName.isDefined) ljobs.get(headDependency).get.jobOutputTableName.get else headDependency
    println(" headdependency : " + headDependency + "  tableName " + tableName)
    return tableName

  }
}
