package com.metlife.wf

case class StatusMessage(flowName: String, jobName: Option[String]=None, jobType: Option[String]=None, msg: String, exception: Option[String] = None,time:java.util.Date, tableName:Option[String] = None,tableCount:Option[Long] = None)

object StatusUpdate {


  def addStatus(statusMessage: StatusMessage) = {

    println(" loggin status " + statusMessage)
  }


}


