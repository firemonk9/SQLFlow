package com.metlife.wf

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dhiraj
  */
trait SparkInit {

  /**
    * Initilize Spark Context and SQL Context.
    *
    * @param args
    * @return
    */
  def initilizeSpark(args: Array[String], local: Boolean = false,  params: Option[String] = None): (SparkContext, SQLContext) = {

    val conf = if (local) {
      new SparkConf()
        .setMaster("local[1]")
        .setAppName("SQL_WORK_FLOW")


    } else {
      {
        val paramsList = if (params.isDefined) params.get.split(",").toList else List()
        val ky = paramsList.map(a => {
          val ky = a.split("=")
          ky(0) -> ky(1)
        }).toMap
        new SparkConf()

      }
    }

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    (sc, sqlContext)
  }
}
