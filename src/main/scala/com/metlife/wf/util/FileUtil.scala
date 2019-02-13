package org.wf.util

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
//import org.apache.spark.sql.SQLContext

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by dhiraj
  */
object FileUtil {
  def deleteFile(sourcePath: String, hadoopConf: Configuration): Some[Boolean] = {
    try {
      val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val path: Path = new Path(sourcePath)
      val res = if (fs.exists(path)) {
        fs.delete(path, false)
      }
      Some(true)
    } catch {
      case e: Exception => Some(false)
    }

  }


  /**
    * Gets the content of the file.
    *
    * @param sourcePath
    * @param conf
    * @return
    */
  def getSecureFileContent(sourcePath: String, conf: org.apache.hadoop.conf.Configuration): Option[String] = {
    //val conf = sqlContext.sparkContext.hadoopConfiguration
    import org.apache.hadoop.security.UserGroupInformation

    val ugi: Unit = UserGroupInformation.loginUserFromKeytab("myapplication/myhost", "myapplication.keytab")

    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val path: Path = new Path(sourcePath)
    val res = if (fs.exists(path)) {
      val br = new BufferedReader(new InputStreamReader(fs.open(path)))
      var line = br.readLine()
      val arrayBuffer = ArrayBuffer[String]()
      while (line != null) {
        arrayBuffer += line
        line = br.readLine()
      }
      Some(arrayBuffer.mkString)
    } else None
    res
  }


  /**
    * Gets the content of the file.
    *
    * @param sourcePath
    * @param conf
    * @return
    */
  def getFileContent(sourcePath: String, conf: org.apache.hadoop.conf.Configuration): Option[String] = {
    //val conf = sqlContext.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val path: Path = new Path(sourcePath)
    val res = if (fs.exists(path)) {
      val br = new BufferedReader(new InputStreamReader(fs.open(path)))
      var line = br.readLine()
      val arrayBuffer = ArrayBuffer[String]()
      while (line != null) {
        arrayBuffer += line
        line = br.readLine()
      }
      Some(arrayBuffer.mkString)
    } else None
    res
  }


  /**
    * Gets all the JSON files in the directory.
    *
    * @param sourcePath
    * @param conf
    */
  def fileList(sourcePath: String, conf: Configuration): List[String] = {

    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val iterator = fs.listFiles(new org.apache.hadoop.fs.Path(sourcePath), false)
    val list = new ListBuffer[String]()
    while (iterator.hasNext) {
      val next = iterator.next()
      if (next.getPath.toString.endsWith(".json")) // your name logic
        list += next.getPath.toString

    }
    list.toList
  }


  /**
    * write the content to hdfs file
    *
    * @param sourcePath
    * @param conf
    */
  def isDirectory(sourcePath: String, conf: org.apache.hadoop.conf.Configuration): Boolean = {

    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val path = new org.apache.hadoop.fs.Path(sourcePath)
    fs.isDirectory(path)

  }

  /**
    * Create the directory on HDFS.
    *
    * @param sourcePath
    * @param conf
    */
  def createDirectory(sourcePath: String, conf: org.apache.hadoop.conf.Configuration): Boolean = {

    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val path = new org.apache.hadoop.fs.Path(sourcePath)
    fs.mkdirs(path)
  }


  /**
    * write the content to hdfs file
    *
    * @param sourcePath
    * @param content
    * @param conf
    */
  def writeToFile(sourcePath: Option[String], content: String, conf: org.apache.hadoop.conf.Configuration): Unit = {

    if (sourcePath.isDefined) {
      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
      val byt = content.toString().getBytes()
      val path = new org.apache.hadoop.fs.Path(sourcePath.get)
      val fsOutStream = fs.create(path)
      fsOutStream.write(byt)
      fsOutStream.close()
    }
  }

  /**
    * write the text content to file system.
    *
    * @param sourcePath
    * @param content
    */
  def writeToTextFile(sourcePath: String, content: String): Unit = {
    val file = new java.io.File(sourcePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(content)
    bw.close()
  }


  /**
    * Return the zip files in the directory recursively.
    *
    * @param folder
    * @return
    */
  def listFilesForFolder(folder: File, extension: String = ".json"): Array[String] = {
    val files = new ArrayBuffer[String]
    for (fileEntry <- folder.listFiles) {

      if (fileEntry.isFile && fileEntry.getName.endsWith(extension)) {
        files += fileEntry.getAbsolutePath
      }
    }

    files.toArray
  }

}
