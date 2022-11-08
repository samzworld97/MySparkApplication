package com.samzworld.codebase

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

class FundamentalLoader {


    def fundamentalLoader(prop: Properties, spark: SparkSession, LoaderName: String): Unit ={


      //variable initialization
      println(">>>>>>>>>>>>>>>>Initializing property file reading<<<<<<<<<<<<<<<<<<")
      val jdbcConnection = prop.getProperty("jdbcConnection")
      val tableName = prop.getProperty(s"${LoaderName.toUpperCase}.tableName")
      val dbUser = prop.getProperty("dbUser")
      val dbPassword = prop.getProperty("dbPassword")
      val fundamentalFilePath = prop.getProperty("fundamentalFilePath")
      var dataframe : DataFrame = null

      //dataframe loader
      try {

        dataframe = spark.read.format("csv")
          .option("inferschema","true").option("header","true").load(fundamentalFilePath)

        println(">>>>>>>>>>>>>>>>Dataframe loaded successfully<<<<<<<<<<<<<<<<<<<<")
      }
      catch {
        case exception: Exception => {
          println("File not found : "+ exception)
          println("!!JOB FAILED!!  RAW DATA NOT FOUND!!")
          spark.stop()
          sys.exit(0)
        }
      }

      //Data writing to mysql db table
      try {

        dataframe.printSchema()
        dataframe.write.format("jdbc")
          .option("url", jdbcConnection)
          .option("dbtable", tableName).option("user", dbUser).option("password", dbPassword)
          .mode("append").save()

        println(">>>>>>>>>>>>>>>>Staging table loaded successfully<<<<<<<<<<<<<<<<<<<<")
      }
      catch {
        case exception: Exception => {
          println("Data writing failed due to exception : "+ exception)
          println("STOPPING APPLICATION!!!")
          spark.stop()
          sys.exit(0)
        }
      }
    }
}
