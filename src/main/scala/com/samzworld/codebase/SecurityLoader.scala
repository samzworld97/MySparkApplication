package com.samzworld.codebase

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties



class SecurityLoader {

    def securityLoader(prop: Properties, spark: SparkSession, LoaderName: String): Unit ={


      //variable initialization
      println(">>>>>>>>>>>>>>>>Initializing property file reading<<<<<<<<<<<<<<<<<<")
      val jdbcConnection = prop.getProperty("jdbcConnection")
      val tableName = prop.getProperty(s"${LoaderName.toUpperCase}.tableName")
      val dbUser = prop.getProperty("dbUser")
      val dbPassword = prop.getProperty("dbPassword")
      val securitiesFilePath = prop.getProperty("securitiesFilePath")
      var dataframe : DataFrame = null

      //dataframe loader
      try {

        dataframe = spark.read.format("csv")
          .option("inferschema","true").option("header","true").load(securitiesFilePath)

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

      //Renaming column name as per existing table columns
      dataframe = dataframe.withColumnRenamed("Ticker symbol","ticker_symbol").
        withColumnRenamed("Security","security").
        withColumnRenamed("SEC filings","sec_filing").
        withColumnRenamed("GICS Sector","gics_sector").
        withColumnRenamed("GICS Sub Industry","gics_sub_industry").
        withColumnRenamed("Address of Headquarters","address").
        withColumnRenamed("Date first added","entry_timestamp").
        withColumnRenamed("CIK","cik")


      //Data writing to mysql db table
      try {

        dataframe.write.format("jdbc")
          .option("url", jdbcConnection)
          .option("dbtable", tableName).option("user", dbUser).option("password", dbPassword)
          .mode("append").save()

        dataframe.show()

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
