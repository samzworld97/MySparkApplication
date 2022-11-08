package com.samzworld.codebase

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

class PriceLoader {

    def priceLoader(prop: Properties, spark: SparkSession, LoaderName: String): Unit ={


      //variable initialization
      println(">>>>>>>>>>>>>>>>Initializing property file reading<<<<<<<<<<<<<<<<<<")
      val jdbcConnection = prop.getProperty("jdbcConnection")
      val tableName = prop.getProperty(s"${LoaderName.toUpperCase}.tableName")
      val dbUser = prop.getProperty("dbUser")
      val dbPassword = prop.getProperty("dbPassword")
      val priceFilePath = prop.getProperty("priceFilePath")
      var dataframe : DataFrame = null

      //dataframe loader
      try {

        dataframe = spark.read.format("csv")
          .option("inferschema","true").option("header","true").load(priceFilePath)

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

 //   Renaming column name as per existing table columns
      dataframe = dataframe.withColumnRenamed("date","price_date").
        withColumnRenamed("symbol","ticker_symbol")
        .withColumn("price_date",col("price_date").cast(DateType))


      //Data writing to mysql db table
      try {


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
