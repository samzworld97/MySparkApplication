import com.samzworld.codebase.{FundamentalLoader, PriceAdjustLoader, PriceLoader, SecurityLoader, propertyFileLoader}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StagingTableLoaderMain {

  def main(args:Array[String]): Unit ={

    //property file loader
    val propertyLoader = new propertyFileLoader()
    val securityLoader = new SecurityLoader()
    val priceLoader = new PriceLoader()
    val priceAdjustLoader = new PriceAdjustLoader()
    val fundamentalLoader = new FundamentalLoader()
    propertyLoader.propertySetter
    val inputStream = propertyLoader.inputStreamGetter
    val prop = propertyLoader.propertyFileGetter
    prop.load(inputStream)

    val master = prop.getProperty("master")
    var appName = ""
    val LoaderName = "<Loadername>"

    //LoaderName validation
    if(LoaderName.toUpperCase == "SECURITYLOADER"
      || LoaderName.toUpperCase == "PRICELOADER"
      || LoaderName.toUpperCase == "PRICESADJUSTLOADER"
      || LoaderName.toUpperCase == "FUNDAMENTALLOADER")
      appName = prop.getProperty(s"${LoaderName.toUpperCase}.appName")
    else {
      println("Invalid LoaderName!!")
      sys.exit(0)
    }


    //spark instance creation
    val spark = SparkSession.builder()
      .master(master)
      .appName(appName)
      .getOrCreate()

    println(">>>>>>>>>>>>>>>>SparkSession created successfully<<<<<<<<<<<<<<<<<<")
    println(s">>>>>>>>>>>>>>>>Spark version: ${spark.version}<<<<<<<<<<<<<<<<<<<")

    if(LoaderName.toUpperCase == "SECURITYLOADER")
      securityLoader.securityLoader(prop, spark, LoaderName)
    else if(LoaderName.toUpperCase == "PRICELOADER")
      priceLoader.priceLoader(prop, spark, LoaderName)
    else if(LoaderName.toUpperCase == "PRICESADJUSTLOADER")
      priceAdjustLoader.priceAdjustLoader(prop, spark, LoaderName)
    else
      fundamentalLoader.fundamentalLoader(prop, spark, LoaderName)

    println("!!JOB COMPLETED SUCCESSFULLY!!")
    spark.stop()
    sys.exit(0)
  }
}
