package com.samzworld.codebase
import java.io.FileInputStream
import java.util.Properties

class propertyFileLoader{

  var inputStream : FileInputStream = null
  var prop : Properties = null

  //Property file setter
  def propertySetter{

  try {
    this.inputStream = new FileInputStream("src/main/resources/propertyFiles/sparkPropertyFile.properties")
    this.prop = new Properties()
  }
  catch {
    case exception: Exception => {
      println("Got exception while loading property file : " + exception)
      sys.exit(0)
      }
    }
  }

  //inputstream object getter function
  def inputStreamGetter: FileInputStream ={
    inputStream
  }

  //property object getter function
  def propertyFileGetter: Properties = {
    prop
  }
}
