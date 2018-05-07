import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import play.api.libs.json.Json
import common.CheckDistance
import scala.annotation.tailrec
import scala.collection.mutable

import com.github.nscala_time.time.Imports._

object Task1_MacTable {

  case class MacTablewithChildID(childID:String,macAddress: String, time: Long, latitude: Double,longitude: Double, signalLevel: Long, ssid: String, type_temp: String)
  case class LatLonTable(latitude:Double,longitude:Double)
  case class LastMacTable(macAddress: String, latitude: Double, longitude: Double, type_temp: String, last_updated: String, signal: Long, hit_cnt: Integer, is_modified: List[LatLonTable], ssid: String, childList: Set[String])

  // check whether json data is null or not using pattern matching with recursive!
  @tailrec
  def checkNullWithPatternMatching[T](list:List[T]):Boolean=list match{
    case head::tail => {
      //ssid of gps,data can inclue "network", so exclude gps,empty first! but the funny thing is that there is only one including network!
      if(head=="gps" || head=="empty") false
      else if(head=="network"){
        if(!tail.isEmpty) true
        else false
      }
      else checkNullWithPatternMatching(tail)
    }
    case Nil => false
  }

  //make MacTablelist with childID
  def makeMacTableWithChildID(list:List[String]):List[MacTablewithChildID]={
      val childID = list(1)
      var macArrayData = ""
      for(i<-6 until list.length){
        macArrayData += ","+list(i)
      }
      macArrayData = macArrayData.substring(2,macArrayData.length-1).replaceAll("\"\"","\"")
      val listMacTable = parseToJsonAndTable(childID,macArrayData)
      listMacTable
  }

  // string data to json and then MacTablewichChildID using play.api.libs.json
  def parseToJsonAndTable(childID:String,lines: String): List[MacTablewithChildID] = {
    val jsValue = Json.parse(lines)
    val mac_address = jsValue \\ "macAddress"
    val accuracy = jsValue \\ "accuracy"
    val altitude = jsValue \\ "altitude"
    val filtered = jsValue \\ "filtered"
    val latitude = jsValue \\ "latitude"
    val longitude = jsValue \\ "longitude"
    val signalLevel = jsValue \\ "signalLevel"
    val speed = jsValue \\ "speed"
    val ssid = jsValue \\ "ssid"
    val time = jsValue \\ "time"
    val type_temp = jsValue \\ "type"
    val mac_list = mutable.ListBuffer.empty[MacTablewithChildID]

    for (i <- 0 to mac_address.length - 1) {
      if (type_temp(i).as[String] == "NORMAL") {
        mac_list += MacTablewithChildID(childID,mac_address(i).as[String], time(i).as[Long],(((latitude(i).as[Double]*1000).toInt).toDouble)/1000,(((longitude(i).as[Double]*1000).toInt).toDouble)/1000,signalLevel(i).as[Long], ssid(i).as[String], type_temp(i).as[String])

      }
    }
    mac_list.toList
  }

  // lastMacTable with childID for the task1!
  def finalMactableWithChild(temp_table: List[MacTablewithChildID], iterChildID: Iterable[String]): LastMacTable = {
    val first_table = temp_table(0)
    val is_modified = mutable.ListBuffer[LatLonTable]()
    val latitude = first_table.latitude
    val longitude = first_table.longitude
    val signal = first_table.signalLevel
    val setChildID = iterChildID.toSet
    val countChildID = setChildID.size
    if (temp_table.length > 1) {
      for (i <- 1 until temp_table.length) {
        if (CheckDistance.checkDistance(first_table.latitude, first_table.longitude, temp_table(i).latitude, temp_table(i).longitude) > 500.0) {
          is_modified += LatLonTable(temp_table(i).latitude, temp_table(i).longitude)
        }
      }
    }
    LastMacTable(first_table.macAddress, latitude, longitude, first_table.type_temp, first_table.time.toDateTime.toString(), signal, countChildID, is_modified.toList, first_table.ssid, setChildID)
  }



  def main(args: Array[String]): Unit = {
    // check start time for optimizing
    val startT = System.nanoTime()

    val conf = new SparkConf().setMaster("local").setAppName("mac_table")
    conf.set("spark.network.timeout", "600s")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    // use absolute path
    val file = new File("")
    val FILE_PATH = file.getAbsoluteFile() + "/../data"
    val INPUT_FILE_PATH = FILE_PATH + "/task_loc.csv.bz2"
    val OUTPUT_FILE_PATH = FILE_PATH + "/output_task1_macTable"

    // HDFS FILE_PATH
    val HDFS_FILE_PATH = "hdfs://localhost:9000/user/data"
    val HDFS_INPUT_FILE_PATH = HDFS_FILE_PATH + "/task_loc.csv.bz2"
    val HDFS_OUTPUT_FILE_PATH = HDFS_FILE_PATH + "/output_task1_macTable"



    //Read Data From INPUT_PATH
    val inputRDD = sc.textFile(HDFS_INPUT_FILE_PATH)

    /*
    filter empty, gps. only network included <- fail because ssid can include "network"
    1. remove first index(valueless information)
    2. filter non JSON Array data using pattern matching function, checkNullWithPatternMatching
    3. make List[MacTablewithChildID] RDD
    4. flatMap List -> macTablewithChildID
     */
    val macTableWithChildIDRDD = inputRDD.mapPartitionsWithIndex((idx,iter)=>if (idx==0) iter.drop(1) else iter )
                                        .map(lines=>{
                                          val splitInput = lines.split(",").toList
                                          val checkNull = checkNullWithPatternMatching(splitInput)
                                          if(checkNull) splitInput
                                          else List.empty
                                        }).filter(x=>(x!=List.empty))
                                          .map(list=>makeMacTableWithChildID(list))
                                            .flatMap(list=>list)

    /*
      1. make pair RDD -> (macAddr,childID)
      2. groupByKey -> (macAddr,(childID_1, childID_2, ...))
     */
    val childIDWithMacaddrRDD = macTableWithChildIDRDD.map(x=>(x.macAddress,x.childID))
                                                            .groupByKey()

    /*
      1. make pair RDD -> (macAddr, macTable)
      2. groupByKey -> (macAddr, macTable)
      3. sorting by time which is last_updated
     */
    val macKeyMacTableValuesRDD = macTableWithChildIDRDD.map(mac=>(mac.macAddress,mac))
                                                          .groupByKey()
                                                            .map(x=>{
                                                              val sorting = x._2.toList.sortBy(x=>x.time).reverse
                                                              (x._1,sorting)
                                                            })

    /*
      1. join macTableRDD, childIDRDD -> (macAddress,List[MacTable],Iterable[childID])
      2. finalMacTablewithChildID -> LastMacTable with childSet column
     */
    val finalTask1RDD = macKeyMacTableValuesRDD.join(childIDWithMacaddrRDD)
                                           .map(last=>finalMactableWithChild(last._2._1,last._2._2))

    // write data on HDFS
    finalTask1RDD.saveAsTextFile(HDFS_OUTPUT_FILE_PATH)

    // write data on directory
    finalTask1RDD.saveAsTextFile(OUTPUT_FILE_PATH)

    println("*****************************The End*****************************")

    // Calculate taking time for optimizing
    println("Taking time : "+((System.nanoTime()-startT)/ 1000000000.0)+"seconds")

  }
}
