import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json.Json

import scala.annotation.tailrec
import scala.collection.mutable

object PerformanceJsonRegex {

  case class MacTablewithChildID(macAddress: String, time: Long, latitude: Double,longitude: Double, signalLevel: Long, ssid: String, type_temp: String)

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

  def time[R](block: => R): Double = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val timediff = t1-t0
    val seconds = timediff.toDouble / 1000000000.0
    seconds
  }


  // string data to json and then MacTablewichChildID using play.api.libs.json
  def parseToJsonAll(lines: String): Array[Any] = {

    val jsValue = Json.parse(lines)
    val mac_address = jsValue \\ "macAddress"
    val latitude = jsValue \\ "latitude"
    val longitude = jsValue \\ "longitude"
    val signalLevel = jsValue \\ "signalLevel"
    val time = jsValue \\ "time"
    val type_temp = jsValue \\ "type"
    val ssid = jsValue \\ "ssid"
    val perf_list = mutable.ArrayBuffer.empty[Array[Any]]

    for (i <- 0 to mac_address.length - 1) {
      if(type_temp(i)=="NORMAL") {
        perf_list += Array(mac_address(i), time(i), latitude(i), longitude(i), signalLevel(i), ssid(i), type_temp(i))
      }
    }
    perf_list.toArray
  }
  def parseToRegexAll(lines: String): Array[Any] = {

    val perf_list = mutable.ArrayBuffer.empty[Array[Any]]

    // Regex for json Object
    val reg = """\{\s*([^\[\]]*?)\s*\}""".r
    val arrayReg = (reg findAllIn lines).toArray
      for(i<- 0 until arrayReg.length){
        // ssid's data possibly includes "," , so you can't take the ssid'data using split(",")
        val ssid_start = arrayReg(i).indexOf(",\"ssid\":")
        val ssid_last = arrayReg(i).indexOf(",\"type")

        val arr = arrayReg(i).replaceAll("\"","").split(",")

        val macAddress = arr(0).substring(12,arr(0).length)
        val time = arr(1).substring(5,arr(1).length)
        val longitude = arr(2).substring(8,arr(2).length)
        val latitude = arr(3).substring(7,arr(3).length)
        val signalLevel = arr(6).substring(11,arr(6).length)
        val ssid = arrayReg(i).substring(ssid_start+9,ssid_last-1)
        val type_temp  =arr(arr.length-2).substring(5,arr(arr.length-2).length)
        if(type_temp=="NORMAL"){
          perf_list += Array(macAddress,time,longitude,latitude,signalLevel,ssid,type_temp)
        }
    }
    perf_list.toArray
  }

  def parseToJsonMacaddr(lines: String): Array[Any] = {

    val jsValue = Json.parse(lines)
    val mac_address = jsValue \\ "macAddress"
    val perf_list = mutable.ArrayBuffer.empty[Array[Any]]

    for (i <- 0 to mac_address.length - 1) {
        perf_list += Array(mac_address(i))

    }
    perf_list.toArray
  }
  def parseToRegexMacaddr(lines: String): Array[Any] = {

    val regexMac = "[a-z0-9]{2}:[a-z0-9]{2}:[a-z0-9]{2}:[a-z0-9]{2}:[a-z0-9]{2}:[a-z0-9]{2}".r
    val array = (regexMac findAllIn lines).toArray
    val perf_list = mutable.ArrayBuffer.empty[Array[Any]]

    for (i <- 0 to array.length - 1) {
      perf_list += Array(array(i))

    }
    perf_list.toArray
  }

  def parse(array:Array[String]):Unit= {
    val perf = mutable.ArrayBuffer[Array[Any]]()
    for(i<-0 until array.length){
      //Here's choosing function
      perf += parseToRegexMacaddr(array(i))
    }
    None
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("mac_table")
    conf.set("spark.network.timeout", "600s")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // use absolute path
    val file = new File("")
    val FILE_PATH = file.getAbsoluteFile() + "/../data"
    val T_INPUT_FILE_PATH = FILE_PATH + "/performance_data.tar.gz"
    val INPUT_FILE_PATH = FILE_PATH + "/task_loc.csv.bz2"
    val OUTPUT_FILE_PATH = FILE_PATH + "/performance_check"


//    val inputRDD = sc.textFile(INPUT_FILE_PATH).mapPartitionsWithIndex((idx,iter)=>if (idx==0) iter.drop(1) else iter )
//                                                .map(lines=>{
//                                                  val splitInput = lines.split(",").toList
//                                                  val checkNull = checkNullWithPatternMatching(splitInput)
//                                                  if(checkNull) lines
//                                                  else " "
//                                                }).filter(x=>(x!=" "))
//                                                  .filter(x=> x.split(",")(0).toLong<350000)
//                                                    .map(x=>makeMacTableWithChildID(x.split(",").toList))
//    inputRDD.saveAsTextFile(OUTPUT_FILE_PATH)


    // 102123 possible, data size
    val performanceRDD = sc.textFile(T_INPUT_FILE_PATH)
    var array = performanceRDD.collect()
    // change data size here
//    array = array.slice(0,array.length/100)

    // choose check loop
    var checkNum = 5

    var count = 0

    // time save here
    val totalData5Count= Array.ofDim[Double](checkNum)
    println(s"array length : ${array.length} ,  checkNum : ${checkNum}")
    print("Press only Num")
    var e = readInt()

    while(count<checkNum){
      println((count+1)+" is running")
      totalData5Count(count) = time{parse(array)}
      count+=1
    }

    for(i<- 0 until checkNum){
      println(s"${totalData5Count(i)}")
    }


  }
}
