import java.io.File

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec
import scala.collection.mutable

import common.CheckDistance

object Task2_ChildTable {

  case class ChildTable(childID:String,time:String,latlonTable:LatLonTable)
  case class LatLonTable(latitude:Double,longitude:Double)
  case class MacTable(macAddress:String,time:String,latlonTable:LatLonTable,ssid:String,childID:String)
  case class ChildandMacTable(childID: Long,hotspot:LatLonTable,hit_cnt:Int, include_mac:List[String],exclude_mac:List[String],time_list:List[String], provider_list:List[String])

  // check whether json data is null or not using pattern matching with recursive!
  @tailrec
  def checkNullWithPatternMatching[T](list:List[T]):Boolean=list match{
    case head::tail => {
      // filter empty data only
      if(head=="empty") false
      else if(head=="gps" || head=="network")
        if(!tail.isEmpty) true
        else false
      else checkNullWithPatternMatching(tail)
    }
    case Nil => false
  }

  // make childTable with LatLonTable
  def makeChildTable(list:List[String]):ChildTable={
    val childID = list(1)
    val time = list(2)
    val latitude = ((((list(3).toDouble)* 1000).toInt).toDouble)/1000
    val longitude = ((((list(4).toDouble)* 1000).toInt).toDouble)/1000
    ChildTable(childID,time,LatLonTable(latitude,longitude))
  }

  // clustering and find HotSpot!! using customizing Kmeans!
  def clusteringHostSpotUsingKMeans(list:List[ChildTable]): ChildTable={
    val childID = list(0).childID
    val tempList = mutable.ListBuffer[LatLonTable]()
    val timeList = mutable.ListBuffer[String]()
    for(i<-0 until list.length){
        tempList += list(i).latlonTable
    }
    // remove same data using Set
    val setLatLon = tempList.toSet

    val jamesMeansList = setLatLon.toList
    val sizeData = jamesMeansList.length
    val distance = Array.ofDim[Double](sizeData,sizeData)

    // default value is -1 which means not included
    val includedGroup = Array.fill[Int](sizeData)(-1)

    // centroid location array, it can be more
    val centroidLocation = mutable.ArrayBuffer[Array[Double]]()

    // save the index of centroid location
    val centroidIndex = mutable.ArrayBuffer[Int]()
    var centroidCount = 1

    var checkMoreCentroid = true

    // only first centroid assigned
    centroidLocation += Array(jamesMeansList(0).latitude,jamesMeansList(0).longitude)
    centroidIndex += 0

    for(i<-0 until sizeData){
      for(j<-0 until sizeData){
        val dis =((CheckDistance.checkDistance(jamesMeansList(i).latitude, jamesMeansList(i).longitude, jamesMeansList(j).latitude, jamesMeansList(j).longitude)) * 1000).toInt.toDouble / 1000
        distance(i)(j) = dis
      }
    }

    while(checkMoreCentroid){
      for(i<- 0 until centroidCount) {
        for (j <- 0 until sizeData) {
          val tmpDistance = distance(centroidIndex(i))(j)
          if (tmpDistance < 500.0) {
            var min = tmpDistance
            includedGroup(j) = centroidIndex(i)
            for (k <- 0 until centroidCount) {
              if (distance(centroidIndex(k))(j) < min) {
                min = distance(centroidIndex(k))(j)
                includedGroup(j) = centroidIndex(k)
              }
            }
          }
        }
      }
      checkMoreCentroid = false
      // count
      var count = 0
      var noMoreCentroid = true
      while(noMoreCentroid && count < sizeData){
        // check not included_group data, and make the first data next centroid location
        if(includedGroup(count) == -1){
          centroidIndex += count
          centroidCount += 1
          checkMoreCentroid = true
          noMoreCentroid = false
        }
        count+=1
      }
    }

    val includedGroup2 = Array.ofDim[List[Int]](centroidCount)
    for(i <- 0 until centroidCount){
      val tempList = mutable.ListBuffer[Int]()
      for(j<- 0 until sizeData){
        if(includedGroup(j)==centroidIndex(i)){
          tempList += j
        }
      }
      includedGroup2(i) = tempList.toList
    }
    var hotspotCount = 0
    var hotspotIndex = 0
    for(i<-0 until centroidCount){
      if(hotspotCount < includedGroup2(i).length){
        hotspotCount = includedGroup2(i).length
        hotspotIndex = i
      }
    }
    var hotspotLat= 0.0
    var hotspotLon = 0.0
    for(i <- 0 until hotspotCount){
      val index = includedGroup2(hotspotIndex)(i)
      hotspotLat += jamesMeansList(index).latitude
      hotspotLon += jamesMeansList(index).longitude
    }

    hotspotLat = ((((hotspotLat / hotspotCount)*1000).toInt).toDouble) /1000
    hotspotLon = ((((hotspotLon / hotspotCount)*1000).toInt).toDouble) /1000


    ChildTable(childID,list(0).time,LatLonTable(hotspotLat,hotspotLon))
  }

  // compare distance between ChildTable and MacTable and return ChildandMacTable
  def compareDistanceWithMacTable(childTable: ChildTable, listMacTable: List[MacTable]): ChildandMacTable={
    val incluedMacList = mutable.ListBuffer[String]()
    val excluedMacList = mutable.ListBuffer[String]()
    val providerList = mutable.ListBuffer[String]()
    val timeList = mutable.ListBuffer[String]()

    for(i <- 0 until listMacTable.length){
      val disbetweenChildandMac = CheckDistance.checkDistance(childTable.latlonTable.latitude,childTable.latlonTable.longitude,listMacTable(i).latlonTable.latitude,listMacTable(i).latlonTable.longitude)
      providerList += listMacTable(i).ssid
      timeList += (listMacTable(i).time.toLong).toDateTime.toString

      if(disbetweenChildandMac < 300.0){
        incluedMacList += listMacTable(i).macAddress
      }
      else
        excluedMacList += listMacTable(i).macAddress
    }

    ChildandMacTable(childTable.childID.toLong,childTable.latlonTable, incluedMacList.toList.length,incluedMacList.toList, excluedMacList.toList,timeList.toList,providerList.toList)

  }



  def main(args: Array[String]): Unit = {
    // check start time for optimizing
    val startT = System.nanoTime()

    val conf = new SparkConf().setMaster("local").setAppName("mac_table")
    // default heartbeat is 10s, change to 600s
    conf.set("spark.network.timeout", "600s")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    // use absolute path
    val file = new File("")
    val FILE_PATH = file.getAbsoluteFile() + "/../data"
    val INPUT_FILE_PATH = FILE_PATH + "/task_loc.csv.bz2"
    val INPUT_MACTABLE_FILE_PATH = FILE_PATH + "/task2_macTable.csv"
    val OUTPUT_FILE_PATH = FILE_PATH + "/output_task2_childTable"
    val OUTPUT_TEMP_FILE_PATH = FILE_PATH + "/output_task2_temporary"

    // HDFS FILE_PATH
    val HDFS_FILE_PATH = "hdfs://localhost:9000/user/data"
    val HDFS_INPUT_FILE_PATH = HDFS_FILE_PATH + "/task_loc.csv.bz2"
    val HDFS_INPUT_MACTABLE_FILE_PATH = HDFS_FILE_PATH+ "/task2_macTable.csv"
    val HDFS_OUTPUT_FILE_PATH = HDFS_FILE_PATH + "/output_task2_childTable"


    //Read Data From INPUT_PATH
    val inputRDD = sc.textFile(HDFS_INPUT_FILE_PATH)

    //MacTable From INPUT_MACTABLE_PATH
    val tempMacTableRDD = sc.textFile(INPUT_MACTABLE_FILE_PATH)


    /*
    1. remove first index(header, column's name)
    2. make macTable -> MacTable
    3. make map -> (childID, MacTable)
    4. groupByKey
     */
    val macTableRDD = tempMacTableRDD.mapPartitionsWithIndex((idx,iter)=>if (idx==0) iter.drop(1) else iter )
                                        .map(lines=>{
                                            val splitLines = lines.split(",")
                                            MacTable(splitLines(0),splitLines(3),LatLonTable(splitLines(1).toDouble,splitLines(2)toDouble),splitLines(4),splitLines(5))
                                        }).map(macTable=>(macTable.childID,macTable))
                                          .groupByKey()

    // RDD[MacTable] -> List[MacTable] using collect()
    // collect works as return all of RDD's value
//    val listMacTable = macTableRDD.collect().toList

    /*
    1. remove first index(valueless information)
    2. filter empty using pattern matching and negative of latitude, longitude but nothing there
    3. filter List.emptyd
    4. make ChildTable -> (childID,time,(LatLonTable(latitude,longitude)))
    5. make map ->(childID, List(time, latitude, longitude))
    6. groupByKey -> (childID,Iterable[childTable])
     */
    val childTableRDD = inputRDD.mapPartitionsWithIndex((idx,iter)=>if (idx==0) iter.drop(1) else iter )
                                          .map(lines=>{
                                            val splitInput = lines.split(",").toList
                                            val checkNull = checkNullWithPatternMatching(splitInput)
                                            if(checkNull) {
                                              if(splitInput(3).toDouble > 0 && splitInput(4).toDouble> 0 ) splitInput
                                              else List.empty
                                            }
                                            else List.empty
                                          }).filter(x=>(x!=List.empty))
                                            .map(x=>makeChildTable(x))
                                              .map(x=>(x.childID,x))
                                              .groupByKey()


    /*
    1. make hotspot using customizing Kmeans!
     */
    val clusteringHotspotRDD = childTableRDD.map(x=>(x._1,clusteringHostSpotUsingKMeans(x._2.toList)))

    /*
    1. join clusteringHotspotRDD with mactableRDD (Both are map => (key,table))
    2. check distance and make childandmacTable
     */
    val childWithMaclistRDD = clusteringHotspotRDD.join(macTableRDD)
                                                  .map(x=>compareDistanceWithMacTable(x._2._1,x._2._2.toList))
                                                      .sortBy(x=>x.childID)

    // save rdd in LOCAL_FILE_PATH
    childWithMaclistRDD.saveAsTextFile(OUTPUT_FILE_PATH)

    // save rdd in HDFS_FILE_PATH
    childWithMaclistRDD.saveAsTextFile(HDFS_OUTPUT_FILE_PATH)


    println("*****************************The End*****************************")

    // Calculate taking time for optimizing
    println("Taking time : "+((System.nanoTime()-startT)/ 1000000000.0)+"seconds")

  }
}


