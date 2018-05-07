NTERNSHIP TASK

Scala coding

Project

1. mac_addr mapping table


|     mac_addr      | latitude | longitude | type | last_updated | signal | hit_cnt | is_modified | ssid |
| :---------------: | :------: | :-------: | :--: | :----------: | ------ | ------- | ----------- | ---- |
| 00:27:1c:ca:0a:39 |          |           |      |              |        |         |             |      |

- provider = empty, gps filtering

- 최신 정보 기준으로 반경이 500m이 벗어난 mac addr들도 고려한다. (is_modified)

- 최신 정보 기준으로 aggregation한 정보들을 위 테이블을 채운다.

- type : FILTERING NONE

- unixtime -> datetime, e.g, reable time

- hit_cnt는 unique children 수이다. (advanced) - related to 2 TASK


unixTime = 13 digit = 1504561650141



#### Project Summary

- bz2 File read (works without decompressing bz2)

  - > ```scala
    > val result1 = sc.textFile(INPUT_FILE_PATH).cache()
    > ```

  - Q. stand alone , Rdds are made sequentially. On this condition, Is cache() useful?

- data split

  ```scala
  def func_split(temp:Array[String]): Table_temp={
        var temp_d1 = temp(6)
        for( i<- 7 to (temp.length-1)){
          temp_d1+=","+temp(i)
        }
        Table_temp(temp(0),temp(1),temp(2),temp(3),temp(4),temp(5),temp_d1)
  }
  ```

  ​

- flatMap

  - map vs flatMap : flatMap splits all of elements Lists

    ```scala
    val l = List(List(1,2)List(3,4,5))
    l.map(x=>x*2)
    // List(List(2,4)List(6,8,10))

    l.flatMap(x=>x*2)
    // List(2,4,6,8,10)
    ```

- case class 

  - more convenient using class structure

  ```scala
  case class Mac_Table(macAddress:String,time:Long,longitude:Double,latitude:Double,speed:Double,accuracy:Double,signalLevel:Long,altitude:Double,ssid:String,type_temp:String,filtered:Boolean)
      
  case class Table(id:Long,temp1:Long,unixTime:String,latitude:Double,longitude:Double,network:String,mac_tables:List[Mac_Table])
      
  case class Table_temp(id:String,temp1:String,unixTime:String,latitude:String,longitude:String,network:String,mac_tables:String)
      
  case class Mac_addr_mapping_table(macAddress:String,latitude:mutable.ListBuffer[Double],longitude:mutable.ListBuffer[Double],type_temp:String,last_updated:String,signal:mutable.ListBuffer[Long],hit_cnt:Integer,is_modified:mutable.ListBuffer[Pos_Table],ssid:String)
      
  case class Pos_Table(latitude:Double,longitude:Double)
  ```

- RDD to DataFrame

  - RDD[Class] to RDD[Row]

    ```scala
      //DataFrame
        val customSchema = new StructType(Array(
          StructField("mac_addr",StringType,true),
          StructField("latitude",DoubleType,true),
          StructField("longitude",DoubleType,true),
          StructField("type",StringType,true),
          StructField("last_updated",StringType,true),
          StructField("signal",LongType,true),
          StructField("hit_cnt",IntegerType,true),
          StructField("is_modified",StringType,true),
          StructField("ssid",StringType,true)
        ))

    //class elements to row elements
    val rdd_rows = mapping_rdd_dataframe.map(x=>Row(x.macAddress,x.latitude,x.longitude,x.type_temp,x.last_updated,x.signal,x.hit_cnt,x.is_modified,x.ssid))

    //make DataFrame using RDD[Row] & StructType
    val df = sqlContext.createDataFrame(rdd_rows, customSchema)
    ```

- DataFrame to CSV File (Couldn't solve RDD to CSV File)

  ```scala
  // using library
  df.write.format("com.databricks.spark.csv").option("header","true").mode("overwrite").save(OUTPUT_FILE_PATH)
  ```

- else

  - unixtime -> readable time 

    ```scala
    import com.github.nscala_time.time.Imports._
    //So easy!
    println(1503388739814L.toDateTime)
    ```

  - Calculate distance between two location(latitude,longitude)

    ```scala
    //Calculate distance1
        def checkDistance(lat1:Double,lon1:Double,lat2:Double,lon2:Double)={
    		...    
    }
    ```






-  Error comes out one by one

  - ```scala
    // RDD[Class] -> RDD[Row] -> Dataframe is possible, DataFrame - > CSV is Error cause the 'type'
    Exception in thread "main" java.lang.UnsupportedOperationException: CSV data source does not support array<double> data type.
    ```

- hit_cnt 

  -  new RDD made and join with mac table RDD then make the child list -> child set 

-  Spark on HDFS

   -  spent lots of time...

   -  > First. modify core-site.xml 
      >
      > <property>
      > ​     <name>hadoop.tmp.dir</name>
      >
      > ​     <value>/usr/local/hadoop-2.8.1/tmp</value>
      >
      > </property>
      >
      > Second. bin/hdfs namenode -format , check jps running ok
      >
      > and make directory, put data there
      >
      > Third. put down these code on spark class, Then it Works!!!!!! 
      >
      > ```scala
      > val INPUT_FILE_PATH =FILE_PATH+"/task_loc.csv.bz2"
      > val OUTPUT_FILE_PATH = FILE_PATH+"/output_mac_add_table"
      > val HDFS_INPUT_FILE_PATH =HDFS_FILE_PATH+"/task_loc.csv.bz2"
      > val HDFS_OUTPUT_FILE_PATH = HDFS_FILE_PATH+"/output_mac_add_table"
      >
      > sc.textFile(HDFS_INPUT_FILE_PATH)
      > outRDD.saveAsTextFile(HDFS_OUTPUT_FILE_PATH)
      > ```

      ​

-  Pattern matching(Next time... after studying...)

-  Pom.xml (Library, used for this project)

   ```
   //library for parse json
   <dependency>
               <groupId>com.typesafe.play</groupId>
               <artifactId>play-json_2.11</artifactId>
               <version>2.6.7</version>
           </dependency>
           
   //library for converting unix time to readable time
      <dependency>
               <groupId>com.github.nscala-time</groupId>
               <artifactId>nscala-time_2.11</artifactId>
               <version>2.18.0</version>
           </dependency>
   ```