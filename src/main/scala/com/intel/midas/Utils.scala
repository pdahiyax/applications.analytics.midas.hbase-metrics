package com.intel.midas
import java.io.{File, FileWriter}
import java.io.InputStream
import java.sql.DriverManager
import java.sql.{Connection => TeradataConnection}
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}

import org.apache.commons.configuration2.builder.fluent.Configurations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.hadoop.hbase.client.{Connection => HBaseConnection}
import org.apache.hadoop.io.compress.CompressionCodecFactory

import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}
import scala.util.control.Breaks.break

//Specifying all the Spark, HBase Utils

object Utils {

  final val DELIM = "\u0000"
  final val DELIM_COMMA = ","
  final val NULL_BYTE_REPLACE_STR="<NB>"


  class ModPartitioner[K, V](partitions: Int) extends Partitioner {
    def getPartition(key: Any): Int = {
      key.asInstanceOf[Int] % partitions
    }

    override def numPartitions: Int = partitions
  }


  case class hbaseRow(testName: String, testValue: String)

  case class result_metric(fileNumber: Int, file: String, loadStatus: Int, size: Long, rowCount: Int, unitCount: Int, runDuration: Int, putDuration: Int)

  def pushtoDB(rowKeyPart1: String, prev_Unit_Testing_Seq_Key: String, listOfValues: List[hbaseRow], bufferedMutator: BufferedMutator): Duration = {

    val startTime = LocalDateTime.now()
    //println("count of value in the list" + listOfValues.count(x => true))
    val sortedList = listOfValues.sortWith((x, y) => x.testName < y.testName)
    //after sorting then groupby the testName which returns a map of testName and a list of values for the test name
    //then take the map and convert into a tuple consistig of the testname and the concanated values for each test name separated by a newline
    val groupedConcatList = sortedList.groupBy(x => x.testName).map(m => (m._1, m._2.length + Utils.DELIM + m._2.map(h => h.testValue).mkString("\n")))

    /**
     * get the Rowkey for each and push to hbase. To get the rowkey concatenate the rowkey columns to get the saltByte and then add the SaltByte
     * value to the rowkey in Bytes
     */
    val rowKeyPart2 = rowKeyPart1 + Utils.DELIM + prev_Unit_Testing_Seq_Key
    val saltByte = getSaltValue(rowKeyPart2)
    val rowKeyBytes = Bytes.add(Array(saltByte), rowKeyPart2.getBytes)
    val colFam = "0".getBytes


    val put = new Put(rowKeyBytes)

    if (!groupedConcatList.isEmpty) {
      groupedConcatList.foreach(p => put.addColumn(colFam, Bytes.toBytes(p._1), Bytes.toBytes(p._2)))
      bufferedMutator.mutate(put)
      bufferedMutator.flush()
    }

    val endTime = LocalDateTime.now()
    //TODO: Return the duration to measure the put duration
    Duration.between(startTime, endTime)

  }

  def getSaltValue(s: String): Byte = {


    if (s.isEmpty) return 0
    val a = s.getBytes

    var alength = a.length - 1
    var result = 1 //start with initial result to reduce computation
    for (i <- 0 to alength) {
      result = 31 * result + a(i).toInt
    }
    val salt_bucket = 24
    val salt_bucket_val: Byte = Math.abs(result % salt_bucket).asInstanceOf[Byte]
    return salt_bucket_val
  }

  def getTableBufferedMutator(tableName: String, conn: HBaseConnection): BufferedMutator = {

    require(!tableName.isBlank && !tableName.isEmpty   ,"Null value is for the table Name")
    require(! conn.isClosed , "Connection is not available")
    val DataTableBufferedMutator = conn.getBufferedMutator(
      new BufferedMutatorParams(TableName.valueOf(tableName))
        .writeBufferSize(4194304))
    DataTableBufferedMutator
  }

  //table mutator for HBase so that the puts can be sent. Adjusting the parameters for the buffer size

  import org.apache.spark.sql.functions._

  val getColumnsUDF = udf((details: Seq[String]) => {
    val detailsMap = details.map(_.split("=")).map(x => (x(0), x(1))).toMap
    (detailsMap("col1"), detailsMap("col2"), detailsMap("col3"))
  })

  //this implementation is for the local fileSystem
  def getFileBufferedSourcelocal(filePath: String): BufferedSource = {
    val bufferedSource = Source.fromFile(new File(filePath), 4194304)
    bufferedSource
  }

  //this is for the hdfs fileSytem
  def getFileBufferedSource(filePath: String, conf: Configuration): BufferedSource = {

    val fs = FileSystem.get(conf)
    val inputFilePath = new Path(filePath) // Hadoop DFS Path for Input file
    if (fs.exists(inputFilePath) == false) {
        throw new Exception("Input HDFS file path: " + inputFilePath + " does not exist")
    }
/*
    val factory = new CompressionCodecFactory(conf)
    val codec = factory.getCodec(inputFilePath)

    var inputStream :InputStream = null

    // check if we have a compression codec we need to use
    if (codec != null) inputStream = codec.createInputStream(fs.open(inputFilePath))
    else inputStream = fs.open(inputFilePath)*/

    val inputStream = fs.open(inputFilePath)
    val bufferedSource = new BufferedSource(inputStream, 4194304)
    bufferedSource
  }


  /**
   *
   * @param spark
   * @param dataFilePath
   * @param trigFilePath
   * @return
   */
  def MUPR_fileToDataFrame(spark: SparkSession, dataFilePath: String, trigFilePath: String): DataFrame = {
    import spark.sqlContext.implicits._
    val debugFlag = "N"

    val getSaltValueUdf = udf(getSaltValue(_: String): Byte)

    val schema = new StructType().add(StructField("Unit_Testing_Seq_Key", IntegerType, false))
      .add(StructField("Substructure_ID", StringType, false))
      .add(StructField("Sub_Session_Seq_Num", IntegerType, true))
      .add(StructField("Test_Result_Order_Num", IntegerType, false))
      .add(StructField("Test_Result_Array_Seq_Num", FloatType, true))
      .add(StructField("Test_ID", IntegerType, true))
      .add(StructField("Measurement_Value", FloatType, true))
      .add(StructField("Active_Inactive_Core_Vector", StringType, true))
      .add(StructField("Pass_Fail_Core_Vector", StringType, false))
      .add(StructField("Mask_Vector", StringType, false))
      .add(StructField("Test_Name", StringType, false))


    val linesDF = spark.read.format("csv").schema(schema).option("sep", "\u0000").option("inferSchema", false)
      .load(dataFilePath)

    if (debugFlag == "Y")
      linesDF.show(false)

    val inputFilePath = new Path(dataFilePath);
    val fileName = inputFilePath.getName
    //metadata file can be converted into a broadcast variable
    val metadataFile = spark.read.format("csv").option("header", true).option("inferSchema", true)
      .load(trigFilePath)

    if (debugFlag == "Y")
      metadataFile.show(false)

    val metaDataRow = metadataFile.select("File_Name", "Lot", "Lato_Start_WW", "Lots_seq_key")
      .where(col("File_Name") === fileName).first()

    val lot = metaDataRow.getString(1)
    val Lato_Start_WW = metaDataRow.getInt(2)
    val Lots_seq_key = metaDataRow.getInt(3)

    val linesDFWithLot = linesDF.withColumn("Lot", lit(lot))
      .withColumn("Lato_Start_WW", lit(Lato_Start_WW))
      .withColumn("Lots_seq_key", lit(Lots_seq_key))

    if (debugFlag == "Y")
      linesDFWithLot.show(false)


    val linesDFWithRowConcat = linesDFWithLot.withColumn("Value",
      concat_ws("\u0000", col("Measurement_Value"),
        col("Substructure_ID"),
        col("Test_Result_Order_Num"),
        col("Sub_Session_Seq_Num"),
        col("Active_Inactive_Core_Vector"),
        col("Pass_Fail_Core_Vector"),
        col("Mask_Vector"),
        col("Test_Result_Array_Seq_Num")))
      .withColumn("rowKey",
        concat_ws("\u0000", col("Lot"), col("Lato_Start_WW"), col("Lots_seq_key"),
          col("Unit_Testing_Seq_Key")))
      .select(
        col("rowKey"),
        getSaltValueUdf(col("rowKey")).alias("saltValue"),
        col("Test_Name").alias("columnName"),
        col("Value"))


    if (debugFlag == "Y")
      linesDFWithRowConcat.show(false)

    val linesDFGrouped = linesDFWithRowConcat.groupBy("rowKey", "columnName")
      .agg(collect_list(col("Value")).alias("fileValue"),
        max(col("saltValue")).alias("saltValue"))
    //concat the valuesList from above so that it can be compared with the tabledata
    val linesDFSplit = linesDFGrouped.map(r => (r.getAs[String]("rowKey"),
      r.getAs[String]("columnName"),
      r.getSeq[String](2).mkString("\n"),
      r.getAs[Byte]("saltValue")
    ))
    val linesDFFinal = linesDFSplit.toDF("fileRowKey", "fileColumnName", "fileValue", "fileSaltValue").coalesce(4)
    linesDFFinal
  }

  /**
   * Below function is to delete the folder and its contents in HDFS.
   *
   * @param HdfsFolderlocation
   * @return
   */
  def DeleteFolderFromHDFS(HdfsFolderlocation: String) = {

    var fs: FileSystem = null
    var status: Boolean = false
    try {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val hdfs = new Path(HdfsFolderlocation)
      if (fs.exists(hdfs)) {
        fs.delete(hdfs, true)
      } else {

        status = false
      }
    } catch {
      case e: Exception => e.printStackTrace()

    } finally // Close file descriptors
      if (fs != null)
        fs.close()
    status
  }

  def getTestLogWriter(logFilePathStr: String, conf: Configuration): FSDataOutputStream = {
    val fs = FileSystem.get(conf)
    val logFilePath = new Path(logFilePathStr)
    fs.create(logFilePath, true) //returns a DataouputStream
  }

  def convertFileToCSV(inputFilePath: String, outputFilePath: String, addColNumber: String): Unit = {

    val writer = new FileWriter(outputFilePath)
    if (addColNumber == "Y")
      writer.write(((0 until 200) mkString ",") + "\n")


    val fileBufferedSource = getFileBufferedSourcelocal(inputFilePath)

    for (line <- fileBufferedSource.getLines()) {
      writer.write(line.replace("\u0000", ",") + "\n")
    }

  }

  def haseTableToDataFrame(spark: SparkSession, table_name: String, inputRDD: RDD[(String, Byte,String)], splitColumns: Boolean): DataFrame = {
    val hbaseConfiguration = HBaseConfiguration.create()
   // val zookeeperQuorum = "chm4en002.mden.cps.intel.com,chm4en004.mden.cps.intel.com,chm4en005.mden.cps.intel.com"
    //hbaseConfiguration.set("hbase.zookeeper.quorum", zookeeperQuorum)
    val tableName = TableName.valueOf(table_name)
    val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConfiguration)


    val getResultRDD = hbaseContext.bulkGet(tableName, 1000, inputRDD,
      (rec: (String, Byte,String)) => {
   //TODO:
        val rowKey = rec._1
        val saltValue = Array(rec._2)
        val rowKeyBytes = Bytes.add(saltValue, Bytes.toBytes(rowKey))

        val columnName = Bytes.toBytes(rec._3)
        val colFam = Bytes.toBytes("0")

        //val valueList = rec.getSeq(2) //valueList from the RDD in position 2 starting from 0
        val get = new Get(rowKeyBytes)

        get.addColumn(colFam, columnName)
        get
      }
      , (result: Result) => {
        //each get call gets a result object. Each mutation.
        val cellScanner = result.cellScanner()
        var resultList = List[Row]()
        while (cellScanner.advance()) {
          val cell: Cell = cellScanner.current()
          val rowBytes = CellUtil.cloneRow(cell)
          val rowKey = Bytes.toString(Bytes.tail(rowBytes, Bytes.len(rowBytes) - 1))
          val columnName = Bytes.toString(CellUtil.cloneQualifier(cell))
         //val columnNameremoved = columnName.substring(1)
          val value = Bytes.toString(CellUtil.cloneValue(cell))
         // val valRemoved = value.split("\u0000").tail.mkString("\u0000")
         // val valRemoved = value.split("\u0000").drop(1).mkString("\u0000")
          val index = value.indexOf("\u0000")
          val valremoved = value.substring(index+1)
         // val result = rowKey + ":" + columnName + ":" + valremoved.replace("\u0000", ":")
          //val valueList = valremoved.split("\n")
          resultList = Row(rowKey, columnName, valremoved) :: resultList
        }
        //result.getRow.clone() + ret
        resultList
      }
    ) //end of the bulk Get call
    println("Before Printing")
    val getResultsFlat = getResultRDD.flatMap(x => x) //convert from RDD[List[Row]] to RDD[Row]

    val outputSchema = new StructType()
      .add("tableRowKey", StringType)
      .add("tableColumnName", StringType)
      .add("tableValue", StringType)

    //getResultsFlat.take(20).foreach(println)
    val getResultsDF = spark.createDataFrame(getResultsFlat, outputSchema)

    if (splitColumns)
      {
        //level1 split. For the tableValue two level splits are needed

        val getResultsSplitsDF1= getResultsDF.withColumn("tableRowKeySplits",
        split(col("tableRowKey"),"\u0000"))
        .withColumn("tableColumnNameSplits" ,
            split(col("tableColumnName"),"\u0000"))
            .withColumn("tableValueSplits" ,
              split(col("tableValue"),"\n"))

        getResultsSplitsDF1.show(false)
        val getResultsSplitsDF2 = getResultsSplitsDF1.withColumn("tableValueRows",explode(col("tableValueSplits")))
          .drop("tableRowKey","tableColumnName","tableValue","tableValueSplits")
        getResultsSplitsDF2.show(false)

        val getResultsSplitsDF3 = getResultsSplitsDF2.
          withColumn("tableValueSplits",split(col("tableValueRows"),"\u0000"))
        getResultsSplitsDF3.show(false)
      }

    getResultsDF.show(false)
    getResultsDF
  }

  /**
   * Input dataframe that contains the data from the hbase table.
   * The method will split the columns and return the dataframe
   * @param inputDF : Input Dataframe with the HBase Data
   * @return : Dataframe with the columns split.
   */
  def hbaseDataFrameSplit(inputDF: DataFrame) : DataFrame = {
      val getResultsSplitsDF1= inputDF.withColumn("tableRowKeySplits",
        split(col("tableRowKey"),"\u0000"))
        .withColumn("tableColumnNameSplits" ,
          split(col("tableColumnName"),"\u0000"))
        .withColumn("tableValueRowsConcat" ,
          split(col("tableValue"),"\n"))

      getResultsSplitsDF1.show(false)
      val getResultsSplitsDF2 = getResultsSplitsDF1.withColumn("tableValueRows",explode(col("tableValueRowsConcat")))
        .drop("tableRowKey","tableColumnName","tableValue","tableValueRowsConcat")
      getResultsSplitsDF2.show(false)

      val getResultsSplitsDF3 = getResultsSplitsDF2.
        withColumn("tableValueSplits",split(col("tableValueRows"),"\u0000"))
      getResultsSplitsDF3.show(false)
      getResultsSplitsDF3
/*
    val getResultsCol1= getResultsSplitsDF3.selectExpr("tableRowKeySplits[0] as Lato_Start_WW" ,"tableRowKeySplits[1] as Lot",
      "tableRowKeySplits[2] as Lots_Seq_key","tableRowKeySplits[3] as Unit_Testing_Seq_Key",
      "tableColumnNameSplits","tableValueSplits"
    )
    getResultsCol1.show(false)
     getResultsCol1 */

    }


  /**
   * @param spark      : Spark session to connect to the spark
   * @param table_name : Table name in HBase to query the records
   * @param inputDF    : Input Dataframe containing three columns : rowKey, saltValue, columnName (Testname for the tables,
   * @return : Returns a Dataframe with the rowKey, columnName, value . The columnName will be the TestName
   */
  def haseTableToDataFrame(spark: SparkSession, table_name: String, inputDF: DataFrame, splitColumns: Boolean): DataFrame = {
    val hbaseConfiguration = HBaseConfiguration.create()
    val zookeeperQuorum = "chm4en002.mden.cps.intel.com,chm4en004.mden.cps.intel.com,chm4en005.mden.cps.intel.com"
    hbaseConfiguration.set("hbase.zookeeper.quorum", zookeeperQuorum)
    val tableName = TableName.valueOf(table_name)
    val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConfiguration)
    val inputDFRdd = inputDF.rdd


    val getResultRDD = hbaseContext.bulkGet(tableName, 1000, inputDFRdd,
      (rec: Row) => {

        val rowKey = rec.getAs[String]("fileRowKey")
        val saltValue = Array(rec.getAs[Byte]("fileSaltValue"))
        val rowKeyBytes = Bytes.add(saltValue, Bytes.toBytes(rowKey))

        val columnName = Bytes.toBytes(rec.getAs[String]("fileColumnName"))
        val colFam = Bytes.toBytes("0")

        //val valueList = rec.getSeq(2) //valueList from the RDD in position 2 starting from 0
        val get = new Get(rowKeyBytes)

        get.addColumn(colFam, columnName)
        get
      }
      , (result: Result) => {
        //each get call gets a result object. Each mutation.
        val cellScanner = result.cellScanner()
        var resultList = List[Row]()
        while (cellScanner.advance()) {
          val cell: Cell = cellScanner.current()

          val rowBytes = CellUtil.cloneRow(cell)
          val rowKey = Bytes.toString(Bytes.tail(rowBytes, Bytes.len(rowBytes) - 1))
          val columnName = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          val valRemoved = value.split("\u0000").tail.mkString("\u0000")
          println("********First Value Removing**************" + valRemoved)
          val result = rowKey + ":" + columnName + ":" + value.replace("\u0000", ":")
          val valueList = value.split("\n")
          resultList = Row(rowKey, columnName, value) :: resultList
        }
        //result.getRow.clone() + ret
        resultList
      }
    ) //end of the bulk Get call
    println("Before Printing")
    val getResultsFlat = getResultRDD.flatMap(x => x) //convert from RDD[List[Row]] to RDD[Row]

    val outputSchema = new StructType()
      .add("tableRowKey", StringType)
      .add("tableColumnName", StringType)
      .add("tableValue", StringType)

    //getResultsFlat.take(20).foreach(println)
    val getResultsDF = spark.createDataFrame(getResultsFlat, outputSchema)

    getResultsDF.show(false)
    getResultsDF
  }

  def getHDFSFileWriter(logFilePathStr: String, conf: Configuration): FSDataOutputStream = {
    val fs = FileSystem.get(conf)
    val logFilePath = new Path(logFilePathStr)
    fs.create(logFilePath, true) //returns a DataouputStream
  }

  /**
   * Convert MDS_Unit_counter table into Dataframe given the following inputs
   *
   * @param spark        : SparkSession
   * @param dataFilePath : full path to the datafile in HDFS
   * @param trigFilePath : Path to the trigger file that contains he lot, lato_start_ww,lots_seq key
   * @return
   */
  def MUCR_fileToDataFrame(spark: SparkSession, dataFilePath: String, trigFilePath: String): DataFrame = {
    import spark.sqlContext.implicits._
    val debugFlag = "N"

    /**
     * The MUCR has a structure that contains a few columns at the beginning and an array of columns in the end. Each record has a number of repeating value and the
     * file contains the number of such values
     */

    val getSaltValueUdf = udf(getSaltValue(_: String): Byte)


    //using datasets
    val fileDS = spark.read.textFile(dataFilePath)
    if (debugFlag == "Y")
      fileDS.show(10, false)


    val filesDS1 = fileDS.map(line => {
      val splits = line.split("\u0000")
      val noOfCols = splits(5).toInt
      val remaining = splits.splitAt(6)._2
      /* getting the hexadecimal value of the unit_counter_id so that it can be used in the rowkey generation. The column is treated as string to get the lenght and then
      get the hexadecimal value of the lenght
       */

      val split_arr = (0 to noOfCols - 1) map (i => (remaining(i * 3), remaining(i * 3).length.toHexString, remaining((i * 3) + 1), remaining((i * 3) + 2)))
      //(splits(0) , splits(1), splits(2) , splits(3),splits(4),splits(5) ,Array(splits(6) ,splits(7)) )
      (splits(0), splits(1), splits(2), splits(3), splits(4), splits(5), split_arr)
    })

    //converting to dataframe
    if (debugFlag == "Y")
      filesDS1.show(false)

    val filesDF1 = filesDS1.toDF("Unit_Testing_Seq_Key", "Sub_Session_Seq_Num", "Test_Program_Name",
      "unit_counter_pass_fail_flg", "Substructure_ID", "number_of_counters", "remaining")
    if (debugFlag == "Y")
      filesDF1.show(2, false)

    val filesDFexploded = filesDF1.select(col("Unit_Testing_Seq_Key")
      , col("Sub_Session_Seq_Num")
      , col("Test_Program_Name")
      , col("unit_counter_pass_fail_flg")
      , col("Substructure_ID")
      , col("number_of_counters")
      , explode(col("remaining")))

    val filesDFFinal = filesDFexploded.selectExpr("*", "remaining._1 as Unit_Counter_ID", "remaining._2 as UCID_hex",
      "remaining._3 as Repeating_Counter_Occurrences"
      , "remaining._4 as Unit_Counter_Seq_Num")

    if (debugFlag == "Y")
      filesDFFinal.show(false)

    val linesDFWithLot = addMetaDataFromTrigFile(spark, dataFilePath, trigFilePath, debugFlag, filesDFFinal)

    if (debugFlag == "Y")
      linesDFWithLot.show(false)

    val linesDFWithRowConcat = linesDFWithLot.withColumn("columnName", concat(col("unit_counter_pass_fail_flg"), col("UCID_hex")
      , col("Unit_Counter_ID")))
      .withColumn("rowKey",
        concat_ws("\u0000", col("Lot"), col("Lato_Start_WW"), col("Lots_seq_key"),
          col("Unit_Testing_Seq_Key")))
      .withColumn("Value", concat_ws("\u0000", col("Unit_Counter_Seq_Num"), col("Substructure_ID"), col("Repeating_Counter_Occurrences")))
      .select(
        col("rowKey"),
        getSaltValueUdf(col("rowKey")).alias("saltValue"),
        col("Test_Name").alias("columnName"),
        col("Value"))

    if (debugFlag == "Y")
      linesDFWithRowConcat.show(false)

    val linesDFGrouped = linesDFWithRowConcat.groupBy("rowKey", "columnName")
      .agg(collect_list(col("Value")).alias("fileValue"),
        max(col("saltValue")).alias("saltValue"))
    //concat the valuesList from above so that it can be compared with the tabledata
    val linesDFSplit = linesDFGrouped.map(r => (r.getAs[String]("rowKey"),
      r.getAs[String]("columnName"),
      r.getSeq[String](2).mkString("\n"),
      r.getAs[Byte]("saltValue")
    ))
    val linesDFFinal = linesDFSplit.toDF("fileRowKey", "fileColumnName", "fileValue", "fileSaltValue").coalesce(4)
    linesDFFinal
  }


  private[midas] def addMetaDataFromTrigFile(spark: SparkSession, dataFilePath: String, trigFilePath: String, debugFlag: String, inputDF: DataFrame): DataFrame = {

    val keys = getMetaDataFromTrigFile(spark, dataFilePath, trigFilePath, debugFlag)

    val DFWithLot = inputDF.withColumn("Lot", lit(keys._1))
      .withColumn("Lato_Start_WW", lit(keys._2))
      .withColumn("Lots_seq_key", lit(keys._3))

    DFWithLot
  }

  private[midas] def getMetaDataFromTrigFile(spark: SparkSession, dataFilePath: String, trigFilePath: String, debugFlag: String): (String, Int, Int) = {
    val inputFilePath = new Path(dataFilePath)
    val fileName = inputFilePath.getName
    //metadata file can be converted into a broadcast variable
    val metadataFile = spark.read.format("csv").option("header", true).option("inferSchema", true)
      .load(trigFilePath)

    if (debugFlag == "Y")
      metadataFile.show(false)

    val metaDataRow = metadataFile.select("File_Name", "Lot", "Lato_Start_WW", "Lots_seq_key").
      withColumn("Lato_Start_WW_Int",col("Lato_Start_WW").cast(IntegerType))
      .where(col("File_Name") === fileName).first()
    //println("***metaDataRow***"+metaDataRow)
    val Lot = metaDataRow.getString(1)
    val Lato_Start_WW = metaDataRow.getInt(4)
    val Lots_seq_key = metaDataRow.getInt(3)

    (Lot, Lato_Start_WW, Lots_seq_key)
  }

  val zeroByteStr = "\"\\u0000\""


  /**
   * Creates the expression object using passed columns for the contatenation purposes with the separator of ZeroBytes
   *
   * @param cols : A variadic input of column name strings
   * @return : Column to be used for dataframe SQL
   */
  def getExprWs(cols: String*): Column = {
    val concatStr = cols.mkString(",", ",", ")")
    expr("concat_ws(\"\\u0000\"" + concatStr)
  }

  def getExpr(cols: String*): Column = {
    val concatStr = cols.mkString("(", ",", ")")
    expr("concat" + concatStr)
  }


  def getFileListFromPath(filePath: String, conf: Configuration): Option[RemoteIterator[LocatedFileStatus]] = {
    val fs = FileSystem.get(conf);
    val inputFilePath = new Path(filePath);
    val fileStatus = fs.getFileStatus(inputFilePath)

    if (fileStatus.isDirectory) {

      val fileStatusArray = fs.listFiles(inputFilePath, false)
      Some(fileStatusArray)
    }
    else {
      None
    }

  }

  def runTestRunner(compareType: String, hdfsPath: String,
                            logPath: String, trigFilePath: String, spark: SparkSession,
                            conf: Configuration , configFilePath:String): Unit = {
    val logFileName = new Path(hdfsPath).getName + "_test_log_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-H_mm_ss")) + "_.log"
    val testLogWriter = getHDFSFileWriter(logPath + "/" + logFileName, conf)
    //run all the tests and connect the results for summary reporting
    val config_values = Map(
      "data_file_path" -> hdfsPath,
      "trig_file_path" -> trigFilePath,
      "compareType" -> compareType,
      "configFilePath" -> configFilePath
    )
  }
    def writeToFilePath(content: String, filePath: String, conf: Configuration): Boolean = {
    val fs = FileSystem.get(conf);
    val inputFilePath = new Path(filePath);
    val exists = fs.exists(inputFilePath)
    if (exists) {
      val fileStatus = fs.getFileStatus(inputFilePath)
      if (fileStatus.isFile) {
        //println("Input path is a file, writing to file started..")
        val wr = fs.append(new Path(filePath));
        wr.write(content.getBytes);
        wr.close();
        true
      } else {

        false
      }
    }
    else {
      val wr = fs.create(new Path(filePath));
      wr.write(content.getBytes);
      wr.close();
      true
    }
  }


  /** @Params: list of the parameters
   * @return: returns a tuple that has the Lot,Lato_Start_WW and the Lots_seq_key
   *
   */
  def getMetadata(trigFilePath: String, fileNameToSearch: String, delim: String, conf: Configuration): Tuple3[String, String, String] = {

    /**
     * The metadafile will have entries as below. The first line will be a header and the next set of lines will correspond to each of the files in the batch. A batch can consist of 2000 files and each
     * file needs to have the metadata to process. In a batch there can be several files that are to be loaded for the MDS_Parametric_result table. Similarly there will be files for the other tables also.
     * The format will be as below. Assuming the batch is loading for three lots : A,B,C, the file name that we are currently loading is is lotA_202032_mds_parametric_result
     *
     * Metadata File format:
     * FileName                                ,      Lot,          Lato_Start_WW,       Lot_seq_key
     * lotA_202032_mds_parametric_result.dat              A                  202032               2
     * lotA_202032_mds_unit_result.dat                    A                  202032               2
     * lotB_202032_mds_parametric_result.dat              B                  202032               1
     * lotC_202032_mds_parametric_result.dat              C                  202032               1
     *
     * We need to search for the record for the current file that we are loading and then pick the metadata for the corresponding file
     *
     */

    /*
     * Step 1: get the filename from the overall path. The overall path in hdfs will be /data/ezone/mds/ingestion/input/LotA_202032_mds_parametric_result.dat
     */
    try {
      val inputFilePath = new Path(fileNameToSearch);

      ///data/ezone/mds/spark_imdata/ingestion/sample/A06!U015A700_6261_1A_ALL_20200616104113_MDS_Unit_String_Test_Result_Input.dat
      val fileName = inputFilePath.getName //returns the name of the file to search for
      println("fileName:" + fileName)
      println("trigFilePath:" + trigFilePath)
      //A06!U015A700_6261_1A_ALL_20200616104113_MDS_Unit_String_Test_Result_Input.dat
      val buf = getFileBufferedSource(trigFilePath, conf)


      var foundFile = false

      var sa1 = Array[String]()
      for (line <- buf.getLines()) {
        if (!foundFile) {
          sa1 = line.split(delim)
          if (sa1(0) == fileName) {
            foundFile = true
          }
        }
      }

      if (foundFile) {
        println("file is found in the metadata file")
        (sa1(1).trim, sa1(2).trim, sa1(3).trim)
      } else {
        println("file is not found in the metadata file")
        ("File Missing","File Missing","File Missing")
      }

    }
    catch {
      case ar:ArrayIndexOutOfBoundsException => ("Lot Missing","Lato Missing","Lots_Seq_key Missing")
      case e: Exception => {
        println(fileNameToSearch); throw e
      }
    }
  }

  /** @Params: list of the parameters
   * @return: returns a tuple that has the Lot,Lato_Start_WW and the Lots_seq_key
   *
   */
  def getMetadata_new(trigFilePath: String, fileNameToSearch: String, delim: String, conf: Configuration): Tuple5[String, String, String, String, String] = {

    /**
     * The metadafile will have entries as below. The first line will be a header and the next set of lines will correspond to each of the files in the batch. A batch can consist of 2000 files and each
     * file needs to have the metadata to process. In a batch there can be several files that are to be loaded for the MDS_Parametric_result table. Similarly there will be files for the other tables also.
     * The format will be as below. Assuming the batch is loading for three lots : A,B,C, the file name that we are currently loading is is lotA_202032_mds_parametric_result
     *
     * Metadata File format:
     * FileName                                ,      Lot,          Lato_Start_WW,       Lot_seq_key
     * lotA_202032_mds_parametric_result.dat              A                  202032               2
     * lotA_202032_mds_unit_result.dat                    A                  202032               2
     * lotB_202032_mds_parametric_result.dat              B                  202032               1
     * lotC_202032_mds_parametric_result.dat              C                  202032               1
     *
     * We need to search for the record for the current file that we are loading and then pick the metadata for the corresponding file
     *
     */

    /*
     * Step 1: get the filename from the overall path. The overall path in hdfs will be /data/ezone/mds/ingestion/input/LotA_202032_mds_parametric_result.dat
     */
    try {
      val inputFilePath = new Path(fileNameToSearch);

      ///data/ezone/mds/spark_imdata/ingestion/sample/A06!U015A700_6261_1A_ALL_20200616104113_MDS_Unit_String_Test_Result_Input.dat
      val fileName = inputFilePath.getName //returns the name of the file to search for
      println("fileName:" + fileName)
      println("trigFilePath:" + trigFilePath)
      //A06!U015A700_6261_1A_ALL_20200616104113_MDS_Unit_String_Test_Result_Input.dat
      val buf = getFileBufferedSource(trigFilePath, conf)


      var foundFile = false

      var sa1 = Array[String]()
      for (line <- buf.getLines()) {
        if (!foundFile) {
          sa1 = line.split(delim)
          if (sa1(0) == fileName) {
            foundFile = true
          }
        }
      }

      if (foundFile) {
        println("file is found in the metadata file")
        (sa1(1).trim, sa1(2).trim, sa1(3).trim, sa1(4).trim, sa1(5).trim)
      } else {
        println("file is not found in the metadata file")
        ("File Missing","File Missing","File Missing","File Missing","File Missing")
      }

    }
    catch {
      case ar:ArrayIndexOutOfBoundsException => ("Lot Missing","Lato Missing","Lots_Seq_key Missing","Facility Missing","Operation Missing")
      case e: Exception => {
        println(fileNameToSearch); throw e
      }
    }
  }

   def getTeradataDataframe(spark: SparkSession,configpath: String,execQuery: String): DataFrame = {

    val jobConfigs = new Configurations()
    val jobConfig = jobConfigs.properties(configpath)
    val tdDbName = jobConfig.getString("teradata.database.host")
    val tdUserName = jobConfig.getString("teradata.database.user")
    val tdPassword = jobConfig.getString("teradata.database.password")

    //val execQuery = s"""(SELECT lato_start_ww as TD_WW, Lot as TD_Lot, lots_seq_key as TD_lots_seq_key,unit_testing_seq_key as TD_unit_testing_seq_key,Test_Name as TD_Test_Name FROM  midas_ent.MDS_Unit_Parametric_Result_With_Lot WHERE Lot='8027C406') UPR"""

    //val execQuery = s"""(SELECT TOP 10000 lato_start_ww as TD_WW, Lot as TD_Lot, lots_seq_key as TD_lots_seq_key,unit_testing_seq_key as TD_unit_testing_seq_key,Test_Name as TD_Test_Name FROM  midas_ent.MDS_Unit_Parametric_Result_With_Lot WHERE Lot='8027C406') UPR"""

    println("ExecQuery: " + execQuery)

    val df1 = spark.read.format("jdbc")
      .option("driver", "com.teradata.jdbc.TeraDriver")
      .option("url", "jdbc:teradata://" + tdDbName + "/USER=" + tdUserName + ",PASSWORD=" + tdPassword)
      .option("dbtable", execQuery)
      .option("user", tdUserName)
      .option("password", tdPassword)
      .load()

    df1.show(4,false)
     df1

  }

  def getTeradataConnection(configPath:String): TeradataConnection= {
    val jobConfigs = new Configurations()
    val jobConfig = jobConfigs.properties(configPath)
    val tdDbName = jobConfig.getString("teradata.database.host")
    val tdUserName = jobConfig.getString("teradata.database.user")
    val tdPassword = jobConfig.getString("teradata.database.password")

    Class.forName("com.teradata.jdbc.TeraDriver")
    val tdConnStr = "jdbc:teradata://" + tdDbName + "/USER=" + tdUserName + ",PASSWORD=" + tdPassword
    DriverManager.getConnection(tdConnStr)
  }


  /**
   * This function was created since the splits on the array ignore the trailing null bytes. See the tests for an example
   * @param str : string to parse and convert to array of strings
   * @param delim: delimiter to be used. Default zero byte
   * @return : Array[String]
   *
   */
  def getSplitsFromString(str: String, delim:String= DELIM) : Array[String] = {
    var startPos = 0
    var nextPos = startPos
    val arrayBuffer = new ArrayBuffer[String]()
    while (nextPos != -1) {
      nextPos = str.indexOf(delim, startPos)
      if (nextPos != -1) {
      //  println(str.substring(startPos, nextPos))
        arrayBuffer.+=(str.substring(startPos, nextPos))
      }
      else  {
        //println(str.substring(startPos))
        arrayBuffer.+=(str.substring(startPos))
      }
      startPos = nextPos + 1
    }
    arrayBuffer.toArray
  }
}