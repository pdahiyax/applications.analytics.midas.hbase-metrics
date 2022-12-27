package com.intel.midas

import java.io.{File, FileNotFoundException, IOException}
import scala.io.{BufferedSource, Source}

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration,TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{PrefixFilter,Filter}

import com.intel.midas.Utils.getFileBufferedSourcelocal

object MetricDetails {

  def getFileBufferedSourcelocal(filePath: String): BufferedSource = {
    val bufferedSource = Source.fromFile(new File(filePath), 4194304)
    bufferedSource
  }

   def logFileRead(filePath: String): (String, String, String) = {
     try {

       val filename = getFileBufferedSourcelocal(filePath)
       println("Reading Log file's data in filename variable")

       val status = filename.getLines().filter(line => line.contains("spark submit job is "))
       val finalLine = status.filter(line => line.contains("successful") || line.contains("failed")).toArray.head

         val JobRE = "([0-9]{6,12})+".r
         val jobName = JobRE.findFirstIn(finalLine)
         val job_seq_key = jobName match {
           case Some(y) => y
           case None => "Missing job_seq_key Number"
         }
         //println("Value of job_seq_key is: " + job_seq_key)

         val statusRE = "([a-zA-Z]{5,6} [a-zA-Z]{6,7} [a-zA-Z]{3,4} [a-zA-Z]{2,3} [a-zA-Z]{6,10})+".r
         val statusMetrics = statusRE.findFirstIn(finalLine)
         val jobStatus = statusMetrics match {
           case Some(y) => y.split(" ")(4)
           case None => "Missing status metrics"
         }
         //println("Job status is: " + jobStatus)

         val exitCodeRE = "([a-zA-Z]{4,5} [0-9]{1,3})+".r
         val exitCodeValue = exitCodeRE.findFirstIn(finalLine)
         val exitCode = exitCodeValue match {
           case Some(y) => y.split(" ")(1)
           case None => "Missing Exit Code"
         }
         //println("Exit code is: " + exitCode)

       filename.close
       println("Closed the file")

       (job_seq_key, jobStatus, exitCode)
     }

     catch {
       case e: FileNotFoundException => println(s"Couldn't find that file ${e.getMessage}"); throw e
       case e: IOException => println(s"Got an IOException! ${e.getMessage}"); throw e
     }
   }

  def main(args: Array[String]): Unit = {

    println("Started the scala program")

    val path = "/ldata/mds/logs/invokeSparkSubmit/261050_202212072303.log"
    val output = logFileRead(path)

    val jobseqkey = output._1
    val jobstatus = output._2
    val exitcode  = output._3

    println("Value of jobseqkey is: " + jobseqkey)
    println("Value of job status is: " + jobstatus)
    println("Value of exit code is: " + exitcode)

    val conf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin()

    // list the tables
    val listtables = admin.listTableNames()
    println("Printing all Hbase tables")
    listtables.foreach(println)

    val tableName = "MDS:MDS_METRIC"
    val table = TableName.valueOf(tableName)
    val HbaseTable = conn.getTable(table)

    println("Value of tableName is : " + tableName)
    println("Value of table is : " + table)
    println("Value of HbaseTable is : " + HbaseTable)

    // Initiate a new client scanner to retrieve records
    val scan = new Scan()

    // Set the filter condition and
    //val prfxValue = "IF140872000072022-07-0421"
    val prfxValue = s"IF${jobseqkey}"
    val filter: Filter = new PrefixFilter(Bytes.toBytes(prfxValue))

    scan.setFilter(filter)
    scan.addColumn(Bytes.toBytes("0"), Bytes.toBytes("LATO_START_WW"))
    scan.addColumn(Bytes.toBytes("0"), Bytes.toBytes("LOT"))
    scan.addColumn(Bytes.toBytes("0"), Bytes.toBytes("LOT_SEQ_KEY"))
    scan.addColumn(Bytes.toBytes("0"), Bytes.toBytes("START_DATE"))
    scan.addColumn(Bytes.toBytes("0"), Bytes.toBytes("END_DATE"))
    scan.addColumn(Bytes.toBytes("0"), Bytes.toBytes("LOADSTATUS"))
    scan.addColumn(Bytes.toBytes("0"), Bytes.toBytes("FILENAME"))
    scan.addColumn(Bytes.toBytes("0"), Bytes.toBytes("MESSAGE"))
    //scan.addColumn(Bytes.toBytes("0"), Bytes.toBytes("JOB_KEY"))

    val scanner = HbaseTable.getScanner(scan)
    println("Value of scanner is : " + scanner)

    try {
      var result = scanner.next()
      while ( result != null ) {
        println("Found result : " + result)

        val LATO_START_WW = result.getValue(Bytes.toBytes("0"), Bytes.toBytes("LATO_START_WW"))
        val LOT = result.getValue(Bytes.toBytes("0"), Bytes.toBytes("LOT"))
        val LOT_SEQ_KEY = result.getValue(Bytes.toBytes("0"), Bytes.toBytes("LOT_SEQ_KEY"))
        val START_DATE = result.getValue(Bytes.toBytes("0"), Bytes.toBytes("START_DATE"))
        val END_DATE = result.getValue(Bytes.toBytes("0"), Bytes.toBytes("END_DATE"))
        val LOADSTATUS = result.getValue(Bytes.toBytes("0"), Bytes.toBytes("LOADSTATUS"))
        val FILENAME = result.getValue(Bytes.toBytes("0"), Bytes.toBytes("FILENAME"))
        val MESSAGE = result.getValue(Bytes.toBytes("0"), Bytes.toBytes("MESSAGE"))

        println("Value of LATO_START_WW: " + Bytes.toString(LATO_START_WW))
        println("Value of LOT: " + Bytes.toString(LOT))
        println("Value of LOT_SEQ_KEY: " + Bytes.toString(LOT_SEQ_KEY))
        println("Value of START_DATE: " + Bytes.toString(START_DATE))
        println("Value of END_DATE: " + Bytes.toString(END_DATE))
        println("Value of LOADSTATUS: " + Bytes.toString(LOADSTATUS))
        println("Value of FILENAME: " + Bytes.toString(FILENAME))
        println("Value of MESSAGE: " + Bytes.toString(MESSAGE))

        result = scanner.next
      }
    }
    finally
    {
      scanner.close
    }

    HbaseTable.close()
    conn.close()

  }
}



