package it.nerdammer.spark.hbase.example

import org.apache.spark.{SparkConf, SparkContext}
import it.nerdammer.spark.hbase._

/**
 * Created by gaowei on 6/23/15.
 */
object HbaseExample {
  def main(args: Array[String]): Unit = {
    val master = args.filter(x => x.contains("master"))
    val addr = master(0).toString.split("=")(1)
    val sparkConf = new SparkConf().setAppName("SparkWithHBase")
    sparkConf.setMaster(addr)
    val sc = new SparkContext(sparkConf)
    //val tableName = "ds_ifeng_blog"
    val tableName = "ds_163_blog"

    // write example
    val rdd = sc.parallelize(1 to 100)
      .map(i => (i.toString, i+1, "Hello"))
    rdd.toHBaseTable(tableName)
      .toColumns("column1", "column2")
      .inColumnFamily("mycf")
      .save()

    // read example
    val hBaseRDD = sc.hbaseTable[(String, Int, String)](tableName)
      .select("column1", "column2")
      .inColumnFamily("mycf")
    println(hBaseRDD.collect().toString)
  }
}
