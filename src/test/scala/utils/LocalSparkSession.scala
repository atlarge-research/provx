package lu.magalhaes.gilles.provxlib
package utils

import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

object LocalSparkSession {
  def stop(spark: SparkSession): Unit = {
    if (spark != null) {
      spark.stop()
    }
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSparkSession[T](f: SparkSession => T): T = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")

    GraphXUtils.registerKryoClasses(sparkConf)

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    try {
      f(spark)
    } finally {
      stop(spark)
    }
  }

}
