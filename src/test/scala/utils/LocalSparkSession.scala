package lu.magalhaes.gilles.provxlib
package utils

import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait LocalSparkSession extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: Suite =>

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")

    GraphXUtils.registerKryoClasses(sparkConf)

    spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    try {
      LocalSparkSession.stop(spark)
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      spark = null
    } finally {
      super.afterEach()
    }
  }
}

object LocalSparkSession {
  def stop(spark: SparkSession): Unit = {
    if (spark != null) {
      spark.stop()
    }
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSparkSession[T](sc: SparkSession)(f: SparkSession => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

}
