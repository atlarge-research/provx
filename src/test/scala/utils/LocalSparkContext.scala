package lu.magalhaes.gilles.provxlib
package utils

import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.{SparkConf, SparkContext}

trait LocalSparkContext {
  /** Runs `f` on a new SparkContext and ensures that it is stopped afterwards. */
  def withSpark[T](f: SparkContext => T): T = {
    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext("local", "test", conf)
    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }
}
