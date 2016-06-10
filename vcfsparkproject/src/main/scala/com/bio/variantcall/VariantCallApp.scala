/*
 */

package com.bio.variantcall

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object VariantCallApp{
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Variant Call App"))
    println("num lines: " + countLines(sc, args(0)))
  }

  def countLines(sc: SparkContext, path: String): Long = {
    sc.textFile(path).count()
  }
}

