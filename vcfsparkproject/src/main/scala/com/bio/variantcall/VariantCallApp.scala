/*
 */

package com.bio.variantcall

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


case class VariantData(chrom: String, pos: Int, id: String, 
    ref: String, alt: String)

object VariantCallApp{
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Variant Call App"))
    println("num lines: " + countLines(sc, args(0)))
  }

  def countLines(sc: SparkContext, path: String): Long = {
    sc.textFile(path).count()
  }
  def isTableHeader(line: String):Boolean = {
  	line.startsWith("#CHROM")
  }
  
  def isHeader(line: String) = line.contains("##")

  
  def testFileRead(filepath: String) {
      val rawblocks = sc.textFile(filepath)
      
      // read first line
      rawblocks.first

      //read first 10 lines
      val head = rawblocks.take(10)
      head.length

      //filter with function
      head.filter(isHeader).foreach(println)
      head.filterNot(isHeader).length
      head.filter(x => !isHeader(x)).length
      head.filter(!isHeader(_)).length

      //
      val line = head(5)
      val pieces = line.split(',')


  }
  
  def parseVariantLine(line: String) = {
      val pieces = line.split('\t')
      val chrom = pieces(0)
      val pos = pieces(1).toInt
      val id = pieces(2)
      val ref = pieces(3)
      val alt = pieces(4)

      VariantData(chrom,pos,id,ref,alt)
  }

  def vcfRead(){

    val filename = "./data/fb_sample.vcf"
    val vcf = sc.textFile("./data/fb_sample.vcf")
    vcf.filter(isHeader).foreach(println)

    vcf.count
    vcf.filter(isHeader).count
    vcf.filter(!isHeader(_)).count
    vcf.filter(!isHeader(_)).map(_.split("\t").drop(3).take(2))
    vcf.filter(!isHeader(_)).map(_.split("\t")).take(2)
    vcf.filter(!isHeader(_)).map(_.split("\t").take(2)).take(2)
    vcf.filter(!isHeader(_)).map(_.split("\t").take(5)).take(4)

    val vcf2 = sc.textFile("./data/gs_sample.vcf")

    var  s1 = vcf.filter(!isHeader(_)).map(_.split("\t").take(5)).take(4)
    var  s2 = vcf2.filter(!isHeader(_)).map(_.split("\t").take(5)).take(4)

    val line = s1(1)
    val pieces = s1(1)

    val noheader1 =  vcf.filter(!isHeader(_))
    val noheader2 =  vcf2.filter(!isHeader(_))

    val parseline = parseVariantLine(noheader1.take(5)(1))
    parseline.chrom

    val parsed1 = noheader1.map(line => parseVariantLine(line))


  }

  def bioWordCountSpark(filepath: String) {
  
    val file = sc.textFile(filepath)
    val rows = file.filter(!_.startsWith("#"))
    val refVar = rows.map(_.split("\t").drop(3).take(2))
    val refVarNoIndel = refVar.filter(refvar => (refvar(0).length() == 1) && (refvar(1).length() == 1))
    val answer = refVarNoIndel.map(data => (data(0)+" -> "+data(1) -> 1)).reduceByKey(_ + _)
    answer.sortByKey().saveAsTextFile("answer")
  
  }

  
  def inVarArray(refvar:Array[String], chr:String,pos:String) : Boolean = {
     (refvar(0) == chr) && (refvar(1) == pos)
  }

  def compare2vcf() {
    val filename = "./data/fb_sample.vcf"
    val file = sc.textFile(filename)
    val rows = file.filter(!_.startsWith("#"))
    val refVar = rows.map(_.split("\t").take(2))
    refVar.first
    refVar.filter(x => inVarArray(x,"chr11","190597")).count



    val filepath = "./data/gs_sample.vcf"
    val filegs = sc.textFile(filepath)
    val refValgs = filegs.filter(!_.startsWith("#")).map(_.split("\t").take(2))



  }
}

