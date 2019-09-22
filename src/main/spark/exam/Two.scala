package com.weekTest

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Two {
  def main(args: Array[String]): Unit = {
    var list: List[String] = List()
    val conf = new SparkConf().setAppName("li").setMaster("local")
    val sc = new SparkContext(conf)
    val js: RDD[String] = sc.textFile("C:\\Users\\13997\\Desktop\\考试\\json.txt")
    val jss = js.collect()
    for(i <- 0 until jss.length) {
      val str: String = jss(i).toString
      val jsonparse: JSONObject = JSON.parseObject(str)
      val status = jsonparse.getIntValue("status")
      if (status == 0) return ""
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""
      val poisArray = regeocodeJson.getJSONArray("pois")
      if (poisArray == null || poisArray.isEmpty) return ""
      val LB = collection.mutable.ListBuffer[String]()
      for (item <- poisArray.toArray) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          LB.append(json.getString("type"))
        }
      }
      list:+=LB.mkString(";")
    }
    val res2: List[(String, Int)] = list.flatMap(x => x.split(";"))
      .map(x => ( x, 1))
      .groupBy(x => x._1)
      .mapValues(x => x.size).toList
    res2.foreach(x => println(x))
  }
}

