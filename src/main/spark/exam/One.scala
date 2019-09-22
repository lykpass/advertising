package com.weekTest

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object One {
  def main(args: Array[String]): Unit = {
    var list: List[List[String]] = List()
    val conf = new SparkConf().setAppName("li").setMaster("local")
    val sc = new SparkContext(conf)
    val js: RDD[String] = sc.textFile("C:\\Users\\13997\\Desktop\\考试\\json.txt")
    val jss = js.collect()
    for(i <- 0 until jss.length){
      val str: String = jss(i).toString
      val jsonparse: JSONObject = JSON.parseObject(str)
      val status = jsonparse.getIntValue("status")
      if(status == 0) return ""
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""
      val poisArray: JSONArray = regeocodeJson.getJSONArray("pois")
      if(poisArray == null || poisArray.isEmpty) return ""
      // 创建集合 保存数据
      val LB = collection.mutable.ListBuffer[String]()
      // 循环输出
      for(item <- poisArray.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json: JSONObject = item.asInstanceOf[JSONObject]
          val str: String = json.getString("businessarea")

          LB.append(json.getString("businessarea"))
        }
      }
      val list2: List[String] = LB.toList
      list:+=list2
    }
    val res = list.flatten(x=>x)
      .filter(x => x != "[]").map(x => (x, 1))
      .groupBy(x => x._1)
      .mapValues(x => x.size).toList
    res.foreach(println)
  }
}