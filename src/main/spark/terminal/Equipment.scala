package terminal

import org.apache.spark.sql.{DataFrame, SparkSession}
import util.RptUtils

object Equipment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //获取数据
    var logDF: DataFrame = spark.read.parquet("D:\\项目数据\\log.parquet")
    logDF.rdd.map(x=>{
      val requestmode = x.getAs[Int]("requestmode")
      val processnode = x.getAs[Int]("processnode")
      val iseffective = x.getAs[Int]("iseffective")
      val isbilling = x.getAs[Int]("isbilling")
      val isbid = x.getAs[Int]("isbid")
      val iswin = x.getAs[Int]("iswin")
      val adordeerid = x.getAs[Int]("adorderid")
      val winprice = x.getAs[Double]("winprice")
      val adpayment = x.getAs[Double]("adpayment")
      // 处理请求数
      val rptList = RptUtils.ReqPt(requestmode,processnode)
      // 处理展示点击
      val clickList = RptUtils.clickPt(requestmode,iseffective)
      // 处理广告
      val adList = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)
      // 所有指标
      val allList: List[Double] = rptList ++ clickList ++ adList
      val devicetype = {
        val dev = x.getAs[Int]("devicetype")
        if (dev == 1) {
          "手机"
        } else if (dev == 2) {
          "平板"
        }
      }
      (devicetype,allList)
    })
      .reduceByKey((list1,list2)=>{
        list1.zip(list2).map(t=>t._1+t._2)
      })
      .map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile("D:\\项目数据\\Equipment")
  }
}
