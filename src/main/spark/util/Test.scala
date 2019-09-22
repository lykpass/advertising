package util

import org.apache.spark.sql.SparkSession

/**
  * 测试工具类
  */
object Test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Tags").master("local[*]").getOrCreate()
    import spark.implicits._

    // 读取数据文件
    val df = spark.read.parquet("D:\\项目数据\\log.parquet")
    df.rdd.map(row=>{
      // 圈
      AmapUtil.getBusinessFromAmap(
        String2Type.toDouble(row.getAs[String]("long")),
        String2Type.toDouble(row.getAs[String]("lat")))
    }).foreach(println)
  }
}
