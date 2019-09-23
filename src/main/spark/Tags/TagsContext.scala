package Tags

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import util.TagUtils
import org.apache.spark.sql.SparkSession
import util.TagUtils

/**
  * 上下文标签主类
  */
object TagsContext {

  def main(args: Array[String]): Unit = {
    if(args.length!=3){
      println("目录不正确")
      sys.exit()
    }

    val Array(inputPath,docs,stopwords)=args

    // 创建Spark上下文
    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()
    import spark.implicits._

    // 读取数据文件
    val df = spark.read.parquet(inputPath)
    //读取字典文件
    val docsRDD = spark.sparkContext.textFile(docs).map(_.split("\\s")).filter(_.length>=5)
        .map(arr=>(arr(4),arr(1))).collectAsMap()
    //广播字典
    val broadValue = spark.sparkContext.broadcast(docsRDD)
    // 读取停用词典
    val stopwordsRDD = spark.sparkContext.textFile(stopwords).map((_,0)).collectAsMap()
    // 广播字典
    val broadValues = spark.sparkContext.broadcast(stopwordsRDD)
    // 处理数据信息
    val res: RDD[(String, List[(String, Int)])] = df.rdd.map(row=>{
      // 获取用户的唯一ID
      val userId = TagUtils.getOneUserId(row)
      // 接下来标签 实现
      val adList = TagsAd.makeTags(row)
      // 商圈
      val businessList: Seq[(String, Int)] = BusinessTag.makeTags(row)
      // 媒体标签
      val appList = TagsAPP.makeTags(row,broadValue)
      // 设备标签
      val devList = TagsDevice.makeTags(row)
      // 地域标签
      val locList = TagsLocation.makeTags(row)
      // 关键字标签
      val kwList = TagsKword.makeTags(row,broadValues)
      (userId,adList++appList++businessList++devList++locList++kwList)
    }).reduceByKey((list1,list2)=>{
      (list1:::list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    })
   println(res.collect().toBuffer)
    // .map{
//      case (userId,userTags) =>{
//        // 设置rowkey和列、列名
//        val put = new Put(Bytes.toBytes(userId))
//        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
//        (new ImmutableBytesWritable(),put)
//      }
//    }.saveAsHadoopDataset(conf)
  }
}
