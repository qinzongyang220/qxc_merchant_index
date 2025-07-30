package activity

import dao.MyHive
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * 展示Hive表数据
 */
object ShowHiveDataJob {
  def main(args: Array[String]): Unit = {
    // 使用dao包的连接器连接Hive
    implicit val jobName: String = "ShowHiveDataJob"
    val spark: SparkSession = MyHive.conn
    
    // 设置日期参数
    val dataDate = if (args.length > 0) args(0) else "2025-06-08"
    
    try {
      // 设置显示选项，增加字符串列的最大显示长度
      spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
      
      // 从Hive表读取数据，添加page_id='1005'的过滤条件
      val query = "SELECT * FROM user_tag.t_ods_app_logdata WHERE dt = '" + dataDate + "' AND page_id = '1005'"
      
      // 执行查询
      val aa = spark.sql(query).show()
      val df = spark.sql(query)
      
      // 动态解析JSON并展示完整数据
      import spark.implicits._
      
      // 获取JSON样本并推断schema
      val jsonSample = df.select("label").limit(1).collect()(0).getString(0)
      val jsonSchema = spark.read.json(Seq(jsonSample).toDS()).schema
      
      // 解析JSON并与原始数据合并
      val resultDF = df.withColumn("json_data", from_json(col("label"), jsonSchema))
                       .select(col("*"), col("json_data.*"))
                       .drop("json_data", "label")
      
      // 显示最终结果
      resultDF.show(20, false)
      
    } finally {
      spark.stop()
    }
  }
}
