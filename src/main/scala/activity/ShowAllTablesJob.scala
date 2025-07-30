package activity

import dao.MyHive
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

/**
 * 展示Hive表数据任务
 */
object ShowAllTablesJob {
  def main(args: Array[String]): Unit = {

    // 设置日志级别为WARN，减少不必要的INFO日志
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // 使用dao包的连接器连接Hive
    implicit val jobName: String = "ShowAllTablesJob"
    val spark: SparkSession = MyHive.conn
    
    try {
      // 设置显示选项，增加字符串列的最大显示长度
      spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
      
      // 需要读取的表列表
      val allTables = Array(
        "t_ads_download_count",
        "t_ads_stats_by_snap",
        "t_ads_stats_daily_summary",
        "t_ads_stats_pv_uv",
        "t_ods_areaip",
        "t_ods_log_login_track",
        "t_ods_log_product_track",
        "t_ods_log_search_track",
        "t_ods_log_shop_track",
        "t_ods_log_tz_prod_comm",
        "t_ods_tz_brand_lang",
        "t_ods_tz_category",
        "t_ods_tz_category_brand",
        "t_ods_tz_category_lang",
        "t_ods_tz_order",
        "t_ods_tz_prod",
        "t_ods_tz_prod_comm",
        "t_ods_tz_properties",
        "t_ods_tz_properties_value",
        "t_ods_tz_shop_detail",
        "t_ods_tz_sku",
        "t_ods_tz_spec",
        "t_ods_tz_spec_value",
        "t_ods_tz_user",
        "t_ods_tz_user_addr",
        "t_ods_tz_user_addr_order",
        "t_ods_tz_user_category",
        "t_ods_tz_user_collection",
        "t_ods_tz_user_collection_shop"
      )
      
      // 遍历所有表并显示数据
      for (tableName <- allTables) {
        try {
          println(s"\n===== 表: $tableName =====")
          
          // 尝试检查表是否存在
          val checkQuery = s"DESCRIBE mall_bbc.$tableName"
          try {
            spark.sql(checkQuery)
            
            // 表存在，执行查询
            val query = s"SELECT * FROM mall_bbc.$tableName LIMIT 10"
            println(s"执行查询: $query")
            val df = spark.sql(query).show(false) // false表示不截断列
          } catch {
            case e: Exception =>
              println(s"表 $tableName 不存在或无法访问: ${e.getMessage}")
              println("跳过此表并继续...")
          }
        } catch {
          case e: Exception =>
            val errorMsg = e.getMessage
            val rootCause = if (e.getCause != null) e.getCause.getMessage else "未知"
            println(s"查询表 $tableName 时出错:")
            println(s"错误信息: $errorMsg")
            println(s"根本原因: $rootCause")
            println("跳过此表并继续...")
        }
      }
    } finally {
      spark.stop()
    }
  }
} 