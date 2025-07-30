package util

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive商品概况分析作业
 * 从Hive表中读取商品概况数据，只分析昨天的数据，按商店ID分组
 */
object HiveProductOverviewJob {

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "HiveProductOverview"
    val spark: SparkSession = MyHive.conn

    try {
      // 设置Spark配置，增加显示字段数
      spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
      
      println("成功连接到Hive")
      
      // 使用mall_bbc和user_tag数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")
      
      // 获取昨天的日期
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      cal.add(Calendar.DAY_OF_MONTH, -1) // 昨天
      val yesterday = dateFormat.format(cal.getTime())
      
      // 计算一周前的日期（从昨天往前推7天）
      val weekAgoCal = Calendar.getInstance()
      weekAgoCal.add(Calendar.DAY_OF_MONTH, -7)
      val weekAgo = dateFormat.format(weekAgoCal.getTime())
      
      println(s"分析日期范围: $weekAgo 至 $yesterday")
      println("按商店ID分组处理所有商店数据")
      
      // 临时注册埋点视图
      registerExposureView(spark, weekAgo, yesterday)
      
      // 执行商品概况分析查询
      println("执行商品概况分析查询...")
      val productOverviewSQL = s"""
      -- 商品概况分析SQL，分析一周的数据，按商店ID分组
      WITH 
      -- 基础商品信息（新增商品）
      new_products AS (
          SELECT 
              shop_id,
              COUNT(prod_id) AS newProd
          FROM mall_bbc.t_ods_tz_prod
          WHERE status != '-1' 
              AND create_time BETWEEN '$weekAgo 00:00:00' AND '$yesterday 23:59:59'
          GROUP BY shop_id
      ),
      
      -- 被访问商品数
      visited_products AS (
          SELECT 
              shop_id,
              COUNT(DISTINCT prod_id) AS visitedProd
          FROM temp_exposure
          GROUP BY shop_id
      ),
      
      -- 动销商品数
      dynamic_sales AS (
          SELECT 
              o.shop_id,
              COUNT(DISTINCT oi.prod_id) AS dynamicSale
          FROM mall_bbc.t_ods_tz_order o
          LEFT JOIN mall_bbc.t_ods_tz_order_item oi 
              ON o.order_number = oi.order_number
          WHERE o.create_time >= '$weekAgo 00:00:00'
              AND o.create_time <= '$yesterday 23:59:59'
          GROUP BY o.shop_id
      ),
      
      -- 商品曝光数和浏览数
      product_exposure AS (
          SELECT 
              shop_id,
              SUM(expose_count) AS expose,
              SUM(expose_count) AS browse  -- 使用相同的数据来源
          FROM temp_exposure
          GROUP BY shop_id
      ),
      
      -- 商品访客数
      product_visitors AS (
          SELECT 
              shop_id,
              COUNT(DISTINCT uuid) AS visitor
          FROM temp_exposure_users
          GROUP BY shop_id
      ),
      
      -- 下单件数
      order_items AS (
          SELECT 
              o.shop_id,
              SUM(CAST(oi.prod_count AS INT)) AS orderNum
          FROM mall_bbc.t_ods_tz_order o
          LEFT JOIN mall_bbc.t_ods_tz_order_item oi 
              ON o.order_number = oi.order_number
          WHERE o.create_time >= '$weekAgo 00:00:00'
              AND o.create_time <= '$yesterday 23:59:59'
          GROUP BY o.shop_id
      ),
      
      -- 支付件数
      payment_items AS (
          SELECT 
              o.shop_id,
              SUM(CASE WHEN o.is_payed = 'true' THEN CAST(oi.prod_count AS INT) ELSE 0 END) AS payNum
          FROM mall_bbc.t_ods_tz_order o
          LEFT JOIN mall_bbc.t_ods_tz_order_item oi 
              ON o.order_number = oi.order_number
          WHERE o.create_time >= '$weekAgo 00:00:00'
              AND o.create_time <= '$yesterday 23:59:59'
          GROUP BY o.shop_id
      ),
      
      -- 所有商店ID列表（确保结果中包含所有相关商店）
      all_shops AS (
          SELECT shop_id FROM new_products
          UNION
          SELECT shop_id FROM visited_products
          UNION
          SELECT shop_id FROM dynamic_sales
          UNION
          SELECT shop_id FROM product_exposure
          UNION
          SELECT shop_id FROM product_visitors
          UNION
          SELECT shop_id FROM order_items
          UNION
          SELECT shop_id FROM payment_items
      )
      
      -- 最终查询，合并所有数据
      SELECT 
          s.shop_id,
          COALESCE(np.newProd, 0) AS newProd,
          COALESCE(vp.visitedProd, 0) AS visitedProd,
          COALESCE(ds.dynamicSale, 0) AS dynamicSale,
          COALESCE(pe.expose, 0) AS expose,
          COALESCE(pe.browse, 0) AS browse,
          COALESCE(pv.visitor, 0) AS visitor,
          COALESCE(oi.orderNum, 0) AS orderNum,
          COALESCE(pi.payNum, 0) AS payNum,
          '$yesterday' AS stat_date
      FROM 
          all_shops s
      LEFT JOIN new_products np ON s.shop_id = np.shop_id
      LEFT JOIN visited_products vp ON s.shop_id = vp.shop_id
      LEFT JOIN dynamic_sales ds ON s.shop_id = ds.shop_id
      LEFT JOIN product_exposure pe ON s.shop_id = pe.shop_id
      LEFT JOIN product_visitors pv ON s.shop_id = pv.shop_id
      LEFT JOIN order_items oi ON s.shop_id = oi.shop_id
      LEFT JOIN payment_items pi ON s.shop_id = pi.shop_id
      ORDER BY s.shop_id
      """
      
      val productOverviewDF = spark.sql(productOverviewSQL)
      println("商品概况分析查询结果:")
      productOverviewDF.show(Int.MaxValue, false) // 显示所有行，不截断
      
      // 写入MySQL
      val jdbcUrl = "jdbc:mysql://rm-2zedtr7h3427p19kcbo.mysql.rds.aliyuncs.com:3306/dlc_statistics?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai"
      val jdbcUser = "bigdata_statistics"
      val jdbcPassword = "Y&%20Am1!"
      val jdbcDriver = "com.mysql.cj.jdbc.Driver"
      val tableName = "tz_bd_merchant_product_overview"
      // 先删除该日期数据
      import java.sql.DriverManager
      Class.forName(jdbcDriver)
      val connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      val deleteSql = s"DELETE FROM `$tableName` WHERE stat_date = '$yesterday'"
      println(s"执行删除SQL: $deleteSql")
      val statement = connection.createStatement()
      val deletedRows = statement.executeUpdate(deleteSql)
      println(s"从 $tableName 表中删除了 $deletedRows 行数据 (日期: $yesterday)")
      connection.close()
      // 写入新数据
      productOverviewDF.write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("user", jdbcUser)
        .option("password", jdbcPassword)
        .option("driver", jdbcDriver)
        .mode("append")
        .save()
      println(s"成功写入数据到 $tableName 表 (日期范围: $weekAgo 至 $yesterday)")
      
      println("数据查询完成")

    } catch {
      case e: Exception => 
        println(s"错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // 关闭Spark会话
      spark.stop()
      println("Spark会话已关闭")
    }
  }
  
  /**
   * 注册临时视图，用于分析埋点数据
   */
  private def registerExposureView(spark: SparkSession, startDate: String, endDate: String): Unit = {
    try {
      // 切换到user_tag数据库以获取埋点数据
      spark.sql("USE user_tag")
      println("切换到数据库: user_tag (用于查询埋点数据)")
      
      // 曝光视图 - 按商店ID分组
      val exposureSQL = s"""
        SELECT 
          CAST(prodid AS BIGINT) AS prod_id,
          CAST(shopid AS BIGINT) AS shop_id,
          COUNT(*) AS expose_count,
          COUNT(DISTINCT cid) AS expose_person_num
        FROM user_tag.t_ods_app_logdata
        WHERE dt >= '$startDate' AND dt <= '$endDate'
          AND action ='enter'
          AND page_id='1005'
          AND prodid IS NOT NULL
          AND shopid IS NOT NULL
        GROUP BY CAST(prodid AS BIGINT), CAST(shopid AS BIGINT)
      """
      
      // 曝光用户视图 - 按商店ID分组
      val exposureUsersSQL = s"""
        SELECT 
          cid AS uuid,
          CAST(prodid AS BIGINT) AS prod_id,
          CAST(shopid AS BIGINT) AS shop_id
        FROM user_tag.t_ods_app_logdata
        WHERE dt >= '$startDate' AND dt <= '$endDate'
          AND action ='enter'
          AND page_id='1005'
          AND prodid IS NOT NULL
          AND shopid IS NOT NULL
      """
      
      // 创建临时视图
      println("创建曝光临时视图...")
      val exposureDF = spark.sql(exposureSQL)
      exposureDF.createOrReplaceTempView("temp_exposure")
      
      println("创建曝光用户临时视图...")
      val exposureUsersDF = spark.sql(exposureUsersSQL)
      exposureUsersDF.createOrReplaceTempView("temp_exposure_users")
      
      // 切换回mall_bbc数据库处理订单相关数据
      spark.sql("USE mall_bbc")
      println("切换回数据库: mall_bbc (用于查询订单数据)")
      
    } catch {
      case e: Exception => 
        println(s"创建临时视图时出错: ${e.getMessage}")
        e.printStackTrace()
    }
  }
} 