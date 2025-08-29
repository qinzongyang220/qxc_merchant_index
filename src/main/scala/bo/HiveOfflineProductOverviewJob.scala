package bo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import dao.MyHive
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive离线商品概况分析作业
 * 基于HiveRealtimeProductOverviewJob创建的离线版本
 * 从Hive全量表(full)中读取昨天的商品概况数据，按商店分组
 * 支持历史数据回填和日常离线处理
 */
object HiveOfflineProductOverviewJob {

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "HiveOfflineProductOverview"
    val spark: SparkSession = MyHive.conn

    try {
      // 设置Spark配置，增加显示字段数
      spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
      
      println("成功连接到Hive")
      
      // 使用mall_bbc和user_tag数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")
      
      // 处理昨天的数据
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      cal.add(Calendar.DAY_OF_MONTH, -1) // 昨天
      val yesterday = dateFormat.format(cal.getTime())
      
      println(s"处理昨天数据: $yesterday")
      processSingleDay(spark, yesterday)
      
      println("数据处理完成")

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
   * 处理单天数据
   */
  def processSingleDay(spark: SparkSession, targetDate: String): Unit = {
    val startTime = s"$targetDate 00:00:00"
    val endTime = s"$targetDate 23:59:59"
    
    println(s"分析时间范围: $startTime 至 $endTime (离线全天)")
    
    // 临时注册埋点视图
    registerExposureView(spark, targetDate)
    
    // 执行商品概况分析查询（按商店分组）
    println("执行商品概况离线分析查询（按商店分组）...")
    val productOverviewSQL = s"""
    -- 商品概况离线分析SQL，使用Hive全量表，按商店分组
    SELECT 
        orders.shop_id,
        '$targetDate' AS current_day,
        
        -- 新增商品数
        COALESCE(products.newProd, 0) AS new_prod,
        
        -- 被访问商品数（使用新的埋点逻辑）
        COALESCE(visited.visitedProd, 0) AS visited_prod,
        
        -- 动销商品数
        orders.dynamicSale AS dynamic_sale,
        
        -- 商品曝光数（使用埋点数据）
        COALESCE(exposure.expose, 0) AS expose,
        
        -- 商品浏览量（使用埋点数据）
        COALESCE(exposure.browse, 0) AS browse,
        
        -- 商品访客数（使用埋点数据）
        COALESCE(visitors.visitor, 0) AS visitor,
        
        -- 加购件数（使用默认值0）
        0 AS add_cart,
        
        -- 下单件数
        orders.orderNum AS order_num,
        
        -- 支付件数
        orders.payNum AS pay_num,
        
        -- 分享访问次数（使用埋点数据）
        COALESCE(share_visits.share_visit, 0) AS share_visit
        
    FROM (
        -- 订单相关统计
        SELECT
            o.shop_id,
            COALESCE(SUM(CAST(oi.prod_count AS INT)), 0) AS orderNum,
            COALESCE(SUM(CASE WHEN o.is_payed = '1' THEN CAST(oi.prod_count AS INT) ELSE 0 END), 0) AS payNum,
            COUNT(DISTINCT oi.prod_id) AS dynamicSale
        FROM mall_bbc.t_dwd_order_full o
        LEFT JOIN mall_bbc.t_dwd_order_item_full oi ON o.order_number = oi.order_number
        WHERE o.create_time >= '$startTime'
          AND o.create_time <= '$endTime'
        GROUP BY o.shop_id
    ) orders
    LEFT JOIN (
        -- 新增商品统计
        SELECT
            shop_id,
            COUNT(prod_id) AS newProd
        FROM mall_bbc.t_dwd_prod_full
        WHERE status != '-1'
          AND create_time >= '$startTime'
          AND create_time <= '$endTime'
        GROUP BY shop_id
    ) products ON orders.shop_id = products.shop_id
    LEFT JOIN (
        -- 被访问商品数（使用原逻辑：从曝光数据统计）
        SELECT 
            shop_id,
            COUNT(DISTINCT prod_id) AS visitedProd
        FROM temp_exposure
        GROUP BY shop_id
    ) visited ON orders.shop_id = visited.shop_id
    LEFT JOIN (
        -- 商品曝光数和浏览数（使用埋点数据）
        SELECT 
            shop_id,
            SUM(expose_count) AS expose,
            SUM(expose_count) AS browse
        FROM temp_exposure
        GROUP BY shop_id
    ) exposure ON orders.shop_id = exposure.shop_id
    LEFT JOIN (
        -- 商品访客数（使用埋点数据）
        SELECT 
            shop_id,
            COUNT(DISTINCT uuid) AS visitor
        FROM temp_exposure_users
        GROUP BY shop_id
    ) visitors ON orders.shop_id = visitors.shop_id
    LEFT JOIN (
        -- 分享访问次数（使用新埋点逻辑：isShare=1）
        SELECT 
            shop_id,
            COUNT(*) AS share_visit
        FROM temp_share_visit
        GROUP BY shop_id
    ) share_visits ON orders.shop_id = share_visits.shop_id
    
    UNION
    
    -- 只有新增商品但没有订单的商店
    SELECT 
        products_only.shop_id,
        '$targetDate' AS current_day,
        products_only.newProd AS new_prod,
        COALESCE(visited_only.visitedProd, 0) AS visited_prod,
        0 AS dynamic_sale,
        COALESCE(exposure_only.expose, 0) AS expose,
        COALESCE(exposure_only.browse, 0) AS browse,
        COALESCE(visitors_only.visitor, 0) AS visitor,
        0 AS add_cart,
        0 AS order_num,
        0 AS pay_num,
        COALESCE(share_visits_only.share_visit, 0) AS share_visit
        
    FROM (
        SELECT
            shop_id,
            COUNT(prod_id) AS newProd
        FROM mall_bbc.t_dwd_prod_full
        WHERE status != '-1'
          AND create_time >= '$startTime'
          AND create_time <= '$endTime'
        GROUP BY shop_id
    ) products_only
    LEFT JOIN (
        -- 被访问商品数（使用原逻辑：从曝光数据统计）
        SELECT 
            shop_id,
            COUNT(DISTINCT prod_id) AS visitedProd
        FROM temp_exposure
        GROUP BY shop_id
    ) visited_only ON products_only.shop_id = visited_only.shop_id
    LEFT JOIN (
        -- 商品曝光数和浏览数（使用埋点数据）
        SELECT 
            shop_id,
            SUM(expose_count) AS expose,
            SUM(expose_count) AS browse
        FROM temp_exposure
        GROUP BY shop_id
    ) exposure_only ON products_only.shop_id = exposure_only.shop_id
    LEFT JOIN (
        -- 商品访客数（使用埋点数据）
        SELECT 
            shop_id,
            COUNT(DISTINCT uuid) AS visitor
        FROM temp_exposure_users
        GROUP BY shop_id
    ) visitors_only ON products_only.shop_id = visitors_only.shop_id
    LEFT JOIN (
        -- 分享访问次数（使用新埋点逻辑：isShare=1）
        SELECT 
            shop_id,
            COUNT(*) AS share_visit
        FROM temp_share_visit
        GROUP BY shop_id
    ) share_visits_only ON products_only.shop_id = share_visits_only.shop_id
    WHERE products_only.shop_id NOT IN (
        SELECT DISTINCT o.shop_id
        FROM mall_bbc.t_dwd_order_full o
        WHERE o.create_time >= '$startTime'
          AND o.create_time <= '$endTime'
    )
    
    ORDER BY shop_id
    """
    
    val productOverviewDF = spark.sql(productOverviewSQL)
    val totalCount = productOverviewDF.count()
    
    if (totalCount > 0) {
      println(s"======= 商品概况离线分析查询结果 ($targetDate，共 $totalCount 条) =======")
      productOverviewDF.show(50, false)
      
      // 使用自定义方法写入MySQL（确保使用correct_day字段）
      writeToMySQL(productOverviewDF, "tz_bd_merchant_product_overview", targetDate)
      println(s"数据成功写入MySQL表: tz_bd_merchant_product_overview (日期: $targetDate)")
    } else {
      println(s"$targetDate 无商品概况数据")
    }
  }
  
  /**
   * 注册临时视图，用于分析离线埋点数据（指定日期数据）
   */
  private def registerExposureView(spark: SparkSession, targetDate: String): Unit = {
    try {
      // 切换到user_tag数据库以获取埋点数据
      spark.sql("USE user_tag")
      println(s"切换到数据库: user_tag (用于查询 $targetDate 埋点数据)")
      
      // 曝光视图 - 按商店ID分组，使用指定日期数据
      val exposureSQL = s"""
        SELECT 
          CAST(prodid AS BIGINT) AS prod_id,
          CAST(shopid AS BIGINT) AS shop_id,
          COUNT(*) AS expose_count,
          COUNT(DISTINCT cid) AS expose_person_num
        FROM user_tag.t_ods_app_logdata
        WHERE dt = '$targetDate'
          AND action = 'enter'
          AND page_id = '1005'
          AND prodid IS NOT NULL
          AND shopid IS NOT NULL
        GROUP BY CAST(prodid AS BIGINT), CAST(shopid AS BIGINT)
      """
      
      // 曝光用户视图 - 按商店ID分组，使用指定日期数据
      val exposureUsersSQL = s"""
        SELECT 
          cid AS uuid,
          CAST(prodid AS BIGINT) AS prod_id,
          CAST(shopid AS BIGINT) AS shop_id
        FROM user_tag.t_ods_app_logdata
        WHERE dt = '$targetDate'
          AND action = 'enter'
          AND page_id = '1005'
          AND prodid IS NOT NULL
          AND shopid IS NOT NULL
      """
      
      // 分享访问视图 - 使用新的埋点逻辑：page_id=1005 and event='AppViewScreen' and action='enter' and isShare=1
      val shareVisitSQL = s"""
        SELECT 
          CAST(prodid AS BIGINT) AS prod_id,
          CAST(shopid AS BIGINT) AS shop_id,
          cid AS uuid
        FROM user_tag.t_ods_app_logdata
        WHERE dt = '$targetDate'
          AND page_id = '1005'
          AND event = 'AppViewScreen'
          AND action = 'enter'
          AND get_json_object(label, '$$.isShare') = '1'
          AND prodid IS NOT NULL
          AND shopid IS NOT NULL
      """
      
      // 创建临时视图
      println(s"创建 $targetDate 曝光临时视图...")
      val exposureDF = spark.sql(exposureSQL)
      exposureDF.createOrReplaceTempView("temp_exposure")
      
      println(s"创建 $targetDate 曝光用户临时视图...")
      val exposureUsersDF = spark.sql(exposureUsersSQL)
      exposureUsersDF.createOrReplaceTempView("temp_exposure_users")
      
      println(s"创建 $targetDate 分享访问临时视图...")
      val shareVisitDF = spark.sql(shareVisitSQL)
      shareVisitDF.createOrReplaceTempView("temp_share_visit")
      
      // 切换回mall_bbc数据库处理订单相关数据
      spark.sql("USE mall_bbc")
      println(s"切换回数据库: mall_bbc (用于查询 $targetDate 订单数据)")
      
    } catch {
      case e: Exception => 
        println(s"创建临时视图时出错: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  /**
   * 写入MySQL方法（使用current_day字段）
   */
  private def writeToMySQL(df: DataFrame, tableName: String, statDate: String): Unit = {
    var connection: java.sql.Connection = null
    try {
      println(s"开始写入数据到表 $tableName")
      println(s"数据行数: ${df.count()}")
      
      // 先删除当天数据
      connection = Constants.DatabaseUtils.getWriteConnection
      val deleteSql = s"DELETE FROM `$tableName` WHERE current_day = '$statDate'"
      println(s"执行删除SQL: $deleteSql")
      val statement = connection.createStatement()
      val deletedRows = statement.executeUpdate(deleteSql)
      println(s"删除了 $deletedRows 行数据")
      statement.close()
      connection.close()

      // 插入新数据
      val props = new java.util.Properties()
      props.put("user", Constants.JdbcWriteStatistics.username)
      props.put("password", Constants.JdbcWriteStatistics.password)
      props.put("driver", Constants.JdbcWriteStatistics.driver)
      
      df.write
        .mode("append")
        .jdbc(Constants.JdbcWriteStatistics.jdbcUrl, tableName, props)
        
    } catch {
      case e: Exception =>
        println(s"写入错误: ${e.getMessage}")
        throw e
    } finally {
      if (connection != null) {
        try { connection.close() } catch { case _: Exception => }
      }
    }
  }
} 