package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive实时商品概况分析作业
 * 从Hive增量表(inc)中读取今天的商品概况数据，按商店ID分组
 * 只读取数据，不写入MySQL
 */
object HiveRealtimeProductOverviewJob {

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "HiveRealtimeProductOverview"
    val spark: SparkSession = MyHive.conn

    try {
      // 设置Spark配置，增加显示字段数
      spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
      
      println("成功连接到Hive")
      
      // 使用mall_bbc和user_tag数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")
      
      // 获取今天的日期
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      val today = dateFormat.format(cal.getTime())
      
      println(s"分析日期: $today (实时)")
      println("使用inc增量表读取今天实时数据，按商店ID分组处理所有商店数据")
      
      // 临时注册埋点视图
      registerExposureView(spark, today)
      
      // 执行商品概况分析查询
      println("执行商品概况实时分析查询...")
      val productOverviewSQL = s"""
      -- 商品概况实时分析SQL，使用Hive增量表，参考ProductOverviewJob查询结构
      SELECT 
          orders.shop_id,
          orders.orderNum,
          orders.payNum,
          orders.dynamicSale,
          COALESCE(products.newProd, 0) AS newProd,
          -- 被访问商品数（使用埋点数据）
          COALESCE(visited.visitedProd, 0) AS visitedProd,
          -- 商品曝光数（使用埋点数据）
          COALESCE(exposure.expose, 0) AS expose,
          COALESCE(exposure.browse, 0) AS browse,
          -- 商品访客数（使用埋点数据）
          COALESCE(visitors.visitor, 0) AS visitor,
          '自然日' AS time_period,
          '$today' AS stat_date
          
      FROM (
          -- 订单相关统计（类似ProductOverviewJob的orders查询）
          SELECT
              o.shop_id,
              COALESCE(SUM(CAST(oi.prod_count AS INT)), 0) AS orderNum,
              COALESCE(SUM(CASE WHEN o.is_payed = '1' THEN CAST(oi.prod_count AS INT) ELSE 0 END), 0) AS payNum,
              COUNT(DISTINCT oi.prod_id) AS dynamicSale
          FROM mall_bbc.t_dwd_order_inc o
          LEFT JOIN mall_bbc.t_dwd_order_item_inc oi ON o.order_number = oi.order_number
          WHERE o.dt = '$today'
            AND oi.dt = '$today'
            AND o.create_time LIKE '$today%'
          GROUP BY o.shop_id
      ) orders
      LEFT JOIN (
          -- 新增商品统计（类似ProductOverviewJob的products查询）
          SELECT
              shop_id,
              COUNT(prod_id) AS newProd
          FROM mall_bbc.t_dwd_prod_inc
          WHERE dt = '$today'
            AND is_delete_hive = '0'
            AND status != '-1'
            AND create_time LIKE '$today%'
          GROUP BY shop_id
      ) products ON orders.shop_id = products.shop_id
      LEFT JOIN (
          -- 被访问商品数（使用埋点数据）
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
      
      UNION
      
      -- 只有新增商品但没有订单的商店
      SELECT 
          products_only.shop_id,
          0 AS orderNum,
          0 AS payNum, 
          0 AS dynamicSale,
          products_only.newProd,
          -- 被访问商品数（使用埋点数据）
          COALESCE(visited_only.visitedProd, 0) AS visitedProd,
          -- 商品曝光数（使用埋点数据）
          COALESCE(exposure_only.expose, 0) AS expose,
          COALESCE(exposure_only.browse, 0) AS browse,
          -- 商品访客数（使用埋点数据）
          COALESCE(visitors_only.visitor, 0) AS visitor,
          '自然日' AS time_period,
          '$today' AS stat_date
          
      FROM (
          SELECT
              shop_id,
              COUNT(prod_id) AS newProd
          FROM mall_bbc.t_dwd_prod_inc
          WHERE dt = '$today'
            AND is_delete_hive = '0'
            AND status != '-1'
            AND create_time LIKE '$today%'
          GROUP BY shop_id
      ) products_only
      LEFT JOIN (
          -- 被访问商品数（使用埋点数据）
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
      WHERE products_only.shop_id NOT IN (
          SELECT DISTINCT o.shop_id
          FROM mall_bbc.t_dwd_order_inc o
          WHERE o.dt = '$today'
            AND o.create_time LIKE '$today%'
      )
      
      ORDER BY shop_id
      """
      
      val productOverviewDF = spark.sql(productOverviewSQL)
      val totalCount = productOverviewDF.count()
      
      if (totalCount > 0) {
        println(s"======= 商品概况实时分析查询结果 (共 $totalCount 条) =======")
        productOverviewDF.show(Int.MaxValue, false) // 显示所有行，不截断
      } else {
        println("今天无商品概况数据")
      }
      
      // 写入MySQL
      writeToMySQL(productOverviewDF, "tz_bd_merchant_product_overview", today)
      
      println("数据查询和写入完成")

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
   * 注册临时视图，用于分析实时埋点数据（今天数据）
   */
  private def registerExposureView(spark: SparkSession, today: String): Unit = {
    try {
      // 切换到user_tag数据库以获取埋点数据
      spark.sql("USE user_tag")
      println("切换到数据库: user_tag (用于查询今日埋点数据)")
      
      // 曝光视图 - 按商店ID分组，使用今天数据
      val exposureSQL = s"""
        SELECT 
          CAST(prodid AS BIGINT) AS prod_id,
          CAST(shopid AS BIGINT) AS shop_id,
          COUNT(*) AS expose_count,
          COUNT(DISTINCT cid) AS expose_person_num
        FROM user_tag.t_ods_app_logdata
        WHERE dt = '$today'
          AND action = 'enter'
          AND page_id = '1005'
          AND prodid IS NOT NULL
          AND shopid IS NOT NULL
        GROUP BY CAST(prodid AS BIGINT), CAST(shopid AS BIGINT)
      """
      
      // 曝光用户视图 - 按商店ID分组，使用今天数据
      val exposureUsersSQL = s"""
        SELECT 
          cid AS uuid,
          CAST(prodid AS BIGINT) AS prod_id,
          CAST(shopid AS BIGINT) AS shop_id
        FROM user_tag.t_ods_app_logdata
        WHERE dt = '$today'
          AND action = 'enter'
          AND page_id = '1005'
          AND prodid IS NOT NULL
          AND shopid IS NOT NULL
      """
      
      // 创建临时视图
      println("创建今日曝光临时视图...")
      val exposureDF = spark.sql(exposureSQL)
      exposureDF.createOrReplaceTempView("temp_exposure")
      
      println("创建今日曝光用户临时视图...")
      val exposureUsersDF = spark.sql(exposureUsersSQL)
      exposureUsersDF.createOrReplaceTempView("temp_exposure_users")
      
      // 切换回mall_bbc数据库处理订单相关数据
      spark.sql("USE mall_bbc")
      println("切换回数据库: mall_bbc (用于查询今日订单数据)")
      
    } catch {
      case e: Exception => 
        println(s"创建临时视图时出错: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  /**
   * 写入MySQL数据
   */
  def writeToMySQL(df: DataFrame, tableName: String, statDate: String): Unit = {
    try {
      ensureTableExists(tableName)
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, statDate, deleteBeforeInsert = true)
      println(s"数据成功写入MySQL表: $tableName")
    } catch {
      case e: Exception =>
        println(s"写入MySQL表 $tableName 时出错: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }
  
  /**
   * 确保MySQL表存在
   */
  def ensureTableExists(tableName: String): Unit = {
    val connection = Constants.DatabaseUtils.getWriteConnection
    try {
      val statement = connection.createStatement()
      
      // 检查表是否存在
      val checkTableSQL = s"SHOW TABLES LIKE '$tableName'"
      val resultSet = statement.executeQuery(checkTableSQL)
      
      if (!resultSet.next()) {
        // 表不存在，创建表
        println(s"表 $tableName 不存在，正在创建...")
        val createTableSQL = generateCreateTableSQL(tableName)
        statement.execute(createTableSQL)
        println(s"表 $tableName 创建成功")
      }
      
      resultSet.close()
      statement.close()
    } finally {
      connection.close()
    }
  }
  
  /**
   * 生成创建表的SQL
   */
  def generateCreateTableSQL(tableName: String): String = {
    s"""
      CREATE TABLE IF NOT EXISTS `$tableName` (
        `shop_id` BIGINT NOT NULL COMMENT '商店ID',
        `orderNum` INT DEFAULT 0 COMMENT '下单件数',
        `payNum` INT DEFAULT 0 COMMENT '支付件数',
        `dynamicSale` INT DEFAULT 0 COMMENT '动销商品数',
        `newProd` INT DEFAULT 0 COMMENT '新增商品数',
        `visitedProd` INT DEFAULT 0 COMMENT '被访问商品数',
        `expose` BIGINT DEFAULT 0 COMMENT '商品曝光数',
        `browse` BIGINT DEFAULT 0 COMMENT '商品浏览数',
        `visitor` INT DEFAULT 0 COMMENT '商品访客数',
        `time_period` VARCHAR(255) DEFAULT NULL COMMENT '时间段选择(近7天,近30天，自然日，自然月)',
        `stat_date` DATE NOT NULL COMMENT '统计日期',
        PRIMARY KEY (`shop_id`, `stat_date`, `time_period`),
        KEY `idx_stat_date` (`stat_date`),
        KEY `idx_time_period` (`time_period`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商家商品概况分析表'
    """
  }
} 