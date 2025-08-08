package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Calendar
/**
 * Hive商品概况分析作业
 * 从Hive表中读取商品概况数据，支持7天、30天、自然月多时间范围，按商店ID分组
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
      
      // 定义时间范围类型
      val timeRanges = List("7days", "30days", "thisMonth")
      
      println("开始执行Hive商品概况分析...")
      println(s"时间范围: ${timeRanges.mkString(", ")}")
      
      // 对每个时间范围执行查询
      for (timeRange <- timeRanges) {
        println(s"\n处理时间范围: $timeRange")
        processTimeRange(spark, timeRange)
      }
      
      println("\n所有时间范围数据查询完成")
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
   * 处理指定时间范围的数据
   */
  def processTimeRange(spark: SparkSession, timeRangeType: String): Unit = {
    try {
      // 计算时间范围
      val (startTime, endTime, dateStr, timePeriod) = calculateTimeRange(timeRangeType)
      
      println(s"分析时间范围: $startTime 至 $endTime")
      
      // 临时注册埋点视图
      registerExposureView(spark, startTime.split(" ")(0), endTime.split(" ")(0))
      
      // 执行商品概况分析查询
      println("执行商品概况分析查询...")
      val productOverviewSQL = generateProductOverviewSQL(startTime, endTime, timePeriod, dateStr)
      
      val productOverviewDF = spark.sql(productOverviewSQL)
      val totalCount = productOverviewDF.count()
      
      if (totalCount > 0) {
        println(s"======= $timeRangeType 商品概况分析查询结果 (共 $totalCount 条) =======")
        productOverviewDF.show(Int.MaxValue, false) // 显示所有行，不截断
        
        // 写入MySQL
        Constants.DatabaseUtils.writeDataFrameToMySQL(productOverviewDF, "tz_bd_merchant_product_overview", dateStr, deleteBeforeInsert = true)
        println("数据成功写入MySQL表: tz_bd_merchant_product_overview")
        
      } else {
        println(s"$timeRangeType 无商品概况数据")
      }
      
    } catch {
      case e: Exception => 
        println(s"处理时间范围 $timeRangeType 时出错: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  /**
   * 计算时间范围
   */
  def calculateTimeRange(timeRangeType: String): (String, String, String, String) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    val today = dateFormat.format(cal.getTime()) // 运行时间（今天）
    
    timeRangeType match {
      case "7days" =>
        // 近7天：过去7天，以昨天为终止日期，但stat_date使用运行时间
        cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
        val endDate = dateFormat.format(cal.getTime())
        cal.add(Calendar.DAY_OF_MONTH, -6) // 再往前6天，总共7天
        val startDate = dateFormat.format(cal.getTime())
        val startTime = s"$startDate 00:00:00"
        val endTime = s"$endDate 23:59:59"
        (startTime, endTime, today, "7") // 使用运行时间作为stat_date
        
      case "30days" =>
        // 近30天：过去30天，以昨天为终止日期，但stat_date使用运行时间
        cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
        val endDate = dateFormat.format(cal.getTime())
        cal.add(Calendar.DAY_OF_MONTH, -29) // 再往前29天，总共30天
        val startDate = dateFormat.format(cal.getTime())
        val startTime = s"$startDate 00:00:00"
        val endTime = s"$endDate 23:59:59"
        (startTime, endTime, today, "30") // 使用运行时间作为stat_date
        
      case "thisMonth" =>
        // 自然月：如果今天是1号，分析上个月完整数据；否则分析本月1号到昨天的数据
        val todayDate = cal.getTime()
        val todayDay = cal.get(Calendar.DAY_OF_MONTH)
        
        if (todayDay == 1) {
          // 月初第一天：分析上个月完整数据
          cal.add(Calendar.MONTH, -1) // 回到上个月
          val prevMonth = cal.getTime()
          
          // 上个月1号
          cal.set(Calendar.DAY_OF_MONTH, 1)
          val prevMonthStart = dateFormat.format(cal.getTime())
          
          // 上个月最后一天
          cal.setTime(prevMonth)
          cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH))
          val prevMonthEnd = dateFormat.format(cal.getTime())
          
          // 生成上个月的标识符 (YYYYMM)
          val year = cal.get(Calendar.YEAR)
          val month = f"${cal.get(Calendar.MONTH) + 1}%02d"
          val monthId = s"$year$month"
          
          val startTime = s"$prevMonthStart 00:00:00"
          val endTime = s"$prevMonthEnd 23:59:59"
          
          (startTime, endTime, prevMonthEnd, monthId)
        } else {
          // 月中其他日期：分析本月1号到昨天的数据
          cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
          val yesterday = dateFormat.format(cal.getTime())
          
          cal.setTime(todayDate) // 恢复到今天
          cal.set(Calendar.DAY_OF_MONTH, 1) // 设置到本月1号
          val monthStart = dateFormat.format(cal.getTime())
          
          // 生成当前月份的标识符 (YYYYMM)
          val year = cal.get(Calendar.YEAR)
          val month = f"${cal.get(Calendar.MONTH) + 1}%02d"
          val monthId = s"$year$month"
          
          val startTime = s"$monthStart 00:00:00"
          val endTime = s"$yesterday 23:59:59"
          
          (startTime, endTime, yesterday, monthId)
        }
        
      case _ =>
        throw new IllegalArgumentException(s"不支持的时间范围类型: $timeRangeType")
    }
  }
  
  /**
   * 生成商品概况分析SQL
   */
  def generateProductOverviewSQL(startTime: String, endTime: String, timePeriod: String, dateStr: String): String = {
    s"""
    -- 商品概况分析SQL ($timePeriod)，按商店ID分组
    WITH 
    -- 基础商品信息（新增商品）
    new_products AS (
        SELECT 
            shop_id,
            COUNT(prod_id) AS newProd
        FROM mall_bbc.t_ods_tz_prod
        WHERE status != '-1' 
            AND create_time BETWEEN '$startTime' AND '$endTime'
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
        WHERE o.create_time >= '$startTime'
            AND o.create_time <= '$endTime'
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
        WHERE o.create_time >= '$startTime'
            AND o.create_time <= '$endTime'
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
        WHERE o.create_time >= '$startTime'
            AND o.create_time <= '$endTime'
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
        COALESCE(oi.orderNum, 0) AS orderNum,
        COALESCE(pi.payNum, 0) AS payNum,
        COALESCE(ds.dynamicSale, 0) AS dynamicSale,
        COALESCE(np.newProd, 0) AS newProd,
        COALESCE(vp.visitedProd, 0) AS visitedProd,
        COALESCE(pe.expose, 0) AS expose,
        COALESCE(pe.browse, 0) AS browse,
        COALESCE(pv.visitor, 0) AS visitor,
        '$timePeriod' AS time_period,
        '$dateStr' AS stat_date
    FROM 
        all_shops s
    LEFT JOIN new_products np ON s.shop_id = np.shop_id
    LEFT JOIN visited_products vp ON s.shop_id = vp.shop_id
    LEFT JOIN dynamic_sales ds ON s.shop_id = ds.shop_id
    LEFT JOIN product_exposure pe ON s.shop_id = pe.shop_id
    LEFT JOIN product_visitors pv ON s.shop_id = pv.shop_id
    LEFT JOIN order_items oi ON s.shop_id = oi.shop_id
    LEFT JOIN payment_items pi ON s.shop_id = pi.shop_id
    WHERE s.shop_id IS NOT NULL 
      AND s.shop_id != ''
    ORDER BY s.shop_id
    """
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