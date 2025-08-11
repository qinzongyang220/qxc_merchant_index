package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive商品销售分析作业
 * 从Hive表中读取商品销售排行和访问转化率数据，按商店ID分组
 */
object HiveProductSalesAnalysisJob {

  /**
   * 写入DataFrame到MySQL表，支持基于时间周期的删除条件
   */
  def writeToMySQLWithTimePeriod(df: DataFrame, tableName: String, statDate: String, timePeriod: String): Unit = {
    try {
      val connection = Constants.DatabaseUtils.getWriteConnection
      val stmt = connection.createStatement()
      
      // 根据时间周期使用不同的删除条件
      val deleteSQL = timePeriod match {
        case "7" =>
          // 7天数据：只保留最新一份，删除所有7天数据
          s"DELETE FROM `$tableName` WHERE time_period = '7'"
        case "30" =>
          // 30天数据：只保留最新一份，删除所有30天数据
          s"DELETE FROM `$tableName` WHERE time_period = '30'"
        case monthId if monthId.matches("\\d{6}") =>
          // 自然月（YYYYMM格式）：删除当前月份的数据，保留其他月份
          s"DELETE FROM `$tableName` WHERE time_period = '$timePeriod'"
        case _ =>
          // 其他情况：只删除相同stat_date的数据
          s"DELETE FROM `$tableName` WHERE stat_date = '$statDate'"
      }
      
      println(s"执行删除SQL: $deleteSQL")
      val deletedRows = stmt.executeUpdate(deleteSQL)
      println(s"从 $tableName 表中删除了 $deletedRows 行数据 (日期: $statDate, 时间周期: $timePeriod)")
      
      stmt.close()
      connection.close()
      
      // 写入新数据
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, statDate, deleteBeforeInsert = false)
    } catch {
      case e: Exception =>
        println(s"写入MySQL表 $tableName 时出错: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "HiveProductSalesAnalysis"
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
      
      println("开始执行Hive商品销售分析...")
      println(s"时间范围: ${timeRanges.mkString(", ")}")
      
      // 对每个时间范围执行查询
      for (timeRange <- timeRanges) {
        println(s"\n处理时间范围: $timeRange")
        processTimeRange(spark, timeRange)
      }
      
      println("\n所有时间范围数据查询完成")

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
      
      // 执行商品销售排行查询
      println(s"执行 $timeRangeType 商品销售排行查询...")
      val salesRankingDF = executeSalesRankingQuery(spark, startTime, endTime, dateStr, timePeriod)
      
      if (salesRankingDF.count() > 0) {
        println(s"======= $timeRangeType 商品销售排行数据 =======")
        salesRankingDF.show(50, false)
        
        // 写入MySQL
        writeToMySQLWithTimePeriod(salesRankingDF, "tz_bd_merchant_product_sales_rank", dateStr, timePeriod)
      } else {
        println(s"$timeRangeType 无销售排行数据")
      }
      
      // 执行商品访问转化率查询
      println(s"执行 $timeRangeType 商品访问转化率查询...")
      val visitorConversionDF = executeVisitorConversionQuery(spark, startTime, endTime, dateStr, timePeriod)
      
      if (visitorConversionDF.count() > 0) {
        println(s"======= $timeRangeType 商品访问转化率数据 =======")
        visitorConversionDF.show(50, false)
        
        // 写入MySQL
        writeToMySQLWithTimePeriod(visitorConversionDF, "tz_bd_merchant_product_visitor_conversion", dateStr, timePeriod)
      } else {
        println(s"$timeRangeType 无访问转化率数据")
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
   * 执行商品销售排行查询
   */
  def executeSalesRankingQuery(spark: SparkSession, startTime: String, endTime: String, dateStr: String, timePeriod: String): DataFrame = {
    val productSalesRankingSQL = s"""
    -- 商品销售排行SQL
    -- 此查询用于获取商品销售排行数据，按商店ID分组
    WITH sales_data AS (
      SELECT 
          oi.prod_id, 
          p.shop_id,
          sd.shop_name AS shop_name, 
          p.pic, 
          p.prod_name,
          SUM(CAST(oi.actual_total AS DECIMAL(18,2))) AS pay_amount, 
          SUM(CAST(oi.prod_count AS INT)) AS pay_count,
          ROW_NUMBER() OVER (PARTITION BY p.shop_id ORDER BY SUM(CAST(oi.actual_total AS DECIMAL(18,2))) DESC) AS rank_num
      FROM mall_bbc.t_ods_tz_order_item oi
      LEFT JOIN mall_bbc.t_ods_tz_order o ON o.order_number = oi.order_number
      LEFT JOIN mall_bbc.t_ods_tz_prod p ON p.prod_id = oi.prod_id
      LEFT JOIN (
        SELECT DISTINCT shop_id, first_value(shop_name) OVER (PARTITION BY shop_id ORDER BY shop_name) AS shop_name
        FROM mall_bbc.t_ods_tz_shop_detail
      ) sd ON sd.shop_id = p.shop_id
      WHERE o.is_payed = 'true' 
        AND p.status != '-1'
        AND o.pay_time >= '$startTime' 
        AND o.pay_time <= '$endTime'
      GROUP BY oi.prod_id, p.shop_id, sd.shop_name, p.prod_name, p.pic
    )
    
    SELECT 
        prod_id, 
        shop_id,
        shop_name, 
        pic, 
        prod_name, 
        pay_amount, 
        pay_count,
        '$timePeriod' AS time_period,
        '$dateStr' AS stat_date
    FROM sales_data
    WHERE shop_id IS NOT NULL AND shop_id != ''
    ORDER BY shop_id, pay_amount DESC
    """
    
    val salesRankingDF = spark.sql(productSalesRankingSQL)
    
    // 去重处理，避免唯一索引冲突
    val dedupSalesDF = salesRankingDF.dropDuplicates(Seq("prod_id", "shop_id", "stat_date"))
    val beforeSalesCount = salesRankingDF.count()
    val afterSalesCount = dedupSalesDF.count()
    println(s"销售排行数据去重: $beforeSalesCount -> $afterSalesCount 条记录")
    
    dedupSalesDF
  }
  
  /**
   * 执行商品访问转化率查询
   */
  def executeVisitorConversionQuery(spark: SparkSession, startTime: String, endTime: String, dateStr: String, timePeriod: String): DataFrame = {
    val productVisitorConversionSQL = s"""
    -- 商品访问转化率SQL
    -- 此查询用于获取商品访问转化率数据，按商店ID分组
    WITH visitor_data AS (
      SELECT 
        prod_id,
        shop_id,
        MAX(expose_person_num) AS visitor_num
      FROM temp_exposure
      GROUP BY prod_id, shop_id
    ),
    pay_user_data AS (
      SELECT 
        oi.prod_id,
        o.shop_id,
        COUNT(DISTINCT oi.user_id) AS pay_user_count
      FROM mall_bbc.t_ods_tz_order_item oi
      LEFT JOIN mall_bbc.t_ods_tz_order o ON o.order_number = oi.order_number
      WHERE oi.rec_time >= '$startTime' 
        AND oi.rec_time <= '$endTime'
      GROUP BY oi.prod_id, o.shop_id
    ),
    conversion_data AS (
      SELECT 
        p.prod_id, 
        p.shop_id,
        sd.shop_name AS shop_name,
        p.prod_name,
        p.pic,
        COALESCE(v.visitor_num, 0) AS visitor_num,
        COALESCE(pu.pay_user_count, 0) AS pay_user_count,
        CASE 
          WHEN COALESCE(v.visitor_num, 0) > 0 
          THEN ROUND(COALESCE(pu.pay_user_count, 0) / COALESCE(v.visitor_num, 0), 4)
          ELSE 0
        END AS visitor_to_pay_rate,
        ROW_NUMBER() OVER (PARTITION BY p.shop_id ORDER BY COALESCE(v.visitor_num, 0) DESC) AS rank_num
      FROM mall_bbc.t_ods_tz_prod p
      LEFT JOIN visitor_data v ON p.prod_id = v.prod_id AND p.shop_id = v.shop_id
      LEFT JOIN pay_user_data pu ON p.prod_id = pu.prod_id AND p.shop_id = pu.shop_id
      LEFT JOIN mall_bbc.t_ods_tz_shop_detail sd ON p.shop_id = sd.shop_id
      WHERE p.prod_name IS NOT NULL
        AND p.status != '-1'
        AND p.shop_id IS NOT NULL
        AND p.shop_id != ''
        AND (COALESCE(v.visitor_num, 0) > 0 OR COALESCE(pu.pay_user_count, 0) > 0)
      GROUP BY p.prod_id, p.shop_id, sd.shop_name, p.prod_name, p.pic, COALESCE(v.visitor_num, 0), COALESCE(pu.pay_user_count, 0)
    )
    
    SELECT 
        prod_id, 
        shop_id,
        shop_name, 
        prod_name, 
        pic, 
        visitor_num, 
        visitor_to_pay_rate,
        pay_user_count,
        '$timePeriod' AS time_period,
        '$dateStr' AS stat_date
    FROM conversion_data
    ORDER BY shop_id, visitor_num DESC
    """
    val visitorConversionDF = spark.sql(productVisitorConversionSQL)
    
    // 去重处理，避免唯一索引冲突
    val dedupVisitorDF = visitorConversionDF.dropDuplicates(Seq("prod_id", "shop_id", "stat_date"))
    val beforeCount = visitorConversionDF.count()
    val afterCount = dedupVisitorDF.count()
    println(s"访问转化率数据去重: $beforeCount -> $afterCount 条记录")
    
    dedupVisitorDF
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
          AND action = 'enter'
          AND page_id = '1005'
          AND prodid IS NOT NULL
          AND shopid IS NOT NULL
        GROUP BY CAST(prodid AS BIGINT), CAST(shopid AS BIGINT)
      """
      
      // 创建临时视图
      println("创建商品曝光临时视图...")
      val exposureDF = spark.sql(exposureSQL)
      exposureDF.createOrReplaceTempView("temp_exposure")
      
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