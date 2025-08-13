package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive综合指标统计作业
 * 基于FormItemEnum枚举类定义，统计所有核心业务指标
 * 支持自然日、自然周、自然月三种时间范围统计
 * 包含下单、支付、退款等全链路数据指标
 */
object HiveComprehensiveStatsJob {

  /**
   * 写入DataFrame到MySQL表，根据时间范围类型实现不同的保留策略
   * - 自然日(0)：永久保留，只删除相同日期的重复数据
   * - 自然周(YYYYWW)：一周保留一条，删除同一周的所有旧数据
   * - 自然月(YYYYMM)：一个月保留一条，删除同一月的所有旧数据
   */
  def writeToMySQLWithPermanentKeep(df: DataFrame, tableName: String, statDate: String, timePeriod: String, time_type: Int): Unit = {
    try {
      val connection = Constants.DatabaseUtils.getWriteConnection
      val stmt = connection.createStatement()
      
      val deleteSQL = time_type match {
        case 1 =>
          // 自然日(time_type=1)：只删除相同统计日期的数据
          s"DELETE FROM `$tableName` WHERE stat_date = STR_TO_DATE('$statDate', '%Y-%m-%d') AND time_type = 1"
          
        case 2 =>
          // 自然周(time_type=2)：删除同一周的所有数据
          s"DELETE FROM `$tableName` WHERE date_time = '$timePeriod' AND time_type = 2"
          
        case 3 =>
          // 自然月(time_type=3)：删除同一月的所有数据
          s"DELETE FROM `$tableName` WHERE date_time = '$timePeriod' AND time_type = 3"
          
        case _ =>
          // 默认策略：删除相同统计日期、时间范围和类型的数据
          s"DELETE FROM `$tableName` WHERE stat_date = STR_TO_DATE('$statDate', '%Y-%m-%d') AND date_time = '$timePeriod' AND time_type = $time_type"
      }
      
      val deletedRows = stmt.executeUpdate(deleteSQL)
      
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

  /**
   * 计算时间范围和时间标识
   * 返回: (startTime, endTime, statDate, timePeriod, time_type)
   */
  def calculateTimeRange(timeRangeType: String): (String, String, String, String, Int) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    val today = dateFormat.format(cal.getTime()) // 运行时间（今天）
    
    timeRangeType match {
      case "day" =>
        // 自然日：只统计今天的数据，标识为YYYYMMDD格式，time_type=1
        val startTime = s"$today 00:00:00"
        val endTime = s"$today 23:59:59"
        
        // 计算YYYYMMDD格式的日期标识
        val dayPeriod = today.replace("-", "") // 2025-08-09 -> 20250809
        
        (startTime, endTime, today, dayPeriod, 1)
        
      case "week" =>
        // 自然周：从本周一到今天，标识为YYYYWW格式，time_type=2
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
        val weekStart = dateFormat.format(cal.getTime())
        val startTime = s"$weekStart 00:00:00"
        val endTime = s"$today 23:59:59"
        
        // 计算当前是今年第几周（YYYYWW格式）
        val year = cal.get(Calendar.YEAR)
        val weekOfYear = cal.get(Calendar.WEEK_OF_YEAR)
        val weekPeriod = f"$year%04d$weekOfYear%02d"
        
        (startTime, endTime, today, weekPeriod, 2)
        
      case "month" =>
        // 自然月：从本月1号到今天，标识为YYYYMM格式，time_type=3
        cal.setTime(dateFormat.parse(today))
        cal.set(Calendar.DAY_OF_MONTH, 1)
        val monthStart = dateFormat.format(cal.getTime())
        val startTime = s"$monthStart 00:00:00"
        val endTime = s"$today 23:59:59"
        
        // 计算当前年月（YYYYMM格式）
        val year = cal.get(Calendar.YEAR)
        val month = cal.get(Calendar.MONTH) + 1 // Calendar.MONTH是0-based
        val monthPeriod = f"$year%04d$month%02d"
        
        (startTime, endTime, today, monthPeriod, 3)
        
      case _ =>
        throw new IllegalArgumentException(s"不支持的时间范围类型: $timeRangeType")
    }
  }
  
  /**
   * 处理指定时间范围的数据
   */
  def processTimeRange(spark: SparkSession, timeRangeType: String): Unit = {
    try {
      // 计算时间范围
      val (startTime, endTime, statDate, timePeriod, time_type) = calculateTimeRange(timeRangeType)
      
      println(s"处理 $timeRangeType: $startTime ~ $endTime")
      
      // 执行综合指标查询
      val comprehensiveStatsDF = executeComprehensiveStatsQuery(spark, startTime, endTime, statDate, timePeriod, time_type)
      
      if (comprehensiveStatsDF.count() > 0) {
        comprehensiveStatsDF.show(5, false)
        writeToMySQLWithPermanentKeep(comprehensiveStatsDF, "tz_bd_merchant_comprehensive_stats", statDate, timePeriod, time_type)
        println(s"✓ $timeRangeType 完成")
      } else {
        println(s"✗ $timeRangeType 无数据")
      }
      
    } catch {
      case e: Exception => 
        println(s"处理时间范围 $timeRangeType 时出错: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  /**
   * 执行综合指标统计查询 - 统一使用inc表，所有时间范围都只读增量表
   */
  def executeComprehensiveStatsQuery(spark: SparkSession, startTime: String, endTime: String, statDate: String, timePeriod: String, time_type: Int): DataFrame = {
    
    val sql = s"""
      WITH 
      -- 下单相关指标统计（基于创建时间）
      order_stats AS (
        SELECT 
          shop_id,
          SUM(CAST(actual_total AS DECIMAL(18,2))) AS order_amount,
          COUNT(DISTINCT user_id) AS user_nums,
          COUNT(*) AS order_nums
        FROM t_dwd_order_inc
        WHERE create_time >= '$startTime'
          AND create_time <= '$endTime'
          AND shop_id IS NOT NULL
          AND shop_id != ''
        GROUP BY shop_id
      ),
      
      -- 下单商品数统计（基于创建时间）
      order_product_stats AS (
        SELECT 
          o.shop_id,
          SUM(CAST(oi.prod_count AS INT)) AS product_nums
        FROM t_dwd_order_inc o
        LEFT JOIN t_dwd_order_item_inc oi ON o.order_number = oi.order_number
        WHERE o.create_time >= '$startTime'
          AND o.create_time <= '$endTime'
          AND o.shop_id IS NOT NULL
          AND o.shop_id != ''
        GROUP BY o.shop_id
      ),
      
      -- 支付相关指标统计（基于创建时间，但只统计已支付订单）
      pay_stats AS (
        SELECT 
          shop_id,
          SUM(CAST(actual_total AS DECIMAL(18,2))) AS pay_amount,
          COUNT(DISTINCT user_id) AS pay_user_nums,
          COUNT(*) AS pay_order_nums
        FROM t_dwd_order_inc
        WHERE create_time >= '$startTime'
          AND create_time <= '$endTime'
          AND is_payed = '1'
          AND shop_id IS NOT NULL
          AND shop_id != ''
        GROUP BY shop_id
      ),
      
      -- 支付商品数统计（基于创建时间，但只统计已支付订单）
      pay_product_stats AS (
        SELECT 
          o.shop_id,
          SUM(CAST(oi.prod_count AS INT)) AS pay_product_nums
        FROM t_dwd_order_inc o
        LEFT JOIN t_dwd_order_item_inc oi ON o.order_number = oi.order_number
        WHERE o.create_time >= '$startTime'
          AND o.create_time <= '$endTime'
          AND o.is_payed = '1'
          AND o.shop_id IS NOT NULL
          AND o.shop_id != ''
        GROUP BY o.shop_id
      ),
      
      -- 退款相关指标统计（基于退款时间）
      refund_stats AS (
        SELECT 
          shop_id,
          SUM(CAST(refund_amount AS DECIMAL(18,2))) AS refund_amount,
          COUNT(DISTINCT order_id) AS refund_order_nums
        FROM t_dwd_order_refund_inc
        WHERE refund_time >= '$startTime'
          AND refund_time <= '$endTime'
          AND return_money_sts = 5
          AND shop_id IS NOT NULL
          AND shop_id != ''
        GROUP BY shop_id
      ),
      
      -- 所有商店ID列表
      all_shops AS (
        SELECT shop_id FROM order_stats
        UNION
        SELECT shop_id FROM pay_stats
        UNION  
        SELECT shop_id FROM refund_stats
      )
      
      -- 最终结果查询：合并所有指标
      SELECT 
        s.shop_id,
        
        -- 1. 客单价：支付金额 ÷ 支付人数
        CASE 
          WHEN COALESCE(ps.pay_user_nums, 0) > 0 
          THEN ROUND(COALESCE(ps.pay_amount, 0) / ps.pay_user_nums, 2)
          ELSE 0 
        END AS customer_unit_price,
        
        -- 2. 总成交金额：统计所有已支付订单的总金额
        COALESCE(ps.pay_amount, 0) AS total_transaction_amount,
        
        -- 8. 下单金额：所有创建订单的总金额，包含未支付订单
        COALESCE(os.order_amount, 0) AS order_amount,
        
        -- 9. 下单人数：创建过订单的独立用户数
        COALESCE(os.user_nums, 0) AS user_nums,
        
        -- 10. 下单笔数：创建的订单总数量
        COALESCE(os.order_nums, 0) AS order_nums,
        
        -- 11. 下单商品数：所有订单中商品的总件数
        COALESCE(ops.product_nums, 0) AS product_nums,
        
        -- 12. 支付金额：实际完成支付的订单总金额
        COALESCE(ps.pay_amount, 0) AS pay_amount,
        
        -- 13. 支付人数：完成支付的独立用户数
        COALESCE(ps.pay_user_nums, 0) AS pay_user_nums,
        
        -- 14. 支付订单数：完成支付的订单总数
        COALESCE(ps.pay_order_nums, 0) AS pay_order_nums,
        
        -- 15. 支付商品数：已支付订单中的商品总件数
        COALESCE(pps.pay_product_nums, 0) AS pay_product_nums,
        
        -- 16. 成功退款金额：已完成退款的总金额
        COALESCE(rs.refund_amount, 0) AS refund_amount,
        
        -- 17. 退款订单数量：发生退款的订单总数
        COALESCE(rs.refund_order_nums, 0) AS refund_order_nums,
        
        -- 18. 下单-支付转化率：支付订单数量 ÷ 下单订单数量 × 100%
        CASE 
          WHEN COALESCE(os.order_nums, 0) > 0 
          THEN ROUND(COALESCE(ps.pay_order_nums, 0) * 100.0 / os.order_nums, 2)
          ELSE 0 
        END AS order_to_pay_rate,
        
        -- 19. 退款率：退款订单数量 ÷ 支付订单数量 × 100%
        CASE 
          WHEN COALESCE(ps.pay_order_nums, 0) > 0 
          THEN ROUND(COALESCE(rs.refund_order_nums, 0) * 100.0 / ps.pay_order_nums, 2)
          ELSE 0 
        END AS refund_rate,
        
        -- 统计日期和时间范围
        TO_DATE('$statDate') AS stat_date,
        '$timePeriod' AS date_time,
        $time_type AS time_type
        
      FROM all_shops s
      LEFT JOIN order_stats os ON s.shop_id = os.shop_id
      LEFT JOIN order_product_stats ops ON s.shop_id = ops.shop_id
      LEFT JOIN pay_stats ps ON s.shop_id = ps.shop_id
      LEFT JOIN pay_product_stats pps ON s.shop_id = pps.shop_id
      LEFT JOIN refund_stats rs ON s.shop_id = rs.shop_id
      WHERE s.shop_id IS NOT NULL 
        AND s.shop_id != ''
      ORDER BY s.shop_id
    """
    
    val comprehensiveStatsDF = spark.sql(sql)
      
    comprehensiveStatsDF
  }

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "HiveComprehensiveStats"
    val spark: SparkSession = MyHive.conn

    try {
      spark.sql("USE mall_bbc")
      spark.sql("SET spark.sql.crossJoin.enabled=true")
      
      // 处理三种时间范围：自然日、自然周、自然月
      val timeRangeTypes = Array("day", "week", "month")
      
      timeRangeTypes.foreach { timeRangeType =>
        processTimeRange(spark, timeRangeType)
      }
      
      println("✓ 综合指标数据处理完成")

    } catch {
      case e: Exception => 
        println(s"错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
