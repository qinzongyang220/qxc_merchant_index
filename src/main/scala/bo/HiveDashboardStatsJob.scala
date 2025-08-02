package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * Hive仪表板统计作业
 * 从Hive表中读取订单支付数据并分析昨天退款统计
 */
object HiveDashboardStatsJob {

  /**
   * 写入DataFrame到MySQL表
   */
  def writeToMySQL(df: DataFrame, tableName: String, statDate: String): Unit = {
    try {
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, statDate, deleteBeforeInsert = true)
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
    implicit val jobName: String = "HiveDashboardStats"
    val spark: SparkSession = MyHive.conn

    try {
      println("成功连接到Hive")
      
      // 使用mall_bbc数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")
      
      // 获取昨天的日期
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      
      val cal = Calendar.getInstance()
      cal.add(Calendar.DAY_OF_MONTH, -1) // 减去1天，获取昨天的日期
      val yesterday = dateFormat.format(cal.getTime())
      
      cal.add(Calendar.DAY_OF_MONTH, -1) // 再减去1天，获取前天的日期
      val dayBeforeYesterday = dateFormat.format(cal.getTime())
      
      // 获取本月第一天
      cal.setTime(new Date())
      cal.set(Calendar.DAY_OF_MONTH, 1)
      val firstDayOfMonth = dateFormat.format(cal.getTime())
      
      println(s"处理日期: $yesterday (同比日期: $dayBeforeYesterday)")
      println(s"本月第一天: $firstDayOfMonth")

      // 允许笛卡尔积连接
      spark.sql("SET spark.sql.crossJoin.enabled=true")
      
      // 订单支付数据主查询 - 昨天的数据
      println("执行订单支付数据主查询...")
      try {
        val todayOrderPaymentDF = spark.sql(
          s"""
          SELECT 
              o.shop_id,
              -- 当日支付数据
              NVL(SUM(CAST(o.actual_total AS DECIMAL(18,2))), 0) AS pay_actual_total,
              COUNT(DISTINCT o.user_id) AS pay_user_count,
              COUNT(*) AS pay_order_count,
              -- 当日客单价
              CASE 
                  WHEN COUNT(DISTINCT o.user_id) > 0 
                  THEN NVL(SUM(CAST(o.actual_total AS DECIMAL(18,2))), 0) / COUNT(DISTINCT o.user_id)
                  ELSE 0 
              END AS one_price,
              
              -- 当日退款金额
              NVL(MAX(CAST(refund.refund_sum AS DECIMAL(18,2))), 0) AS refund,
              
              -- 昨日支付数据(同比)
              NVL(MAX(CAST(yesterday_pay.actual_total_sum AS DECIMAL(18,2))), 0) AS yesterday_pay_actual_total,
              NVL(MAX(CAST(yesterday_pay.user_count AS INT)), 0) AS yesterday_pay_user_count,
              NVL(MAX(CAST(yesterday_pay.order_count AS INT)), 0) AS yesterday_pay_order_count,
              
              -- 昨日客单价
              CASE 
                  WHEN NVL(MAX(CAST(yesterday_pay.user_count AS INT)), 0) > 0 
                  THEN NVL(MAX(CAST(yesterday_pay.actual_total_sum AS DECIMAL(18,2))), 0) / NVL(MAX(CAST(yesterday_pay.user_count AS INT)), 0)
                  ELSE 0 
              END AS yesterday_one_price,
              
              -- 昨日退款金额
              NVL(MAX(CAST(yesterday_refund.refund_sum AS DECIMAL(18,2))), 0) AS yesterday_refund,
              
              -- 统计日期
              TO_DATE('$yesterday') AS stat_date
          FROM 
              t_ods_tz_order o
          -- 当日退款数据
          LEFT JOIN (
              SELECT shop_id, SUM(CAST(refund_amount AS DECIMAL(18,2))) AS refund_sum
              FROM t_ods_tz_order_refund
              WHERE return_money_sts = 5
              AND refund_time LIKE '$yesterday%'
              GROUP BY shop_id
          ) refund ON o.shop_id = refund.shop_id
          -- 昨日支付数据
          LEFT JOIN (
              SELECT 
                  shop_id,
                  SUM(CAST(actual_total AS DECIMAL(18,2))) AS actual_total_sum,
                  COUNT(DISTINCT user_id) AS user_count,
                  COUNT(*) AS order_count
              FROM t_ods_tz_order
              WHERE is_payed = 'true'
              AND pay_time LIKE '$dayBeforeYesterday%'
              GROUP BY shop_id
          ) yesterday_pay ON o.shop_id = yesterday_pay.shop_id
          -- 昨日退款数据
          LEFT JOIN (
              SELECT shop_id, SUM(CAST(refund_amount AS DECIMAL(18,2))) AS refund_sum
              FROM t_ods_tz_order_refund
              WHERE return_money_sts = 5
              AND refund_time LIKE '$dayBeforeYesterday%'
              GROUP BY shop_id
          ) yesterday_refund ON o.shop_id = yesterday_refund.shop_id
          WHERE 
              o.is_payed = 'true'
              AND o.pay_time LIKE '$yesterday%'
          GROUP BY 
              o.shop_id
          ORDER BY 
              o.shop_id
          """)
        
        println("订单支付数据主查询结果:")
        todayOrderPaymentDF.show(false)
        
        // 添加详细调试信息
        val dataCount = todayOrderPaymentDF.count()
        println(s"准备写入数据行数: $dataCount")
        if (dataCount > 0) {
          println("数据sample：")
          todayOrderPaymentDF.show(5, false)
        } else {
          println("警告：没有数据要写入！")
        }
        
        // 写入MySQL
        println(s"开始写入表: tz_bd_merchant_daily_order_pay, 日期: $yesterday")
        writeToMySQL(todayOrderPaymentDF, "tz_bd_merchant_daily_order_pay", yesterday)
        println("写入tz_bd_merchant_daily_order_pay完成")
      } catch {
        case e: Exception => 
          println(s"订单支付数据主查询执行失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 订单支付统计查询 - 昨天的数据
      println("执行订单支付统计查询...")
      try {
        val orderPaymentDF = spark.sql(
          s"""
          SELECT 
              a.shop_id, 
              a.pay_order_count, 
              a.pay_actual_total,
              NVL(b.today_amount, 0) AS today_amount,
              NVL(c.month_amount, 0) AS month_amount,
              TO_DATE('$yesterday') AS stat_date
          FROM (
              SELECT shop_id, COUNT(*) AS pay_order_count, SUM(CAST(actual_total AS DECIMAL(18,2))) AS pay_actual_total
              FROM t_ods_tz_order
              WHERE is_payed = 'true' AND pay_time LIKE '$yesterday%'
              GROUP BY shop_id
              
              UNION ALL
              
              SELECT '' AS shop_id, 0 AS pay_order_count, 0 AS pay_actual_total
          ) a
          LEFT JOIN (
              SELECT shop_id, SUM(CAST(actual_total AS DECIMAL(18,2))) AS today_amount
              FROM t_ods_tz_order
              WHERE is_payed = 'true' AND pay_time LIKE '$yesterday%'
              GROUP BY shop_id
          ) b ON a.shop_id = b.shop_id
          LEFT JOIN (
              SELECT shop_id, SUM(CAST(actual_total AS DECIMAL(18,2))) AS month_amount
              FROM t_ods_tz_order
              WHERE is_payed = 'true' 
              AND pay_time BETWEEN '$firstDayOfMonth 00:00:00'
                             AND '$yesterday 23:59:59'
              GROUP BY shop_id
          ) c ON a.shop_id = c.shop_id
          ORDER BY a.shop_id
          """)
        
        println("订单支付统计查询结果:")
        orderPaymentDF.show(false)
        
        // 添加详细调试信息
        val orderPayCount = orderPaymentDF.count()
        println(s"订单支付统计准备写入数据行数: $orderPayCount")
        if (orderPayCount > 0) {
          println("订单支付统计数据sample：")
          orderPaymentDF.show(5, false)
        } else {
          println("警告：订单支付统计没有数据要写入！")
        }
        
        // 写入MySQL
        println(s"开始写入表: tz_bd_merchant_order_pay_agg, 日期: $yesterday")
        writeToMySQL(orderPaymentDF, "tz_bd_merchant_order_pay_agg", yesterday)
        println("写入tz_bd_merchant_order_pay_agg完成")
      } catch {
        case e: Exception => 
          println(s"订单支付统计查询执行失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 退款商品排行榜查询 - 昨天的数据
      println("执行退款商品排行榜查询...")
      try {
        val refundRankingDF = spark.sql(
          s"""
          SELECT 
              toi.prod_id, 
              p.shop_id, 
              NVL(SUM(NVL(CAST(tor.goods_num AS INT), CAST(toi.prod_count AS INT))),0) AS refund_count,
              toi.prod_name AS refund_prod_name, 
              p.pic,
              TO_DATE('$yesterday') AS stat_date
          FROM t_ods_tz_order_item toi
          LEFT JOIN t_ods_tz_order_refund tor ON toi.order_item_id = tor.order_item_id
          LEFT JOIN t_ods_tz_prod p ON toi.prod_id = p.prod_id
          WHERE (toi.order_item_id IN (
              SELECT order_item_id FROM t_ods_tz_order_refund
              WHERE return_money_sts = 5 AND refund_type = 2
              AND TO_DATE(refund_time) = '$yesterday'
          )
          OR toi.order_number IN (
              SELECT order_number FROM t_ods_tz_order WHERE order_id IN (
                  SELECT order_id FROM t_ods_tz_order_refund
                  WHERE return_money_sts = 5 AND refund_type = 1
                  AND TO_DATE(refund_time) = '$yesterday'
              )
          ))
          GROUP BY toi.prod_id, p.shop_id, toi.prod_name, p.pic
          ORDER BY refund_count DESC
          """)
        
        println("退款商品排行榜查询结果:")
        refundRankingDF.show(false)
        
        // 写入MySQL
        writeToMySQL(refundRankingDF, "tz_bd_merchant_refund_prod_rank", yesterday)
      } catch {
        case e: Exception => 
          println(s"退款商品排行榜查询执行失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 退款原因排行榜查询 - 昨天的数据
      println("执行退款原因排行榜查询...")
      try {
        val refundReasonRankingDF = spark.sql(
          s"""
          WITH refund_total AS (
            SELECT 
              shop_id, 
              SUM(CAST(refund_amount AS DECIMAL(18,2))) AS total_refund_amount
            FROM t_ods_tz_order_refund
            WHERE return_money_sts = 5
              AND TO_DATE(refund_time) = '$yesterday'
            GROUP BY shop_id
          ),
          
          refund_data AS (
            SELECT
              a.buyer_reason,
              b.prod_name,  -- 从订单表获取商品名称
              a.refund_amount,
              a.shop_id,
              a.order_item_id,
              rt.total_refund_amount
            FROM t_ods_tz_order_refund a
            JOIN t_ods_tz_order b ON a.order_id = b.order_id  -- 改为JOIN确保获取商品名称
            LEFT JOIN refund_total rt ON a.shop_id = rt.shop_id
            WHERE a.return_money_sts = 5
              AND TO_DATE(a.refund_time) = '$yesterday'
          ),
          
          product_pics AS (
            SELECT DISTINCT
              oi.order_item_id,
              p.pic
            FROM t_ods_tz_order_item oi
            JOIN t_ods_tz_prod p ON oi.prod_id = p.prod_id
          )
          
          SELECT
            rd.buyer_reason AS buyer_reason,
            MAX(rd.prod_name) AS refund_prod_name,
            COUNT(rd.buyer_reason) AS refund_count,
            SUM(rd.refund_amount) AS pay_actual_total,
            CASE 
              WHEN MAX(rd.total_refund_amount) = 0 THEN 0.0
              ELSE ROUND(SUM(rd.refund_amount) / MAX(rd.total_refund_amount) * 100, 1) 
            END AS percent_amount,
            MAX(pp.pic) AS pic,
            rd.shop_id,
            TO_DATE('$yesterday') AS stat_date
          FROM refund_data rd
          LEFT JOIN product_pics pp ON rd.order_item_id = pp.order_item_id
          GROUP BY rd.buyer_reason, rd.shop_id
          ORDER BY refund_count DESC, pay_actual_total DESC
          """)
        
        println("退款原因排行榜查询结果:")
        refundReasonRankingDF.show(false)
        
        // 写入MySQL
        writeToMySQL(refundReasonRankingDF, "tz_bd_merchant_refund_reason_rank", yesterday)
      } catch {
        case e: Exception => 
          println(s"退款原因排行榜查询执行失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 综合退款统计查询 - 昨天的数据
      println("执行综合退款统计查询...")
      try {
        val refundStatsDF = spark.sql(
          s"""
          WITH refund_stats AS (
              SELECT 
                  shop_id AS refund_shop_id,
                  SUM(CAST(refund_amount AS DECIMAL(18,2))) AS refund_amount,
                  COUNT(DISTINCT order_id) AS refund_count
              FROM t_ods_tz_order_refund
              WHERE return_money_sts = 5
              AND TO_DATE(refund_time) = '$yesterday'
              GROUP BY shop_id
          ),
          order_stats AS (
              SELECT 
                  shop_id AS order_shop_id,
                  COUNT(order_id) AS order_count
              FROM t_ods_tz_order
              WHERE status >= 2
              AND TO_DATE(pay_time) = '$yesterday'
              GROUP BY shop_id
          ),
          all_shops AS (
              SELECT refund_shop_id AS shop_id FROM refund_stats
              UNION
              SELECT order_shop_id AS shop_id FROM order_stats
          )
          
          SELECT 
              TO_DATE('$yesterday') AS refund_date, 
              '$yesterday' AS refund_date_to_string,
              COALESCE(s.shop_id, 0) AS shop_id,
              COALESCE(r.refund_amount, 0) AS pay_actual_total,
              COALESCE(r.refund_count, 0) AS refund_count,
              COALESCE(o.order_count, 0) AS pay_order_count,
              CASE 
                  WHEN COALESCE(r.refund_count, 0) > 0 AND COALESCE(o.order_count, 0) = 0 THEN 100.0000
                  WHEN COALESCE(o.order_count, 0) > 0 THEN COALESCE(r.refund_count, 0) / o.order_count * 100
                  ELSE 0 
              END AS refund_rate,
              TO_DATE('$yesterday') AS stat_date
          FROM 
              all_shops s
          LEFT JOIN 
              refund_stats r ON s.shop_id = r.refund_shop_id
          LEFT JOIN 
              order_stats o ON s.shop_id = o.order_shop_id
          ORDER BY 
              s.shop_id
          """)
        
        println("综合退款统计查询结果:")
        refundStatsDF.show(false)
        
        // 写入MySQL
        writeToMySQL(refundStatsDF, "tz_bd_merchant_refund_stats", yesterday)
      } catch {
        case e: Exception => 
          println(s"综合退款统计查询执行失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      println(s"昨天($yesterday)的数据处理完成，同比日期为($dayBeforeYesterday)")

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
} 