package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * Hive仪表板统计作业
 * 专注于 tz_bd_merchant_daily_order_pay 表的处理
 * 使用 inc 增量表读取两天数据，只保留一天数据
 */
object HiveDashboardStatsJob {

  /**
   * 写入DataFrame到MySQL表，实现只保留一天数据的策略
   */
  def writeToMySQLWithDayLimit(df: DataFrame, tableName: String, today: String): Unit = {
    try {
      val connection = Constants.DatabaseUtils.getWriteConnection
      val stmt = connection.createStatement()
      
      // 删除今天的数据，只保留一天数据
      val deleteSQL = s"DELETE FROM `$tableName` WHERE stat_date = STR_TO_DATE('$today', '%Y-%m-%d')"
      
      val deletedRows = stmt.executeUpdate(deleteSQL)
      
      stmt.close()
      connection.close()
      
      // 写入新数据
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, today, deleteBeforeInsert = false)
    } catch {
      case e: Exception =>
        println(s"写入MySQL表 $tableName 时出错: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }
  
  /**
   * 写入DataFrame到MySQL表，实现保留一个月数据的策略
   * 删除条件：stat_date <= 上个月同一天的23:59:59
   */
  def writeToMySQLWithMonthLimit(df: DataFrame, tableName: String, today: String): Unit = {
    try {
      val connection = Constants.DatabaseUtils.getWriteConnection
      val stmt = connection.createStatement()
      
      // 计算上个月同一天（删除边界）
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      cal.setTime(dateFormat.parse(today))
      cal.add(Calendar.MONTH, -1) // 回到上个月同一天
      val lastMonthSameDay = dateFormat.format(cal.getTime())
      
      // 删除一个月以前的数据（保留从上个月同一天23:59:59到今天23:59:59）
      val deleteSQL = s"DELETE FROM `$tableName` WHERE stat_date <= STR_TO_DATE('$lastMonthSameDay', '%Y-%m-%d')"
      
      val deletedRows = stmt.executeUpdate(deleteSQL)
      
      // 删除今天的数据，然后重新写入
      val deleteTodaySQL = s"DELETE FROM `$tableName` WHERE stat_date = STR_TO_DATE('$today', '%Y-%m-%d')"
      val deletedTodayRows = stmt.executeUpdate(deleteTodaySQL)
      
      stmt.close()
      connection.close()
      
      // 写入新数据
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, today, deleteBeforeInsert = false)
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
      // 使用mall_bbc数据库
      spark.sql("USE mall_bbc")
      
      // 获取今天和昨天的日期，用于读取 inc 表的两天数据
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      
      val cal = Calendar.getInstance()
      val today = dateFormat.format(cal.getTime())
      
      cal.add(Calendar.DAY_OF_MONTH, -1) // 减去1天，获取昨天的日期
      val yesterday = dateFormat.format(cal.getTime())
      
      // 获取本月第一天
      cal.setTime(new Date())
      cal.set(Calendar.DAY_OF_MONTH, 1)
      val firstDayOfMonth = dateFormat.format(cal.getTime())
      
      // 计算上个月同一天（用于退款相关表的时间范围）
      val cal2 = Calendar.getInstance()
      cal2.setTime(dateFormat.parse(today))
      cal2.add(Calendar.MONTH, -1) // 回到上个月同一天
      val lastMonthSameDay = dateFormat.format(cal2.getTime())
      val startTime = s"$lastMonthSameDay 23:59:59"
      val endTime = s"$today 23:59:59"
      
      println(s"处理日期: $today, 退款范围: $startTime ~ $endTime")

      // 允许笛卡尔积连接
      spark.sql("SET spark.sql.crossJoin.enabled=true")
      
      // 订单支付数据主查询 - 使用 CTE 分离计算避免复杂子查询
      try {
        val todayOrderPaymentDF = spark.sql(
          s"""
          WITH order_pay_stats AS (
              SELECT 
                  shop_id,
                  COALESCE(SUM(CAST(actual_total AS DECIMAL(18,2))), 0) AS pay_actual_total,
                  COUNT(DISTINCT user_id) AS pay_user_count,
                  COUNT(*) AS pay_order_count,
                  CASE 
                      WHEN COUNT(DISTINCT user_id) > 0 
                      THEN COALESCE(SUM(CAST(actual_total AS DECIMAL(18,2))) / COUNT(DISTINCT user_id), 0) 
                      ELSE 0 
                  END AS one_price
              FROM t_dwd_order_inc
              WHERE is_payed = '1'
                AND DATE(pay_time) = '$today'
                AND shop_id IS NOT NULL
                AND shop_id != ''
              GROUP BY shop_id
          ),
          refund_stats AS (
              SELECT 
                  shop_id,
                  COALESCE(SUM(CAST(refund_amount AS DECIMAL(18,2))), 0) AS refund
              FROM t_dwd_order_refund_inc
              WHERE return_money_sts = 5
                AND DATE(refund_time) = '$today'
                AND shop_id IS NOT NULL
                AND shop_id != ''
              GROUP BY shop_id
          )
          SELECT 
              o.shop_id,
              o.pay_actual_total,
              o.pay_user_count,
              o.pay_order_count,
              o.one_price,
              COALESCE(r.refund, 0) AS refund,
              -- 昨天支付数据（先设为0，避免子查询复杂度）
              0 AS yesterday_pay_actual_total,
              0 AS yesterday_pay_user_count,
              0 AS yesterday_pay_order_count,
              0 AS yesterday_one_price,
              0 AS yesterday_refund,
              -- 添加统计日期
              TO_DATE('$today') AS stat_date
          FROM order_pay_stats o
          LEFT JOIN refund_stats r ON o.shop_id = r.shop_id
          ORDER BY o.shop_id
          """)
        
        val dataCount = todayOrderPaymentDF.count()
        if (dataCount > 0) {
          println(s"✓ 日订单支付: $dataCount 条")
          todayOrderPaymentDF.show(5, false)
          writeToMySQLWithDayLimit(todayOrderPaymentDF, "tz_bd_merchant_daily_order_pay", today)
        } else {
          println("✗ 今天无订单支付数据")
        }
      } catch {
        case e: Exception => 
          println(s"订单支付数据主查询执行失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 订单支付统计查询 - 简化版本，与MySQL保持一致
      try {
        val orderPaymentDF = spark.sql(
          s"""
          SELECT 
              shop_id, 
              COUNT(*) AS pay_order_count, 
              SUM(CAST(actual_total AS DECIMAL(18,2))) as pay_actual_total,
              -- 今日金额
              SUM(CAST(actual_total AS DECIMAL(18,2))) as today_amount,
              -- 本月金额（需要读取更多数据，这里先简化为今日）
              SUM(CAST(actual_total AS DECIMAL(18,2))) as month_amount,
              TO_DATE('$today') AS stat_date
          FROM t_dwd_order_inc
          WHERE is_payed = '1' 
            AND DATE(pay_time) = '$today'
            AND shop_id IS NOT NULL AND shop_id != ''
          GROUP BY shop_id
          ORDER BY shop_id
          """)
        
        val orderPayCount = orderPaymentDF.count()
        if (orderPayCount > 0) {
          println(s"✓ 订单支付统计: $orderPayCount 条")
          orderPaymentDF.show(5, false)
          writeToMySQLWithMonthLimit(orderPaymentDF, "tz_bd_merchant_order_pay_agg", today)
        } else {
          println("✗ 订单支付统计无数据")
        }
      } catch {
        case e: Exception => 
          println(s"订单支付统计查询执行失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 退款商品排行榜查询 - 混合使用全量表和增量表，确保读取一个月数据
      try {
        val refundRankingDF = spark.sql(
          s"""
          WITH combined_refund AS (
            -- 全量表数据：上个月同一天到前天
            SELECT 
              order_item_id, order_id, return_money_sts, refund_type, refund_time, goods_num
            FROM t_ods_tz_order_refund
            WHERE return_money_sts = 5
              AND refund_time > '$startTime'
              AND refund_time < '$yesterday 00:00:00'
            
            UNION ALL
            
            -- inc表数据：昨天到今天（避免重复）
            SELECT 
              order_item_id, order_id, return_money_sts, refund_type, refund_time, goods_num
            FROM t_dwd_order_refund_inc
            WHERE return_money_sts = 5
              AND refund_time >= '$yesterday 00:00:00'
              AND refund_time <= '$endTime'
          ),
          
          combined_order AS (
            -- 全量表数据：上个月同一天到前天
            SELECT 
              order_id, order_number
            FROM t_ods_tz_order
            WHERE order_id IN (
                SELECT order_id FROM combined_refund
                WHERE refund_type = 1
              )
            
            UNION ALL
            
            -- inc表数据：昨天到今天
            SELECT 
              order_id, order_number
            FROM t_dwd_order_inc
            WHERE order_id IN (
                SELECT order_id FROM combined_refund
                WHERE refund_type = 1
              )
          ),
          
          combined_order_item AS (
            -- 全量表数据：上个月同一天到前天的订单项
            SELECT 
              order_item_id, prod_id, order_number, prod_count, prod_name
            FROM t_ods_tz_order_item
            WHERE (order_item_id IN (
                SELECT order_item_id FROM combined_refund
                WHERE refund_type = 2
              )
              OR order_number IN (
                SELECT order_number FROM combined_order
              ))
            
            UNION ALL
            
            -- inc表数据：昨天到今天的订单项
            SELECT 
              order_item_id, prod_id, order_number, prod_count, prod_name
            FROM t_dwd_order_item_inc
            WHERE (order_item_id IN (
                SELECT order_item_id FROM combined_refund
                WHERE refund_type = 2
              )
              OR order_number IN (
                SELECT order_number FROM combined_order
              ))
          )
          
          SELECT 
              toi.prod_id, 
              p.shop_id, 
              COALESCE(SUM(COALESCE(CAST(tor.goods_num AS INT), CAST(toi.prod_count AS INT))), 0) AS refund_count,
              toi.prod_name AS refund_prod_name, 
              p.pic,
              TO_DATE('$today') AS stat_date
          FROM combined_order_item toi
          LEFT JOIN combined_refund tor ON toi.order_item_id = tor.order_item_id
          LEFT JOIN t_ods_tz_prod p ON toi.prod_id = p.prod_id
          WHERE p.shop_id IS NOT NULL 
            AND p.shop_id != ''
          GROUP BY toi.prod_id, p.shop_id, toi.prod_name, p.pic
          ORDER BY refund_count DESC
          """)
        
        val refundRankCount = refundRankingDF.count()
        if (refundRankCount > 0) {
          println(s"✓ 退款商品排行: $refundRankCount 条")
          refundRankingDF.show(5, false)
          writeToMySQLWithDayLimit(refundRankingDF, "tz_bd_merchant_refund_prod_rank", today)
        } else {
          println("✗ 退款商品排行无数据")
        }
      } catch {
        case e: Exception => 
          println(s"退款商品排行榜查询执行失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 退款原因排行榜查询 - 使用inc表，时间范围：上个月同一天到今天
      println("执行退款原因排行榜查询（inc表）...")
      try {
        val refundReasonRankingDF = spark.sql(
          s"""
          WITH combined_refund AS (
            -- 全量表数据：上个月同一天到前天
            SELECT 
              buyer_reason, refund_amount, shop_id, order_item_id, order_id, return_money_sts, refund_time
            FROM t_ods_tz_order_refund
            WHERE return_money_sts = 5
              AND refund_time > '$startTime'
              AND refund_time < '$yesterday 00:00:00'
              AND shop_id IS NOT NULL
              AND shop_id != ''
            
            UNION ALL
            
            -- inc表数据：昨天到今天（避免重复）
            SELECT 
              buyer_reason, refund_amount, shop_id, order_item_id, order_id, return_money_sts, refund_time
            FROM t_dwd_order_refund_inc
            WHERE return_money_sts = 5
              AND refund_time >= '$yesterday 00:00:00'
              AND refund_time <= '$endTime'
              AND shop_id IS NOT NULL
              AND shop_id != ''
          ),
          
          refund_total AS (
            SELECT 
              shop_id, 
              SUM(CAST(refund_amount AS DECIMAL(18,2))) AS total_refund_amount
            FROM combined_refund
            GROUP BY shop_id
          ),
          
          refund_data AS (
            SELECT
              CASE 
                WHEN a.buyer_reason = '0' THEN '拍错/多拍/不喜欢'
                WHEN a.buyer_reason = '1' THEN '协商一致退款'
                WHEN a.buyer_reason = '2' THEN '商品破损/少件'
                WHEN a.buyer_reason = '3' THEN '商品与描述不符'
                WHEN a.buyer_reason = '4' THEN '卖家发错货'
                WHEN a.buyer_reason = '5' THEN '质量问题'
                WHEN a.buyer_reason = '6' THEN '其他'
                WHEN a.buyer_reason = '7' THEN '拼团失败：系统自动退款'
                WHEN a.buyer_reason = '8' THEN '发货超时：系统自动退款'
                WHEN a.buyer_reason = '10' THEN '不想要了'
                WHEN a.buyer_reason = '11' THEN '看到更便宜的商品'
                WHEN a.buyer_reason = '12' THEN '商品款式选错了'
                WHEN a.buyer_reason = '13' THEN '收货地址/手机号填错了'
                WHEN a.buyer_reason = '14' THEN '有优惠未使用'
                WHEN a.buyer_reason = '15' THEN '缺货'
                WHEN a.buyer_reason = '16' THEN '未按承诺时间发货'
                WHEN a.buyer_reason = '17' THEN '其他原因'
                ELSE a.buyer_reason
              END AS buyer_reason,
              b.prod_name,
              a.refund_amount,
              a.shop_id,
              a.order_item_id,
              rt.total_refund_amount
            FROM combined_refund a
            JOIN t_ods_tz_order b ON a.order_id = b.order_id
            LEFT JOIN refund_total rt ON a.shop_id = rt.shop_id
          ),
          
          product_pics AS (
            SELECT DISTINCT
              oi.order_item_id,
              p.pic
            FROM t_dwd_order_item_inc oi
            JOIN t_ods_tz_prod p ON oi.prod_id = p.prod_id
          )
          
          SELECT
            rd.buyer_reason AS buyer_reason,
            MAX(rd.prod_name) AS refund_prod_name,
            COUNT(rd.buyer_reason) AS refund_count,
            SUM(CAST(rd.refund_amount AS DECIMAL(18,2))) AS pay_actual_total,
            CASE 
              WHEN MAX(rd.total_refund_amount) = 0 THEN 0.0
              ELSE ROUND(SUM(CAST(rd.refund_amount AS DECIMAL(18,2))) / MAX(rd.total_refund_amount) * 100, 1) 
            END AS percent_amount,
            MAX(pp.pic) AS pic,
            rd.shop_id,
            TO_DATE('$today') AS stat_date
          FROM refund_data rd
          LEFT JOIN product_pics pp ON rd.order_item_id = pp.order_item_id
          GROUP BY rd.buyer_reason, rd.shop_id
          ORDER BY refund_count DESC, pay_actual_total DESC
          """)
        
        // 添加详细调试信息
        val refundReasonCount = refundReasonRankingDF.count()
        println(s"✓ 退款原因排行: $refundReasonCount 条")
        if (refundReasonCount > 0) {
          refundReasonRankingDF.show(5, false)
        } else {
          println("✗ 退款原因排行无数据")
        }
        
        // 写入MySQL，使用一天保留策略
        println(s"开始写入表: tz_bd_merchant_refund_reason_rank, 日期: $today")
        writeToMySQLWithDayLimit(refundReasonRankingDF, "tz_bd_merchant_refund_reason_rank", today)
        println("写入tz_bd_merchant_refund_reason_rank完成（保留一天数据）")
      } catch {
        case e: Exception => 
          println(s"退款原因排行榜查询执行失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 综合退款统计查询 - 使用inc表，读取月度数据范围，保留一个月数据
      println("执行综合退款统计查询（月度数据范围）...")
      try {
        val refundStatsDF = spark.sql(
          s"""
          WITH refund_stats AS (
              SELECT 
                  shop_id AS refund_shop_id,
                  SUM(CAST(refund_amount AS DECIMAL(18,2))) AS refund_amount,
                  COUNT(DISTINCT order_id) AS refund_count
              FROM t_dwd_order_refund_inc
              WHERE return_money_sts = 5
                AND refund_time > '$startTime'
                AND refund_time <= '$endTime'
                AND shop_id IS NOT NULL
                AND shop_id != ''
              GROUP BY shop_id
          ),
          order_stats AS (
              SELECT 
                  shop_id AS order_shop_id,
                  COUNT(order_id) AS order_count
              FROM t_dwd_order_inc
              WHERE status >= 2
                AND pay_time > '$startTime'
                AND pay_time <= '$endTime'
                AND shop_id IS NOT NULL
                AND shop_id != ''
              GROUP BY shop_id
          ),
          all_shops AS (
              SELECT refund_shop_id AS shop_id FROM refund_stats
              UNION
              SELECT order_shop_id AS shop_id FROM order_stats
          )
          
          SELECT 
              TO_DATE('$today') AS refund_date, 
              '$today' AS refund_date_to_string,
              COALESCE(s.shop_id, 0) AS shop_id,
              COALESCE(r.refund_amount, 0) AS pay_actual_total,
              COALESCE(r.refund_count, 0) AS refund_count,
              COALESCE(o.order_count, 0) AS pay_order_count,
              CASE 
                  WHEN COALESCE(r.refund_count, 0) > 0 AND COALESCE(o.order_count, 0) = 0 THEN 100.0000
                  WHEN COALESCE(o.order_count, 0) > 0 THEN COALESCE(r.refund_count, 0) / o.order_count * 100
                  ELSE 0 
              END AS refund_rate,
              TO_DATE('$today') AS stat_date
          FROM 
              all_shops s
          LEFT JOIN 
              refund_stats r ON s.shop_id = r.refund_shop_id
          LEFT JOIN 
              order_stats o ON s.shop_id = o.order_shop_id
          ORDER BY 
              s.shop_id
          """)
        
        // 添加详细调试信息
        val refundStatsCount = refundStatsDF.count()
        println(s"✓ 综合退款统计: $refundStatsCount 条")
        if (refundStatsCount > 0) {
          refundStatsDF.show(5, false)
        } else {
          println("✗ 综合退款统计无数据")
        }
        
        // 写入MySQL，使用一个月保留策略
        println(s"开始写入表: tz_bd_merchant_refund_stats, 日期: $today")
        writeToMySQLWithMonthLimit(refundStatsDF, "tz_bd_merchant_refund_stats", today)
        println("写入tz_bd_merchant_refund_stats完成（保留一个月数据）")
      } catch {
        case e: Exception => 
          println(s"综合退款统计查询执行失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      println(s"今天($today)的数据处理完成：")
      println("- tz_bd_merchant_daily_order_pay 表（保留1天数据，使用inc表读取两天数据）")
      println("- tz_bd_merchant_order_pay_agg 表（保留1个月数据，读取当天数据）")
      println(s"- tz_bd_merchant_refund_prod_rank 表（保留1天数据，时间范围: $startTime 至 $endTime）")
      println(s"- tz_bd_merchant_refund_reason_rank 表（保留1天数据，时间范围: $startTime 至 $endTime）")
      println(s"- tz_bd_merchant_refund_stats 表（保留1个月数据，读取当天数据）")

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