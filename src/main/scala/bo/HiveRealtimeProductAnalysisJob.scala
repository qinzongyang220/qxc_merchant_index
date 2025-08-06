package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive实时商品分析查询作业
 * 使用埋点表为主表，LEFT JOIN到DWD层表获取当天实时数据
 */
object HiveRealtimeProductAnalysisJob {

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "HiveRealtimeProductAnalysis"
    val spark: SparkSession = MyHive.conn

    try {
      // 设置Spark配置，增加显示字段数和连接稳定性
      spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
      
      println("成功连接到Hive")
      
      // 使用mall_bbc数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")
      
      // 只处理当天数据
      val today = getTodayDate()
      val startTime = s"$today 00:00:00"
      val endTime = s"$today 23:59:59"
      
      println(s"分析当天数据: $today")
      println(s"分析时间范围: $startTime 至 $endTime")
      
      // 处理实时数据
      processRealtimeData(spark, startTime, endTime, today)
      
      println("\n实时数据查询完成")
      
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
   * 获取今天日期
   */
  def getTodayDate(): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(Calendar.getInstance().getTime())
  }
  
  /**
   * 带重试机制的SQL执行方法
   */
  def executeWithRetry(spark: SparkSession, sql: String, maxRetries: Int = 3): DataFrame = {
    var lastException: Exception = null
    for (attempt <- 1 to maxRetries) {
      try {
        println(s"执行SQL查询 (尝试 $attempt/$maxRetries)")
        return spark.sql(sql)
      } catch {
        case e: Exception =>
          lastException = e
          println(s"第 $attempt 次尝试失败: ${e.getMessage}")
          if (attempt < maxRetries) {
            val waitTime = attempt * 5000
            println(s"等待 ${waitTime/1000} 秒后重试...")
            Thread.sleep(waitTime)
            
            // 重新连接Hive MetaStore
            try {
              spark.sql("SHOW DATABASES").collect()
              println("重新连接Hive MetaStore成功")
            } catch {
              case connEx: Exception =>
                println(s"重新连接失败: ${connEx.getMessage}")
            }
          }
      }
    }
    throw lastException
  }
  
  /**
   * 处理实时数据（以埋点表为主表）
   */
  def processRealtimeData(spark: SparkSession, startTime: String, endTime: String, dateStr: String): Unit = {
    try {
      // 生成SQL查询
      val productAnalysisSQL = generateRealtimeAnalysisSQL(startTime, endTime, dateStr)
      
      // 执行查询
      val productAnalysisDF = executeWithRetry(spark, productAnalysisSQL, maxRetries = 3)
      val totalCount = productAnalysisDF.count()
      
      if (totalCount > 0) {
        println(s"\n======= 实时商品分析数据 (共 $totalCount 条) =======")
        productAnalysisDF.show(50, false)
      } else {
        println("当天无数据")
      }
      
    } catch {
      case e: Exception => 
        println(s"处理实时数据时出错: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  /**
   * 生成实时商品分析SQL（以埋点表为主表）
   */
  def generateRealtimeAnalysisSQL(startTime: String, endTime: String, dateStr: String): String = {
    s"""
      -- Hive实时商品分析查询SQL (以埋点表为主表)
      
      SELECT
        pe.prod_id,
        pe.shop_id,
        pe.expose_count,
        pe.expose_person_num,
        -- 商品基础信息从历史表获取，状态从今日inc表获取
        COALESCE(p.prod_name, oi.prod_name, CONCAT('商品_', pe.prod_id)) as prod_name,
        COALESCE(CAST(p.price AS DECIMAL(18,2)), CAST(oi.price AS DECIMAL(18,2)), 0) as price,
        COALESCE(pi.status, oi.status, p.status, '未知状态') as status,
        COALESCE(od.order_user_count, 0) AS order_user_count,
        COALESCE(od.order_item_count, 0) AS order_item_count,
        COALESCE(od.order_amount, 0) AS order_amount,
        COALESCE(pd.pay_user_count, 0) AS pay_user_count,
        COALESCE(pd.pay_num, 0) AS pay_num,
        COALESCE(pd.pay_amount, 0) AS pay_amount,
        -- 转化率
        CASE
          WHEN pe.expose_person_num > 0
          THEN ROUND(COALESCE(pd.pay_user_count, 0) / pe.expose_person_num * 100, 2)
          ELSE 0
        END AS single_prod_rate,
        -- 退款指标
        COALESCE(rd.refund_num, 0) AS refund_order_count,
        COALESCE(rd.refund_person, 0) AS refund_user_count,
        COALESCE(rd.refund_success_num, 0) AS success_refund_order_count,
        COALESCE(rd.refund_success_person, 0) AS success_refund_user_count,
        COALESCE(rd.refund_success_amount, 0) AS success_refund_amount,
        CASE
          WHEN COALESCE(rd.refund_num, 0) > 0
          THEN ROUND(COALESCE(rd.refund_success_num, 0) / COALESCE(rd.refund_num, 0) * 100, 2)
          ELSE 0
        END AS refund_success_rate,
        -- 额外字段
        'today' AS time_range_type,
        '$dateStr' AS stat_date,
        COALESCE(sd.shop_name, '商家228') AS shop_name
      FROM (
        -- 曝光数据作为主表（确保有数据）
        SELECT 
          prodid AS prod_id,
          shopid AS shop_id,
          COUNT(*) AS expose_count,
          COUNT(DISTINCT cid) AS expose_person_num
        FROM user_tag.t_ods_app_logdata
        WHERE dt = '$dateStr'
          AND shopid = '228'
          AND action = 'enter'
          AND page_id = '1005'
          AND prodid IS NOT NULL
        GROUP BY prodid, shopid
      ) pe
      LEFT JOIN (
        -- 商品信息（使用原始ODS表，有历史数据）
        SELECT prod_id, shop_id, prod_name, price, status
        FROM (
          SELECT 
            prod_id, shop_id, prod_name, price, status,
            ROW_NUMBER() OVER (PARTITION BY prod_id, shop_id ORDER BY update_time DESC) as rn
          FROM mall_bbc.t_ods_tz_prod
        ) t WHERE rn = 1
      ) p ON pe.prod_id = p.prod_id AND pe.shop_id = p.shop_id
      LEFT JOIN (
        -- 商店信息（使用原始ODS表，有历史数据）
        SELECT DISTINCT 
          shop_id, 
          first_value(shop_name) OVER (PARTITION BY shop_id ORDER BY shop_name) AS shop_name
        FROM mall_bbc.t_ods_tz_shop_detail
      ) sd ON pe.shop_id = sd.shop_id
      LEFT JOIN (
        -- 从订单表获取商品基础信息（优先级最高，因为能关联上）
        SELECT DISTINCT
          oi.prod_id,
          o.shop_id,
          first_value(oi.prod_name) OVER (PARTITION BY oi.prod_id, o.shop_id ORDER BY o.create_time DESC) as prod_name,
          first_value(oi.price) OVER (PARTITION BY oi.prod_id, o.shop_id ORDER BY o.create_time DESC) as price,
          first_value(oi.status) OVER (PARTITION BY oi.prod_id, o.shop_id ORDER BY o.create_time DESC) as status
        FROM mall_bbc.t_dwd_order_inc o
        JOIN mall_bbc.t_dwd_order_item_inc oi ON o.order_number = oi.order_number
        WHERE o.dt = '$dateStr' AND oi.dt = '$dateStr'
      ) oi ON pe.prod_id = oi.prod_id AND pe.shop_id = oi.shop_id
      LEFT JOIN (
        -- 今日商品状态信息（最新状态）
        SELECT DISTINCT
          prod_id,
          shop_id,
          first_value(status) OVER (PARTITION BY prod_id, shop_id ORDER BY update_time DESC) as status
        FROM mall_bbc.t_dwd_prod_inc
        WHERE dt = '$dateStr'
          AND is_delete_hive = '0'
      ) pi ON pe.prod_id = pi.prod_id AND pe.shop_id = pi.shop_id
      LEFT JOIN (
        -- 下单数据
        SELECT
          oi.prod_id, o.shop_id,
          COUNT(DISTINCT o.user_id) AS order_user_count,
          SUM(CAST(oi.prod_count AS INT)) AS order_item_count,
          SUM(CAST(oi.actual_total AS DECIMAL(18,2))) AS order_amount
        FROM mall_bbc.t_dwd_order_inc o
        JOIN mall_bbc.t_dwd_order_item_inc oi ON o.order_number = oi.order_number
        WHERE o.create_time LIKE '$dateStr%' 
          AND o.dt = '$dateStr' 
          AND oi.dt = '$dateStr'
        GROUP BY oi.prod_id, o.shop_id
      ) od ON pe.prod_id = od.prod_id AND pe.shop_id = od.shop_id
      LEFT JOIN (
        -- 支付数据
        SELECT
          oi.prod_id, o.shop_id,
          COUNT(DISTINCT o.user_id) AS pay_user_count,
          SUM(CAST(oi.prod_count AS INT)) AS pay_num,
          SUM(CAST(oi.actual_total AS DECIMAL(18,2))) AS pay_amount
        FROM mall_bbc.t_dwd_order_inc o
        JOIN mall_bbc.t_dwd_order_item_inc oi ON o.order_number = oi.order_number
        WHERE o.is_payed = '1' 
          AND o.pay_time LIKE '$dateStr%' 
          AND o.dt = '$dateStr' 
          AND oi.dt = '$dateStr'
        GROUP BY oi.prod_id, o.shop_id
      ) pd ON pe.prod_id = pd.prod_id AND pe.shop_id = pd.shop_id
      LEFT JOIN (
        -- 退款数据
        SELECT
          combined.prod_id,
          combined.shop_id,
          COUNT(DISTINCT combined.refund_id) AS refund_num,
          COUNT(DISTINCT combined.user_id) AS refund_person,
          COUNT(DISTINCT CASE WHEN combined.return_money_sts = '5' THEN combined.refund_id END) AS refund_success_num,
          COUNT(DISTINCT CASE WHEN combined.return_money_sts = '5' THEN combined.user_id END) AS refund_success_person,
          SUM(CASE WHEN combined.return_money_sts = '5' THEN CAST(combined.refund_amount AS DECIMAL(18,2)) ELSE 0 END) AS refund_success_amount
        FROM (
          -- 单项退款
          SELECT
            oi.prod_id, oi.shop_id, r.refund_id, r.user_id, r.return_money_sts, r.refund_amount
          FROM mall_bbc.t_dwd_order_refund_inc r
          LEFT JOIN mall_bbc.t_dwd_order_item_inc oi ON r.order_item_id = oi.order_item_id
          WHERE r.apply_time LIKE '$dateStr%'
            AND r.refund_type = '2'
            AND r.dt = '$dateStr'
            AND oi.dt = '$dateStr'
            AND oi.prod_id IS NOT NULL
            AND oi.shop_id IS NOT NULL
          UNION ALL
          -- 整单退款
          SELECT
            oi.prod_id, oi.shop_id, r.refund_id, r.user_id, r.return_money_sts, r.refund_amount
          FROM mall_bbc.t_dwd_order_refund_inc r
          JOIN mall_bbc.t_dwd_order_inc o ON r.order_id = o.order_id
          JOIN mall_bbc.t_dwd_order_item_inc oi ON o.order_number = oi.order_number
          WHERE r.apply_time LIKE '$dateStr%'
            AND r.refund_type = '1'
            AND o.create_time LIKE '$dateStr%'
            AND r.dt = '$dateStr'
            AND o.dt = '$dateStr'
            AND oi.dt = '$dateStr'
            AND oi.prod_id IS NOT NULL
            AND oi.shop_id IS NOT NULL
        ) combined
        GROUP BY combined.prod_id, combined.shop_id
      ) rd ON pe.prod_id = rd.prod_id AND pe.shop_id = rd.shop_id
      ORDER BY pe.expose_count DESC, od.order_amount DESC
      """
  }
}