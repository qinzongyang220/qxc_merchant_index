package bo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import dao.MyHive
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive实时商品分析查询作业
 * 使用埋点表为主表，LEFT JOIN到DWD层表获取当天实时数据
 */
object HiveRealtimeProductAnalysisJob {

  /**
   * 写入DataFrame到MySQL表
   */
  def writeToMySQL(df: DataFrame, tableName: String, statDate: String): Unit = {
    try {
      // 删除全部数据然后写入新数据
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, statDate, deleteBeforeInsert = true)
      
      println(s"数据成功写入MySQL表: $tableName")
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
    implicit val jobName: String = "HiveRealtimeProductAnalysis"
    val spark: SparkSession = MyHive.conn

    try {
      // 设置Spark配置，增加显示字段数和连接稳定性
      spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

      println("成功连接到Hive")

      // 使用mall_bbc数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")

      // 跨天处理逻辑
      val (startTime, endTime, processDate, currentHour) = getTimeRange()

      println(s"分析实时数据: $processDate")
      println(s"时间范围: $startTime 至 $endTime (当前小时: $currentHour)")
      
      // 显示时间范围说明
      displayTimeRanges(startTime, endTime)

      // 定义状态分区（与HiveProductAnalysisQueryJob保持一致）
      val statusFilters = List(0, 1, 2, 3)
      println(s"状态分区: ${statusFilters.mkString(", ")}")
      
      // 处理实时数据（所有状态）
      processRealtimeDataAllStatuses(spark, startTime, endTime, processDate, statusFilters)

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
   * 显示时间范围说明
   */
  def displayTimeRanges(startTime: String, endTime: String): Unit = {
    println("\n======= 时间范围说明 =======")
    println(s"实时数据 (跨天处理): $startTime 至 $endTime")
    println("==========================\n")
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
   * 处理实时数据（所有状态）
   */
  def processRealtimeDataAllStatuses(spark: SparkSession, startTime: String, endTime: String, dateStr: String, statusFilters: List[Int]): Unit = {
    try {
      var allDataFrames = List[DataFrame]()
      
      // 对每个状态执行查询
      for (statusFilter <- statusFilters) {
        println(s"\n处理状态: $statusFilter")
        
        // 生成SQL查询
        val productAnalysisSQL = generateRealtimeAnalysisSQL(startTime, endTime, dateStr, statusFilter)

        // 执行查询
        val productAnalysisDF = executeWithRetry(spark, productAnalysisSQL, maxRetries = 3)
        val totalCount = productAnalysisDF.count()

        if (totalCount > 0) {
          println(s"状态 $statusFilter: $totalCount 条数据")
          allDataFrames = allDataFrames :+ productAnalysisDF
        } else {
          println(s"状态 $statusFilter: 无数据")
        }
      }
      
      // 合并所有状态的数据
      if (allDataFrames.nonEmpty) {
        val combinedDF = allDataFrames.reduce(_.union(_))
        
        // 去重处理并排序，强制单分区以保持顺序
        val dedupDF = combinedDF.dropDuplicates(Seq("prod_id", "shop_id", "stat_date", "status_filter"))
          .orderBy(col("status_filter"), col("expose").desc)
          .coalesce(1)
        val totalCount = combinedDF.count()
        val dedupCount = dedupDF.count()
        
        println(s"\n实时数据总计: 原始 $totalCount 条，去重后 $dedupCount 条")
        
        if (dedupCount > 0) {
          println(s"\n======= 实时商品分析数据 (共 $dedupCount 条) =======")
          dedupDF.show(50, false)
          
          // 写入MySQL
          writeToMySQL(dedupDF, "tz_bd_merchant_product_analysis_0", dateStr)
        }
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
   * 生成实时商品分析SQL（以商品表为主表）
   */
  def generateRealtimeAnalysisSQL(startTime: String, endTime: String, dateStr: String, statusFilter: Int): String = {
    // 根据状态过滤条件生成WHERE子句，统一处理为字符串比较，最后转换为数字
    val statusCondition = statusFilter match {
      case 0 => "CAST(COALESCE(pi.status, oi.status, p.status, '0') AS INT) > -1"  // 全部商品（除删除外）
      case 1 => "CAST(COALESCE(pi.status, oi.status, p.status, '0') AS INT) = 1"   // 出售中
      case 2 => "CAST(COALESCE(pi.status, oi.status, p.status, '0') AS INT) = 0"   // 仓库中
      case 3 => "CAST(COALESCE(pi.status, oi.status, p.status, '0') AS INT) = 3"   // 已售空
      case _ => "CAST(COALESCE(pi.status, oi.status, p.status, '0') AS INT) > -1"
    }
    s"""
      -- Hive实时商品分析查询SQL (以商品表为主表)

      SELECT
        p.prod_id,
        p.shop_id,
        COALESCE(CAST(p.price AS DECIMAL(18,2)), CAST(oi.price AS DECIMAL(18,2)), 0) as price,
        CAST(p.supplier_price AS DECIMAL(18,2)) AS supplier_price,
        
        -- 曝光指标
        COALESCE(pe.expose_count, 0) AS expose,
        COALESCE(pe.expose_person_num, 0) AS expose_person_num,
        
        -- 加购指标
        0 AS add_cart_person,
        0 AS add_cart,
        
        -- 订单指标
        COALESCE(od.order_user_count, 0) AS place_order_person,
        COALESCE(pd.pay_user_count, 0) AS pay_person,
        COALESCE(od.order_item_count, 0) AS place_order_num,
        COALESCE(pd.pay_num, 0) AS pay_num,
        COALESCE(od.order_amount, 0) AS place_order_amount,
        COALESCE(pd.pay_amount, 0) AS pay_amount,
        
        -- 转化率指标
        CASE
          WHEN COALESCE(pe.expose_person_num, 0) > 0
          THEN ROUND(COALESCE(pd.pay_user_count, 0) / COALESCE(pe.expose_person_num, 0) * 100, 2)
          ELSE 0
        END AS single_prod_rate,
        
        -- 退款指标
        COALESCE(rd.refund_num, 0) AS refund_num,
        COALESCE(rd.refund_person, 0) AS refund_person,
        COALESCE(rd.refund_success_num, 0) AS refund_success_num,
        COALESCE(rd.refund_success_person, 0) AS refund_success_person,
        COALESCE(rd.refund_success_amount, 0) AS refund_success_amount,
        CASE
          WHEN COALESCE(rd.refund_num, 0) > 0
          THEN ROUND(COALESCE(rd.refund_success_num, 0) / COALESCE(rd.refund_num, 0) * 100, 2)
          ELSE 0
        END AS refund_success_rate,
        
        -- 商品状态和过滤
        CAST(COALESCE(pi.status, oi.status, p.status, '0') AS INT) as status,
        CASE 
          WHEN COALESCE(pi.status, oi.status, p.status, '0') = '-1' THEN '删除'
          WHEN COALESCE(pi.status, oi.status, p.status, '0') = '0' THEN '商家下架'
          WHEN COALESCE(pi.status, oi.status, p.status, '0') = '1' THEN '上架'
          WHEN COALESCE(pi.status, oi.status, p.status, '0') = '2' THEN '平台下架'
          WHEN COALESCE(pi.status, oi.status, p.status, '0') = '3' THEN '违规下架待审核'
          WHEN COALESCE(pi.status, oi.status, p.status, '0') = '6' THEN '待审核'
          WHEN COALESCE(pi.status, oi.status, p.status, '0') = '7' THEN '草稿状态'
          ELSE ''
        END AS prod_status,
        '$statusFilter' AS status_filter,
        
        -- 维度和时间
        '$dateStr' AS stat_date,
        
        -- 商品信息
        COALESCE(p.prod_name, oi.prod_name, CONCAT('商品_', p.prod_id)) as prod_name,
        COALESCE(p.pic, '') AS prod_url,
        COALESCE(sd.shop_name, CONCAT('商家_', p.shop_id)) AS shop_name
      FROM (
        -- 商品信息作为主表
        SELECT prod_id, shop_id, prod_name, price, supplier_price, status, pic
        FROM (
          SELECT
            prod_id, shop_id, prod_name, price, supplier_price, status, pic,
            ROW_NUMBER() OVER (PARTITION BY prod_id, shop_id ORDER BY update_time DESC) as rn
          FROM mall_bbc.t_dwd_prod_full
        ) t WHERE rn = 1
      ) p
      LEFT JOIN (
        -- 曝光数据
        SELECT
          prodid AS prod_id,
          shopid AS shop_id,
          COUNT(*) AS expose_count,
          COUNT(DISTINCT cid) AS expose_person_num
        FROM user_tag.t_ods_app_logdata
        WHERE dt = '$dateStr'
          AND action = 'enter'
          AND page_id = '1005'
          AND prodid IS NOT NULL
        GROUP BY prodid, shopid
      ) pe ON p.prod_id = pe.prod_id AND p.shop_id = pe.shop_id
      LEFT JOIN (
        -- 商店信息（使用原始ODS表，有历史数据）
        SELECT DISTINCT
          shop_id,
          first_value(shop_name) OVER (PARTITION BY shop_id ORDER BY shop_name) AS shop_name
        FROM mall_bbc.t_dwd_shop_detail_full
      ) sd ON p.shop_id = sd.shop_id
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
      ) oi ON p.prod_id = oi.prod_id AND p.shop_id = oi.shop_id
      LEFT JOIN (
        -- 今日商品状态信息（最新状态）
        SELECT DISTINCT
          prod_id,
          shop_id,
          first_value(status) OVER (PARTITION BY prod_id, shop_id ORDER BY update_time DESC) as status
        FROM mall_bbc.t_dwd_prod_inc
        WHERE dt = '$dateStr'
          AND is_delete_hive = '0'
      ) pi ON p.prod_id = pi.prod_id AND p.shop_id = pi.shop_id
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
      ) od ON p.prod_id = od.prod_id AND p.shop_id = od.shop_id
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
      ) pd ON p.prod_id = pd.prod_id AND p.shop_id = pd.shop_id
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
      ) rd ON p.prod_id = rd.prod_id AND p.shop_id = rd.shop_id
      WHERE p.shop_id IS NOT NULL
        AND p.shop_id != ''
        AND $statusCondition
        -- 过滤掉所有关键指标都为0的记录
        AND (COALESCE(pe.expose_count, 0) > 0 
          OR COALESCE(pe.expose_person_num, 0) > 0 
          OR COALESCE(od.order_user_count, 0) > 0
          OR COALESCE(od.order_item_count, 0) > 0
          OR COALESCE(od.order_amount, 0) > 0
          OR COALESCE(pd.pay_user_count, 0) > 0
          OR COALESCE(pd.pay_num, 0) > 0
          OR COALESCE(pd.pay_amount, 0) > 0
          OR COALESCE(rd.refund_num, 0) > 0
          OR COALESCE(rd.refund_success_amount, 0) > 0)
      ORDER BY status_filter ASC, COALESCE(pe.expose_count, 0) DESC, COALESCE(od.order_amount, 0) DESC
      """
  }
  
  /**
   * 获取跨天处理的时间范围
   */
  def getTimeRange(): (String, String, String, Int) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = Calendar.getInstance()
    val currentHour = cal.get(Calendar.HOUR_OF_DAY)
    val today = dateFormat.format(cal.getTime())
    
    val (startTime, endTime, processDate) = if (currentHour == 0) {
      // 凌晨0点时，处理昨天全天数据
      cal.add(Calendar.DAY_OF_MONTH, -1)
      val yesterday = dateFormat.format(cal.getTime())
      (s"$yesterday 00:00:00", s"$yesterday 23:59:59", yesterday)
    } else {
      // 其他时间，处理今天从0点到当前时间的数据
      val currentTime = timeFormat.format(cal.getTime())
      (s"$today 00:00:00", currentTime, today)
    }
    
    (startTime, endTime, processDate, currentHour)
  }
}