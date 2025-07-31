package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive商品分析查询作业
 * 支持多时间范围（昨天、近7天、近30天）和状态分区，使用新埋点数据
 */
object HiveProductAnalysisQueryJob {

  // MySQL连接配置
  private val jdbcUrl = "jdbc:mysql://rm-2zedtr7h3427p19kcbo.mysql.rds.aliyuncs.com:3306/dlc_statistics?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai"
  private val jdbcUser = "bigdata_statistics"
  private val jdbcPassword = "Y&%20Am1!"
  private val jdbcDriver = "com.mysql.cj.jdbc.Driver"

  /**
   * 写入DataFrame到MySQL表
   */
  def writeToMySQL(df: DataFrame, tableName: String, statDate: String): Unit = {
    var connection: Connection = null
    try {
      // 1. 删除指定日期的现有数据
      Class.forName(jdbcDriver)
      connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      val deleteSql = s"DELETE FROM `$tableName` WHERE stat_date = '$statDate'"
      println(s"执行删除SQL: $deleteSql")
      val statement = connection.createStatement()
      val deletedRows = statement.executeUpdate(deleteSql)
      println(s"从 $tableName 表中删除了 $deletedRows 行数据 (日期: $statDate)")

      // 2. 插入新数据
      df.write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("user", jdbcUser)
        .option("password", jdbcPassword)
        .option("driver", jdbcDriver)
        .mode("append")
        .save()
      println(s"成功写入数据到 $tableName 表 (日期: $statDate)")
    } catch {
      case e: Exception =>
        println(s"写入MySQL表 $tableName 时出错: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      if (connection != null) {
        try {
          connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "HiveProductAnalysisQuery"
    val spark: SparkSession = MyHive.conn

    try {
      // 设置Spark配置，增加显示字段数
      spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
      
      println("成功连接到Hive")
      
      // 使用mall_bbc和user_tag数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")
      
      // 定义时间范围类型和状态分区（适合离线数据）
      val timeRanges = List("yesterday", "7days", "30days") // 移除today和thisMonth
      val statusFilters = List("全部", "出售中", "仓库中", "已售空")
      
      println("开始执行Hive商品分析查询...")
      println(s"时间范围: ${timeRanges.mkString(", ")}")
      println(s"状态分区: ${statusFilters.mkString(", ")}")
      
      // 对每个时间范围和状态组合执行查询
      for (timeRange <- timeRanges) {
        for (statusFilter <- statusFilters) {
          println(s"\n处理时间范围: $timeRange, 状态过滤: $statusFilter")
          processTimeRangeAndStatus(spark, timeRange, statusFilter)
        }
      }
      
      println("\n所有数据查询完成")
      
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
   * 处理指定时间范围和状态的数据
   */
  def processTimeRangeAndStatus(spark: SparkSession, timeRangeType: String, statusFilter: String): Unit = {
    try {
      // 计算时间范围
      val (startTime, endTime, dateStr) = calculateTimeRange(timeRangeType)
      
      println(s"分析时间范围: $startTime 至 $endTime")
      
      // 生成SQL查询
      val productAnalysisSQL = generateProductAnalysisSQL(startTime, endTime, statusFilter, timeRangeType, dateStr)
      
      // 执行查询
      val productAnalysisDF = spark.sql(productAnalysisSQL)
      
      val totalCount = productAnalysisDF.count()
      println(s"查询执行完成，共分析了 $totalCount 条商品数据")
      
      // 去重处理，避免唯一索引冲突
      val dedupDF = productAnalysisDF.dropDuplicates(Seq("prod_id", "shop_id", "stat_date", "status"))
      val dedupCount = dedupDF.count()
      
      if (totalCount != dedupCount) {
        println(s"去重处理：原始 $totalCount 条，去重后 $dedupCount 条，减少 ${totalCount - dedupCount} 条")
      }
      
      // 显示数据内容
      if (dedupCount > 0) {
        println(s"\n======= Hive商品分析数据 (共 $dedupCount 条) =======")
        dedupDF.show(50, false)
        
        // TODO: 写入MySQL代码待实现
        // writeToMySQL(dedupDF, "tz_bd_merchant_product_analysis", dateStr)
        println(s"数据查询完成 (时间范围: $timeRangeType, 状态: $statusFilter)")
      } else {
        println("没有数据")
      }
      
    } catch {
      case e: Exception => 
        println(s"处理时间范围 $timeRangeType，状态 $statusFilter 时出错: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  /**
   * 计算时间范围（适合离线数据）
   */
  def calculateTimeRange(timeRangeType: String): (String, String, String) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    
    timeRangeType match {
      case "yesterday" =>
        // 昨天
        cal.add(Calendar.DAY_OF_MONTH, -1)
        val yesterday = dateFormat.format(cal.getTime())
        val startTime = s"$yesterday 00:00:00"
        val endTime = s"$yesterday 23:59:59"
        (startTime, endTime, yesterday)
        
      case "7days" =>
        // 近7天：过去7天，以昨天为终止日期
        cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
        val endDate = dateFormat.format(cal.getTime())
        cal.add(Calendar.DAY_OF_MONTH, -6) // 再往前6天，总共7天
        val startDate = dateFormat.format(cal.getTime())
        val startTime = s"$startDate 00:00:00"
        val endTime = s"$endDate 23:59:59"
        (startTime, endTime, s"${startDate}至${endDate}")
        
      case "30days" =>
        // 近30天：过去30天，以昨天为终止日期
        cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
        val endDate = dateFormat.format(cal.getTime())
        cal.add(Calendar.DAY_OF_MONTH, -29) // 再往前29天，总共30天
        val startDate = dateFormat.format(cal.getTime())
        val startTime = s"$startDate 00:00:00"
        val endTime = s"$endDate 23:59:59"
        (startTime, endTime, s"${startDate}至${endDate}")
        
      case _ =>
        throw new IllegalArgumentException(s"不支持的时间范围类型: $timeRangeType")
    }
  }
  
  /**
   * 生成商品分析SQL（使用新埋点数据）
   */
  def generateProductAnalysisSQL(startTime: String, endTime: String, statusFilter: String, timeRangeType: String, dateStr: String): String = {
    // 根据状态过滤条件生成WHERE子句
    val statusCondition = statusFilter match {
      case "全部" => "p.status > '-1'"  // 全部商品（除删除外）
      case "出售中" => "p.status = '1'"   // 出售中
      case "仓库中" => "p.status = '0'"   // 仓库中
      case "已售空" => "p.status = '3'"   // 已售空
      case _ => "p.status > '-1'"
    }
    
    // 为昨天的情况使用更简单的时间过滤方式
    val (orderTimeCondition, payTimeCondition, refundTimeCondition, exposureTimeCondition) = 
      if (timeRangeType == "yesterday") {
        val dateOnly = dateStr  // dateStr应该是 yyyy-MM-dd 格式
        (
          s"o.create_time LIKE '$dateOnly%'",
          s"o.pay_time LIKE '$dateOnly%'", 
          s"r.apply_time LIKE '$dateOnly%'",
          s"dt = '$dateOnly'"
        )
      } else {
        // 对于多天的情况，需要提取日期范围
        val startDate = startTime.split(" ")(0)  // 从 "yyyy-MM-dd HH:mm:ss" 中提取日期部分
        val endDate = endTime.split(" ")(0)
        (
          s"o.create_time >= '$startTime' AND o.create_time <= '$endTime'",
          s"o.pay_time >= '$startTime' AND o.pay_time <= '$endTime'",
          s"r.apply_time >= '$startTime' AND r.apply_time <= '$endTime'",
          s"dt >= '$startDate' AND dt <= '$endDate'"
        )
      }
    
    s"""
      -- Hive商品分析查询SQL ($timeRangeType - 状态$statusFilter)
      
      WITH product_exposure AS (
        -- 商品曝光数据（使用新埋点）
        SELECT 
          CAST(prodid AS BIGINT) AS prod_id,
          CAST(shopid AS BIGINT) AS shop_id,
          COUNT(*) AS expose_count,
          COUNT(DISTINCT cid) AS expose_person_num
        FROM user_tag.t_ods_app_logdata
        WHERE $exposureTimeCondition
          AND action = 'enter'
          AND page_id = '1005'
          AND shopid = '228'
          AND prodid IS NOT NULL
        GROUP BY CAST(prodid AS BIGINT), CAST(shopid AS BIGINT)
      ),
      
      order_data AS (
        -- 下单数据
        SELECT
          oi.prod_id,
          o.shop_id,
          COUNT(DISTINCT o.user_id) AS order_user_count,
          SUM(CAST(oi.prod_count AS INT)) AS order_item_count,
          SUM(CAST(oi.actual_total AS DECIMAL(18,2))) AS order_amount
        FROM mall_bbc.t_ods_tz_order o
        JOIN mall_bbc.t_ods_tz_order_item oi ON o.order_number = oi.order_number
        WHERE $orderTimeCondition
          AND oi.rec_time >= '$startTime' AND oi.rec_time <= '$endTime'
          AND o.shop_id = '228'
        GROUP BY oi.prod_id, o.shop_id
      ),
      
      pay_data AS (
        -- 支付数据
        SELECT
          oi.prod_id,
          o.shop_id,
          COUNT(DISTINCT o.user_id) AS pay_user_count,
          SUM(CAST(oi.prod_count AS INT)) AS pay_num,
          SUM(CAST(oi.actual_total AS DECIMAL(18,2))) AS pay_amount
        FROM mall_bbc.t_ods_tz_order o
        JOIN mall_bbc.t_ods_tz_order_item oi ON o.order_number = oi.order_number
        WHERE o.is_payed = 'true'
          AND $payTimeCondition
          AND oi.rec_time >= '$startTime' AND oi.rec_time <= '$endTime'
          AND o.shop_id = '228'
        GROUP BY oi.prod_id, o.shop_id
      ),
      
      refund_single AS (
        SELECT DISTINCT
          oi.prod_id,
          oi.shop_id,
          r.refund_id,
          r.user_id,
          r.return_money_sts,
          r.refund_amount
        FROM mall_bbc.t_ods_tz_order_refund r
        LEFT JOIN mall_bbc.t_ods_tz_order_item oi ON r.order_item_id = oi.order_item_id
        WHERE $refundTimeCondition
          AND r.refund_type = '2'
          AND oi.rec_time >= '$startTime' AND oi.rec_time <= '$endTime'
          AND oi.shop_id = '228'
          AND oi.prod_id IS NOT NULL
          AND oi.shop_id IS NOT NULL
      ),
      
      refund_order AS (
        SELECT DISTINCT
          oi.prod_id,
          oi.shop_id,
          r.refund_id,
          r.user_id,
          r.return_money_sts,
          r.refund_amount
        FROM mall_bbc.t_ods_tz_order_refund r
        JOIN mall_bbc.t_ods_tz_order o ON r.order_id = o.order_id
        JOIN mall_bbc.t_ods_tz_order_item oi ON o.order_number = oi.order_number
        WHERE $refundTimeCondition
          AND r.refund_type = '1'
          AND $orderTimeCondition
          AND oi.rec_time >= '$startTime' AND oi.rec_time <= '$endTime'
          AND o.shop_id = '228'
          AND oi.prod_id IS NOT NULL
          AND oi.shop_id IS NOT NULL
      ),
      
      refund_data AS (
        SELECT
          prod_id,
          shop_id,
          COUNT(DISTINCT refund_id) AS refund_num,
          COUNT(DISTINCT user_id) AS refund_person,
          COUNT(DISTINCT CASE WHEN return_money_sts = '5' THEN refund_id END) AS refund_success_num,
          COUNT(DISTINCT CASE WHEN return_money_sts = '5' THEN user_id END) AS refund_success_person,
          SUM(CASE WHEN return_money_sts = '5' THEN refund_amount ELSE 0 END) AS refund_success_amount
        FROM (
          SELECT * FROM refund_single
          UNION ALL
          SELECT * FROM refund_order
        ) combined
        GROUP BY prod_id, shop_id
      )
      
      SELECT
        p.prod_id,
        p.shop_id,
        CAST(p.price AS DECIMAL(18,2)) AS price,
        -- 按照原来的字段名
        COALESCE(pe.expose_count, 0) AS expose,
        COALESCE(pe.expose_person_num, 0) AS expose_person_num,
        COALESCE(od.order_user_count, 0) AS place_order_person,
        COALESCE(pd.pay_user_count, 0) AS pay_person,
        COALESCE(od.order_item_count, 0) AS place_order_num,
        COALESCE(pd.pay_num, 0) AS pay_num,
        COALESCE(od.order_amount, 0) AS place_order_amount,
        COALESCE(pd.pay_amount, 0) AS pay_amount,
        CASE
          WHEN COALESCE(pe.expose_person_num, 0) > 0
          THEN ROUND(COALESCE(pd.pay_user_count, 0) / COALESCE(pe.expose_person_num, 0) * 100, 2)
          ELSE 0
        END AS single_prod_rate,
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
        p.status AS status,
        TO_DATE('$dateStr') AS stat_date,
        -- 添加额外字段
        '$timeRangeType' AS time_range_type,
        '$statusFilter' AS status_filter,
        '$dateStr' AS date_range,
        p.prod_name AS prod_name,
        sd.shop_name AS shop_name
      FROM (
        -- 只取每个商品的最新记录
        SELECT 
          prod_id,
          shop_id,
          prod_name,
          price,
          status
        FROM (
          SELECT 
            prod_id,
            shop_id,
            prod_name,
            price,
            status,
            ROW_NUMBER() OVER (PARTITION BY prod_id, shop_id ORDER BY update_time DESC) as rn
          FROM mall_bbc.t_ods_tz_prod
        ) t 
        WHERE rn = 1
      ) p
      LEFT JOIN (
        SELECT DISTINCT shop_id, first_value(shop_name) OVER (PARTITION BY shop_id ORDER BY shop_name) AS shop_name
        FROM mall_bbc.t_ods_tz_shop_detail
      ) sd ON sd.shop_id = p.shop_id
      LEFT JOIN product_exposure pe ON p.prod_id = pe.prod_id AND p.shop_id = pe.shop_id
      LEFT JOIN order_data od ON p.prod_id = od.prod_id AND p.shop_id = od.shop_id
      LEFT JOIN pay_data pd ON p.prod_id = pd.prod_id AND p.shop_id = pd.shop_id
      LEFT JOIN refund_data rd ON p.prod_id = rd.prod_id AND p.shop_id = rd.shop_id
      WHERE $statusCondition
        AND p.shop_id = '228'
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
      ORDER BY COALESCE(pe.expose_count, 0) DESC
      """
  }
}