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
      // 设置Spark配置，增加显示字段数和连接稳定性
      spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
      // Hive连接重试和超时配置
      spark.conf.set("hive.metastore.client.connect.retry.delay", "5")
      spark.conf.set("hive.metastore.client.socket.timeout", "600")
      spark.conf.set("hive.metastore.connect.retries", "10")
      spark.conf.set("hive.metastore.failure.retries", "10")
      // 设置网络超时和重试参数
      spark.conf.set("spark.sql.hive.metastore.jars.path", "")
      spark.conf.set("spark.hadoop.hive.metastore.uris", "thrift://cdh02:9083")
      
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
      
      // 对每个时间范围处理所有状态，然后写入对应的表
      for (timeRange <- timeRanges) {
        println(s"\n处理时间范围: $timeRange")
        processTimeRangeAllStatuses(spark, timeRange, statusFilters)
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
            val waitTime = attempt * 5000 // 递增等待时间：5秒、10秒、15秒
            println(s"等待 ${waitTime/1000} 秒后重试...")
            Thread.sleep(waitTime)
            
            // 重新连接Hive MetaStore
            try {
              spark.sql("SHOW DATABASES").collect() // 测试连接
              println("重新连接Hive MetaStore成功")
            } catch {
              case connEx: Exception =>
                println(s"重新连接失败: ${connEx.getMessage}")
            }
          }
      }
    }
    // 如果所有重试都失败，抛出最后一个异常
    throw lastException
  }
  
  /**
   * 处理指定时间范围的所有状态数据
   */
  def processTimeRangeAllStatuses(spark: SparkSession, timeRangeType: String, statusFilters: List[String]): Unit = {
    try {
      // 计算时间范围
      val (startTime, endTime, dateStr) = calculateTimeRange(timeRangeType)
      
      println(s"分析时间范围: $startTime 至 $endTime")
      var allDataFrames = List[org.apache.spark.sql.DataFrame]()
      
      // 对每个状态执行查询
      for (statusFilter <- statusFilters) {
        println(s"处理状态: $statusFilter")
        
        // 生成SQL查询
        val productAnalysisSQL = generateProductAnalysisSQL(startTime, endTime, statusFilter, timeRangeType, dateStr)
        
        // 执行查询，增加重试机制
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
        
        // 去重处理
        val dedupDF = combinedDF.dropDuplicates(Seq("prod_id", "shop_id", "stat_date", "status_filter"))
        val totalCount = combinedDF.count()
        val dedupCount = dedupDF.count()
        
        println(s"\n$timeRangeType 总计: 原始 $totalCount 条，去重后 $dedupCount 条")
        
        if (dedupCount > 0) {
          println(s"\n======= $timeRangeType 商品分析数据 (共 $dedupCount 条) =======")
          dedupDF.show(20, false)
          
          // 写入对应的表
          val tableName = timeRangeType match {
            case "yesterday" => "tz_bd_merchant_product_analysis_yesterday"
            case "7days" => "tz_bd_merchant_product_analysis_7days"
            case "30days" => "tz_bd_merchant_product_analysis_30days"
            case _ => throw new IllegalArgumentException(s"不支持的时间范围: $timeRangeType")
          }
          
          writeToMySQL(dedupDF, tableName, dateStr)
          println(s"$timeRangeType 数据写入完成")
        } else {
          println(s"$timeRangeType 无数据")
        }
      } else {
        println(s"$timeRangeType 所有状态都无数据")
      }
      
    } catch {
      case e: Exception => 
        println(s"处理时间范围 $timeRangeType 时出错: ${e.getMessage}")
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
        (startTime, endTime, endDate) // 使用结束日期作为stat_date
        
      case "30days" =>
        // 近30天：过去30天，以昨天为终止日期
        cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
        val endDate = dateFormat.format(cal.getTime())
        cal.add(Calendar.DAY_OF_MONTH, -29) // 再往前29天，总共30天
        val startDate = dateFormat.format(cal.getTime())
        val startTime = s"$startDate 00:00:00"
        val endTime = s"$endDate 23:59:59"
        (startTime, endTime, endDate) // 使用结束日期作为stat_date
        
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
        '$dateStr' AS stat_date,
        -- 添加额外字段
        '$statusFilter' AS status_filter,
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