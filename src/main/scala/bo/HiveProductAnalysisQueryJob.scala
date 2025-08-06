package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive商品分析查询作业
 * 支持多时间范围（昨天、近7天、近30天）和状态分区，使用新埋点数据
 */
object HiveProductAnalysisQueryJob {

  /**
   * 写入DataFrame到MySQL表
   */
  def writeToMySQL(df: DataFrame, tableName: String, statDate: String, timeRangeType: String): Unit = {
    try {
      // 检查并创建表
      ensureTableExists(tableName, timeRangeType)
      
      // 根据时间范围类型执行不同的删除策略
      val deleteStrategy = timeRangeType match {
        case "7days" | "30days" =>
          // 7天和30天：删除全部数据
          "DELETE_ALL"
        case "thisMonth" =>
          // 自然月：只删除当前月份的数据
          val cal = Calendar.getInstance()
          val year = cal.get(Calendar.YEAR)
          val month = f"${cal.get(Calendar.MONTH) + 1}%02d"
          s"DELETE_BY_MONTH:$year-$month"
        case _ => "DELETE_ALL"
      }
      
      // 执行删除策略
      executeDeleteStrategy(tableName, deleteStrategy)
      
      // 写入数据（不自动删除，因为我们已经手动删除了）
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, statDate, deleteBeforeInsert = false)
    } catch {
      case e: Exception =>
        println(s"写入MySQL表 $tableName 时出错: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }
  
  /**
   * 执行删除策略
   */
  def executeDeleteStrategy(tableName: String, deleteStrategy: String): Unit = {
    try {
      val connection = Constants.DatabaseUtils.getWriteConnection
      val stmt = connection.createStatement()
      
      // 检查表是否存在
      val checkTableSQL = s"SHOW TABLES LIKE '$tableName'"
      val rs = stmt.executeQuery(checkTableSQL)
      val tableExists = rs.next()
      rs.close()
      
      if (tableExists) {
        val deleteSQL = if (deleteStrategy == "DELETE_ALL") {
          s"DELETE FROM $tableName"
        } else if (deleteStrategy.startsWith("DELETE_BY_MONTH:")) {
          val monthPattern = deleteStrategy.substring("DELETE_BY_MONTH:".length)
          s"DELETE FROM $tableName WHERE stat_date LIKE '$monthPattern%'"
        } else {
          throw new IllegalArgumentException(s"未知的删除策略: $deleteStrategy")
        }
        
        println(s"执行删除策略: $deleteSQL")
        val deletedRows = stmt.executeUpdate(deleteSQL)
        println(s"删除了 $deletedRows 行数据")
      } else {
        println(s"表 $tableName 不存在，跳过删除策略")
      }
      
      stmt.close()
      connection.close()
    } catch {
      case e: Exception =>
        println(s"执行删除策略时出错: ${e.getMessage}")
        throw e
    }
  }
  
  /**
   * 确保表存在，如果不存在则创建
   */
  def ensureTableExists(tableName: String, timeRangeType: String): Unit = {
    try {
      val connection = Constants.DatabaseUtils.getWriteConnection
      val stmt = connection.createStatement()
      
      // 检查表是否存在
      val checkTableSQL = s"SHOW TABLES LIKE '$tableName'"
      val rs = stmt.executeQuery(checkTableSQL)
      
      if (!rs.next()) {
        // 表不存在，创建表
        println(s"表 $tableName 不存在，正在创建...")
        val createTableSQL = generateCreateTableSQL(tableName, timeRangeType)
        stmt.execute(createTableSQL)
        println(s"表 $tableName 创建成功")
      }
      
      rs.close()
      stmt.close()
      connection.close()
    } catch {
      case e: Exception =>
        println(s"检查/创建表 $tableName 时出错: ${e.getMessage}")
        throw e
    }
  }
  
  /**
   * 生成建表SQL
   */
  def generateCreateTableSQL(tableName: String, timeRangeType: String): String = {
    val comment = timeRangeType match {
      case "7days" => "商品洞察-7天"
      case "30days" => "商品洞察-30天" 
      case "thisMonth" => "商品洞察-自然月"
      case _ => "商品洞察"
    }
    
    s"""
    CREATE TABLE `$tableName` (
      `id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
      `prod_id` BIGINT(20) NOT NULL COMMENT '商品ID',
      `shop_id` BIGINT(20) NOT NULL COMMENT '商店ID',
      `price` DECIMAL(18,2) DEFAULT NULL COMMENT '商品价格',
      `expose` BIGINT(20) DEFAULT 0 COMMENT '曝光次数',
      `expose_person_num` BIGINT(20) DEFAULT 0 COMMENT '曝光人数',
      `place_order_person` BIGINT(20) DEFAULT 0 COMMENT '下单人数',
      `pay_person` BIGINT(20) DEFAULT 0 COMMENT '支付人数',
      `place_order_num` BIGINT(20) DEFAULT 0 COMMENT '下单件数',
      `pay_num` BIGINT(20) DEFAULT 0 COMMENT '支付件数',
      `place_order_amount` DECIMAL(18,2) DEFAULT 0.00 COMMENT '下单金额',
      `pay_amount` DECIMAL(18,2) DEFAULT 0.00 COMMENT '支付金额',
      `single_prod_rate` DECIMAL(5,2) DEFAULT 0.00 COMMENT '单品转化率(%)',
      `refund_num` BIGINT(20) DEFAULT 0 COMMENT '申请退款订单数',
      `refund_person` BIGINT(20) DEFAULT 0 COMMENT '申请退款人数',
      `refund_success_num` BIGINT(20) DEFAULT 0 COMMENT '成功退款订单数',
      `refund_success_person` BIGINT(20) DEFAULT 0 COMMENT '成功退款人数',
      `refund_success_amount` DECIMAL(18,2) DEFAULT 0.00 COMMENT '成功退款金额',
      `refund_success_rate` DECIMAL(5,2) DEFAULT 0.00 COMMENT '退款成功率(%)',
      `status` INT(11) DEFAULT NULL COMMENT '商品状态',
      `status_filter` VARCHAR(50) DEFAULT NULL COMMENT '状态过滤器',
      `prod_name` VARCHAR(500) DEFAULT NULL COMMENT '商品名称',
      `shop_name` VARCHAR(200) DEFAULT NULL COMMENT '商店名称',
      `stat_date` DATE NOT NULL COMMENT '统计日期',
      PRIMARY KEY (`id`),
      UNIQUE KEY `uk_prod_shop_statdate_status` (`prod_id`, `shop_id`, `stat_date`, `status_filter`),
      KEY `idx_stat_date` (`stat_date`),
      KEY `idx_shop_id` (`shop_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='$comment'
    """.trim
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
      // Hive连接重试和超时配置已在MyHive中统一配置
      
      println("成功连接到Hive")
      
      // 使用mall_bbc和user_tag数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")
      
      // 定义时间范围类型和状态分区（适合离线数据）
      val timeRanges = List("7days", "30days", "thisMonth")
      val statusFilters = List(0, 1, 2, 3)
      
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
  def processTimeRangeAllStatuses(spark: SparkSession, timeRangeType: String, statusFilters: List[Int]): Unit = {
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
            case "7days" => "tz_bd_merchant_product_analysis_7"
            case "30days" => "tz_bd_merchant_product_analysis_30"
            case "thisMonth" => 
              // 动态生成月份表名（去掉下划线）
              val cal = Calendar.getInstance()
              val year = cal.get(Calendar.YEAR)
              val month = f"${cal.get(Calendar.MONTH) + 1}%02d" // Calendar.MONTH从0开始
              s"tz_bd_merchant_product_analysis_${year}${month}"
            case _ => throw new IllegalArgumentException(s"不支持的时间范围: $timeRangeType")
          }
          
          writeToMySQL(dedupDF, tableName, dateStr, timeRangeType)
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
      case "thisMonth" =>
        // 自然月：本月1号到昨天
        val today = cal.getTime()
        cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
        val yesterday = dateFormat.format(cal.getTime())
        
        cal.setTime(today) // 恢复到今天
        cal.set(Calendar.DAY_OF_MONTH, 1) // 设置到本月1号
        val monthStart = dateFormat.format(cal.getTime())
        
        val startTime = s"$monthStart 00:00:00"
        val endTime = s"$yesterday 23:59:59"
        
        // 分区使用今天日期（运行时间）
        cal.setTime(today)
        val runDate = dateFormat.format(cal.getTime())
        (startTime, endTime, runDate)
        
      case "7days" =>
        // 近7天：过去7天，以昨天为终止日期
        val today = cal.getTime()
        cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
        val endDate = dateFormat.format(cal.getTime())
        cal.add(Calendar.DAY_OF_MONTH, -6) // 再往前6天，总共7天
        val startDate = dateFormat.format(cal.getTime())
        val startTime = s"$startDate 00:00:00"
        val endTime = s"$endDate 23:59:59"
        
        // 分区使用今天日期（运行时间）
        cal.setTime(today)
        val runDate = dateFormat.format(cal.getTime())
        (startTime, endTime, runDate)
        
      case "30days" =>
        // 近30天：过去30天，以昨天为终止日期
        val today = cal.getTime()
        cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
        val endDate = dateFormat.format(cal.getTime())
        cal.add(Calendar.DAY_OF_MONTH, -29) // 再往前29天，总共30天
        val startDate = dateFormat.format(cal.getTime())
        val startTime = s"$startDate 00:00:00"
        val endTime = s"$endDate 23:59:59"
        
        // 分区使用今天日期（运行时间）
        cal.setTime(today)
        val runDate = dateFormat.format(cal.getTime())
        (startTime, endTime, runDate)
        
      case _ =>
        throw new IllegalArgumentException(s"不支持的时间范围类型: $timeRangeType")
    }
  }
  
  /**
   * 生成商品分析SQL（使用新埋点数据）
   */
  def generateProductAnalysisSQL(startTime: String, endTime: String, statusFilter: Int, timeRangeType: String, dateStr: String): String = {
    // 根据状态过滤条件生成WHERE子句
    val statusCondition = statusFilter match {
      case 0 => "p.status > '-1'"  // 全部商品（除删除外）
      case 1 => "p.status = '1'"   // 出售中
      case 2 => "p.status = '0'"   // 仓库中
      case 3 => "p.status = '3'"   // 已售空
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
        $statusFilter AS status_filter,
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
        AND p.shop_id IS NOT NULL
        AND p.shop_id != ''
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