package bo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import dao.MyHive
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive商品综合统计查询作业
 * 支持商品销售指标、服务指标的综合统计分析
 */
object HiveProductComprehensiveStatsJob {

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "HiveProductComprehensiveStats"
    val spark: SparkSession = MyHive.conn

    try {
      println("成功连接到Hive")

      // 使用mall_bbc数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")


      // 定义时间范围类型（移除today和todayRealtime，只处理离线数据）
      val timeRanges = List("7days", "30days", "thisMonth") // 处理7天、30天和自然月数据

      println("开始执行Hive商品综合统计查询（离线数据）...")
      println(s"时间范围: ${timeRanges.mkString(", ")}")

      // 显示所有时间范围的具体时间段
      displayTimeRanges(timeRanges)

      // 对每个时间范围处理，参考MySQLProductComprehensiveStatsJob的个别状态处理模式
      for (timeRange <- timeRanges) {
        println(s"\n处理时间范围: $timeRange")
        processTimeRangeAllStatuses(spark, timeRange)
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
   * 获取表名
   */
  def getTableName(timeRangeType: String): String = {
    timeRangeType match {
      case "7days" => "tz_bd_platform_product_analysis_7"
      case "30days" => "tz_bd_platform_product_analysis_30"
      case "thisMonth" =>
        // 根据运行日期决定表名
        val cal = Calendar.getInstance()
        val todayDay = cal.get(Calendar.DAY_OF_MONTH)

        if (todayDay == 1) {
          // 1号运行：使用上个月作为表名
          cal.add(Calendar.MONTH, -1) // 回到上个月
          val year = cal.get(Calendar.YEAR)
          val month = f"${cal.get(Calendar.MONTH) + 1}%02d"
          s"tz_bd_platform_product_analysis_${year}${month}"
        } else {
          // 其他日期运行：使用当前月份作为表名
          val year = cal.get(Calendar.YEAR)
          val month = f"${cal.get(Calendar.MONTH) + 1}%02d"
          s"tz_bd_platform_product_analysis_${year}${month}"
        }
      case _ => throw new IllegalArgumentException(s"不支持的时间范围: $timeRangeType")
    }
  }

  /**
   * 显示所有时间范围的具体时间段
   */
  def displayTimeRanges(timeRanges: List[String]): Unit = {
    println("\n======= 时间范围说明 =======")
    for (timeRange <- timeRanges) {
      val (startTime, endTime, _) = calculateTimeRange(timeRange)
      val description = timeRange match {
        case "7days" => "近7天"
        case "30days" => "近30天"
        case "thisMonth" => "本月自然月"
        case _ => timeRange
      }
      println(s"$description ($timeRange): $startTime 至 $endTime")
    }
    println("==========================\n")
  }

  /**
   * 确保表存在，如果不存在则创建
   */
  def ensureTableExists(tableName: String): Unit = {
    try {
      val connection = Constants.DatabaseUtils.getWriteConnection
      val stmt = connection.createStatement()

      // 检查表是否存在
      val checkTableSQL = s"SHOW TABLES LIKE '$tableName'"
      val rs = stmt.executeQuery(checkTableSQL)

      if (!rs.next()) {
        // 表不存在，创建表
        println(s"表 $tableName 不存在，正在创建...")
        val createTableSQL = generateCreateTableSQL(tableName)
        stmt.execute(createTableSQL)
        println(s"表 $tableName 创建成功")
      } else {
        println(s"表 $tableName 已存在")
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
  def generateCreateTableSQL(tableName: String): String = {
    s"""
    CREATE TABLE `$tableName` (
      `id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
      `prod_id` BIGINT(20) NOT NULL COMMENT '商品ID',
      `price` DECIMAL(18,2) DEFAULT NULL COMMENT '商品价格',
      `supplier_price` DECIMAL(18,2) DEFAULT NULL COMMENT '供应商价格',

      -- 曝光指标
      `expose` BIGINT(20) DEFAULT 0 COMMENT '曝光次数',
      `expose_person_num` BIGINT(20) DEFAULT 0 COMMENT '曝光人数',

      -- 加购指标
      `add_cart` BIGINT(20) DEFAULT 0 COMMENT '加购次数',
      `add_cart_person` BIGINT(20) DEFAULT 0 COMMENT '加购人数',

      -- 订单指标
      `place_order_person` BIGINT(20) DEFAULT 0 COMMENT '下单人数',
      `pay_person` BIGINT(20) DEFAULT 0 COMMENT '支付人数',
      `place_order_num` BIGINT(20) DEFAULT 0 COMMENT '下单件数',
      `pay_num` BIGINT(20) DEFAULT 0 COMMENT '支付件数',
      `place_order_amount` DECIMAL(18,2) DEFAULT 0.00 COMMENT '下单金额',
      `pay_amount` DECIMAL(18,2) DEFAULT 0.00 COMMENT '支付金额',

      -- 转化率指标
      `single_prod_rate` DECIMAL(5,2) DEFAULT 0.00 COMMENT '单品转化率(%)',

      -- 退款指标
      `refund_num` BIGINT(20) DEFAULT 0 COMMENT '申请退款订单数',
      `refund_person` BIGINT(20) DEFAULT 0 COMMENT '申请退款人数',
      `refund_success_num` BIGINT(20) DEFAULT 0 COMMENT '成功退款订单数',
      `refund_success_person` BIGINT(20) DEFAULT 0 COMMENT '成功退款人数',
      `refund_success_amount` DECIMAL(18,2) DEFAULT 0.00 COMMENT '成功退款金额',
      `refund_success_rate` DECIMAL(5,2) DEFAULT 0.00 COMMENT '退款成功率(%)',

      -- 商品状态和过滤
      `status` INT(11) DEFAULT NULL COMMENT '商品状态',
      `prod_status` VARCHAR(50) DEFAULT NULL COMMENT '商品状态描述',
      `status_filter` VARCHAR(50) DEFAULT NULL COMMENT '状态过滤器(0:全部,1:出售中,2:仓库中,3:已售空)',

      -- 维度和时间
      `stat_date` VARCHAR(50) NOT NULL COMMENT '统计日期',

      -- 商品信息
      `prod_name` VARCHAR(500) DEFAULT NULL COMMENT '商品名称',
      `prod_url` VARCHAR(500) DEFAULT NULL COMMENT '商品图片URL',
      `shop_name` VARCHAR(200) DEFAULT NULL COMMENT '店铺名称',

      PRIMARY KEY (`id`),
      UNIQUE KEY `uk_prod_statdate_status` (`prod_id`, `stat_date`, `status_filter`),
      KEY `idx_stat_date` (`stat_date`),
      KEY `idx_status_filter` (`status_filter`),
      KEY `idx_prod_id` (`prod_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='平台端-数据-商品洞察'
    """.trim
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
   * 写入DataFrame到MySQL表
   */
  def writeToMySQL(df: org.apache.spark.sql.DataFrame, tableName: String, statDate: String, timeRangeType: String): Unit = {
    try {

      // 检查并创建表
      ensureTableExists(tableName)

      // 根据时间范围类型执行不同的删除策略
      val deleteStrategy = timeRangeType match {
        case "7days" | "30days" =>
          // 7天和30天：删除全部数据
          "DELETE_ALL"
        case "thisMonth" =>
          // 自然月：只删除当前月份的数据
          import java.util.Calendar
          val cal = Calendar.getInstance()
          val year = cal.get(Calendar.YEAR)
          val month = f"${cal.get(Calendar.MONTH) + 1}%02d"
          s"DELETE_BY_MONTH:$year$month"
        case _ => "DELETE_ALL"
      }

      // 执行删除策略
      executeDeleteStrategy(tableName, deleteStrategy)

      println(s"开始写入数据到表 $tableName")
      println(s"DataFrame数据量: ${df.count()}条")

      // 写入数据，不再使用框架的deleteBeforeInsert
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, statDate, deleteBeforeInsert = false)

      println(s"$timeRangeType 数据写入完成")
    } catch {
      case e: Exception =>
        println(s"写入MySQL表 $tableName 时出错: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }

  /**
   * 处理指定时间范围的所有状态数据（参考MySQLProductComprehensiveStatsJob的做法）
   */
  def processTimeRangeAllStatuses(spark: SparkSession, timeRangeType: String): Unit = {
    // 处理状态0,1,2,3分别生成数据，然后合并
    val statusFilters = List(0, 1, 2, 3)
    var allDataFrames = List[org.apache.spark.sql.DataFrame]()

    for (statusFilter <- statusFilters) {
      try {
        println(s"处理状态过滤: $statusFilter")
        val df = processTimeRangeAndStatus(spark, timeRangeType, statusFilter)
        if (df.count() > 0) {
          allDataFrames = allDataFrames :+ df
          println(s"状态$statusFilter: ${df.count()}条数据")
        } else {
          println(s"状态$statusFilter: 无数据")
        }
      } catch {
        case e: Exception =>
          println(s"处理状态$statusFilter 时出错: ${e.getMessage}")
      }
    }

    // 合并所有DataFrame并写入
    if (allDataFrames.nonEmpty) {
      val combinedDF = allDataFrames.reduce(_.union(_))
      val totalCount = combinedDF.count()
      println(s"$timeRangeType 总计: $totalCount 条数据")

      // 计算时间范围用于显示和写入
      val (startTime, endTime, dateStr) = calculateTimeRange(timeRangeType)

      println(s"\n======= $timeRangeType 商品综合统计数据 (共 $totalCount 条) =======")
      
      // 缓存DataFrame避免重复计算
      combinedDF.cache()
      
      // 暂时注释掉show()避免资源问题，只显示数据量
      println(s"DataFrame数据量: ${combinedDF.count()}条")

      // 获取表名
      val tableName = getTableName(timeRangeType)
      writeToMySQL(combinedDF, tableName, dateStr, timeRangeType)
      
      // 释放缓存
      combinedDF.unpersist()
    }
  }

  /**
   * 处理指定时间范围和状态的数据（参考MySQLProductComprehensiveStatsJob的做法）
   */
  def processTimeRangeAndStatus(spark: SparkSession, timeRangeType: String, statusFilter: Int): org.apache.spark.sql.DataFrame = {
    try {
      // 计算时间范围
      val (startTime, endTime, dateStr) = calculateTimeRange(timeRangeType)

      println(s"分析时间范围: $startTime 至 $endTime")

      // 生成SQL查询
      val productStatsSQL = generateProductComprehensiveStatsSQL(startTime, endTime, timeRangeType, dateStr, statusFilter)

      // 执行查询，增加重试机制
      val productStatsDF = executeWithRetry(spark, productStatsSQL, maxRetries = 3)
      val totalCount = productStatsDF.count()

      productStatsDF

    } catch {
      case e: Exception =>
        println(s"处理时间范围 $timeRangeType-状态$statusFilter 时出错: ${e.getMessage}")
        e.printStackTrace()
        // 返回空DataFrame
        spark.emptyDataFrame
    }
  }

  /**
   * 计算时间范围（与MySQL版本完全一致）
   */
  def calculateTimeRange(timeRangeType: String): (String, String, String) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dateFormatNoDash = new SimpleDateFormat("yyyyMMdd") // 无横杠的日期格式
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = Calendar.getInstance()

    timeRangeType match {
      case "thisMonth" =>
        // 自然月：如果今天是1号，分析上个月完整数据；否则分析本月1号到昨天的数据
        val today = cal.getTime()
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

          val startTime = s"$prevMonthStart 00:00:00"
          val endTime = s"$prevMonthEnd 23:59:59"

          (startTime, endTime, dateFormatNoDash.format(cal.getTime()))
        } else {
          // 月中其他日期：分析本月1号到昨天的数据
          cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
          val yesterday = dateFormat.format(cal.getTime())
          val statDate = dateFormatNoDash.format(cal.getTime()) // stat_date是数据截止日期（昨天）

          cal.setTime(today) // 恢复到今天
          cal.set(Calendar.DAY_OF_MONTH, 1) // 设置到本月1号
          val monthStart = dateFormat.format(cal.getTime())

          val startTime = s"$monthStart 00:00:00"
          val endTime = s"$yesterday 23:59:59"

          (startTime, endTime, statDate) // 使用数据截止日期作为stat_date
        }

      case "7days" =>
        // 近7天：过去7天，以昨天为终止日期
        val todayStr = dateFormat.format(cal.getTime()) // 今天的日期（用于stat_date）
        cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
        val endDate = dateFormat.format(cal.getTime())
        cal.add(Calendar.DAY_OF_MONTH, -6) // 再往前6天，总共7天
        val startDate = dateFormat.format(cal.getTime())
        val startTime = s"$startDate 00:00:00"
        val endTime = s"$endDate 23:59:59"

        println(s"7天时间范围调试: $startDate 至 $endDate")

        (startTime, endTime, dateFormatNoDash.format(Calendar.getInstance().getTime())) // 数据范围到昨天，stat_date用今天无横杠格式

      case "30days" =>
        // 近30天：过去30天，以昨天为终止日期
        val todayStr = dateFormat.format(cal.getTime()) // 今天的日期（用于stat_date）
        cal.add(Calendar.DAY_OF_MONTH, -1) // 先到昨天
        val endDate = dateFormat.format(cal.getTime())
        cal.add(Calendar.DAY_OF_MONTH, -29) // 再往前29天，总共30天
        val startDate = dateFormat.format(cal.getTime())
        val startTime = s"$startDate 00:00:00"
        val endTime = s"$endDate 23:59:59"

        println(s"30天时间范围调试: $startDate 至 $endDate")

        (startTime, endTime, dateFormatNoDash.format(Calendar.getInstance().getTime())) // 数据范围到昨天，stat_date用今天无横杠格式

      case _ =>
        throw new IllegalArgumentException(s"不支持的时间范围类型: $timeRangeType")
    }
  }

  /**
   * 生成商品综合统计SQL（Hive版本）
   * 修正数据质量和避免笛卡尔积问题
   */
  def generateProductComprehensiveStatsSQL(startTime: String, endTime: String, timeRangeType: String, dateStr: String, statusFilter: Int): String = {

    // 计算曝光时间条件（参考HiveProductAnalysisQueryJob的逻辑）
    val exposureTimeCondition = {
      val startDate = startTime.split(" ")(0)  // 从 "yyyy-MM-dd HH:mm:ss" 中提取日期部分
      val endDate = endTime.split(" ")(0)
      s"dt >= '$startDate' AND dt <= '$endDate'"
    }

    s"""
    -- 先获取有业务数据的商品范围，避免全表扫描导致笛卡尔积
    WITH product_exposure AS (
        -- 商品曝光数据（使用新埋点，参考HiveProductAnalysisQueryJob逻辑）
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
    
    active_prod_ids AS (
        SELECT DISTINCT prod_id FROM (
            -- 曝光数据中的商品ID
            SELECT DISTINCT prod_id FROM product_exposure
            
            UNION
            
            -- 下单数据中的商品ID（按create_time过滤）
            SELECT DISTINCT oi.prod_id 
            FROM mall_bbc.t_dwd_order_item_full oi
            JOIN mall_bbc.t_dwd_order_full o ON oi.order_number = o.order_number
            WHERE o.create_time >= '$startTime' AND o.create_time <= '$endTime'
            
            UNION
            
            -- 支付数据中的商品ID（按pay_time过滤）
            SELECT DISTINCT oi.prod_id 
            FROM mall_bbc.t_dwd_order_item_full oi
            JOIN mall_bbc.t_dwd_order_full o ON oi.order_number = o.order_number
            WHERE o.pay_time >= '$startTime' AND o.pay_time <= '$endTime'
              AND o.is_payed = 1
              
            UNION
            
            -- 退款数据中的商品ID（按申请时间过滤）
            SELECT DISTINCT oi.prod_id
            FROM mall_bbc.t_dwd_order_refund_full r
            JOIN mall_bbc.t_dwd_order_item_full oi ON r.order_item_id = oi.order_item_id
            WHERE r.apply_time >= '$startTime' AND r.apply_time <= '$endTime'
            
            UNION
            
            SELECT DISTINCT oi.prod_id
            FROM mall_bbc.t_dwd_order_refund_full r
            JOIN mall_bbc.t_dwd_order_full o ON r.order_id = o.order_id
            JOIN mall_bbc.t_dwd_order_item_full oi ON o.order_number = oi.order_number
            WHERE r.apply_time >= '$startTime' AND r.apply_time <= '$endTime'
        ) all_prod_ids
    )

    SELECT 
        -- 按照正确的字段顺序
        0 AS id,                                                                 -- 主键ID
        p.prod_id,                                                               -- 商品ID
        CAST(p.price AS DECIMAL(18,2)) AS price,                                 -- 商品价格
        CAST(p.supplier_price AS DECIMAL(18,2)) AS supplier_price,               -- 供应商价格
        COALESCE(pe.expose_count, 0) AS expose,                                  -- 曝光次数
        COALESCE(pe.expose_person_num, 0) AS expose_person_num,               -- 曝光人数
        0 AS add_cart_person,                                                    -- 加购人数
        0 AS add_cart,                                                           -- 加购件数
        COALESCE(order_data.place_order_person, 0) AS place_order_person,        -- 下单人数
        COALESCE(order_data.pay_person, 0) AS pay_person,                       -- 支付人数
        COALESCE(order_data.place_order_num, 0) AS place_order_num,             -- 下单件数
        COALESCE(order_data.pay_num, 0) AS pay_num,                             -- 支付件数
        COALESCE(order_data.place_order_amount, 0) AS place_order_amount,       -- 下单金额
        COALESCE(order_data.pay_amount, 0) AS pay_amount,                       -- 支付金额
        CASE
            WHEN COALESCE(pe.expose_person_num, 0) > 0
            THEN ROUND(COALESCE(order_data.pay_person, 0) / COALESCE(pe.expose_person_num, 0) * 100, 2)
            ELSE 0.00
        END AS single_prod_rate,                                                 -- 转化率
        COALESCE(refund_data.refund_num, 0) AS refund_num,                       -- 申请退款订单数
        COALESCE(refund_data.refund_person, 0) AS refund_person,                 -- 申请退款人数
        COALESCE(refund_data.refund_success_num, 0) AS refund_success_num,       -- 成功退款订单数
        COALESCE(refund_data.refund_success_person, 0) AS refund_success_person, -- 成功退款人数
        COALESCE(refund_data.refund_success_amount, 0) AS refund_success_amount, -- 成功退款金额
        CASE 
            WHEN COALESCE(refund_data.refund_num, 0) > 0 
            THEN ROUND(COALESCE(refund_data.refund_success_num, 0) / COALESCE(refund_data.refund_num, 0) * 100, 2)
            ELSE 0.00 
        END AS refund_success_rate,                                              -- 退款成功率
        CAST(p.status AS INT) AS status,                                         -- 商品状态
        CASE 
            WHEN p.status = -1 THEN '删除'
            WHEN p.status = 0 THEN '商家下架'
            WHEN p.status = 1 THEN '上架'
            WHEN p.status = 2 THEN '平台下架'
            WHEN p.status = 3 THEN '违规下架待审核'
            WHEN p.status = 6 THEN '待审核'
            WHEN p.status = 7 THEN '草稿状态'
            ELSE ''
        END AS prod_status,                                                      -- 商品状态描述
        '$statusFilter' AS status_filter,                                        -- 状态过滤器
        '$dateStr' AS stat_date,                                                 -- 统计日期（使用正确的dateStr格式）
        p.prod_name,                                                             -- 商品名称
        COALESCE(p.pic, '') AS prod_url,                                         -- 商品图片URL
        COALESCE(sd.shop_name, '') AS shop_name                                  -- 店铺名称

    FROM (
        -- 只取每个商品的最新记录，避免重复
        SELECT
          prod_id,
          prod_name,
          price,
          supplier_price,
          status,
          shop_id,
          pic
        FROM (
          SELECT
            prod_id,
            prod_name,
            price,
            supplier_price,
            status,
            shop_id,
            pic,
            ROW_NUMBER() OVER (PARTITION BY prod_id ORDER BY update_time DESC) as rn
          FROM mall_bbc.t_dwd_prod_full
        ) t
        WHERE rn = 1
    ) p
    
    -- 只查询有业务数据的商品，避免笛卡尔积
    INNER JOIN active_prod_ids api ON p.prod_id = api.prod_id
    
    -- 店铺信息LEFT JOIN（优化查询结构）
    LEFT JOIN (
        SELECT 
            shop_id, 
            shop_name
        FROM (
            SELECT
                CAST(shop_id AS BIGINT) AS shop_id,
                shop_name,
                ROW_NUMBER() OVER (PARTITION BY CAST(shop_id AS BIGINT) ORDER BY shop_name ASC) as rn
            FROM mall_bbc.t_ods_tz_shop_detail
            WHERE shop_name IS NOT NULL
              AND shop_name != ''
              AND TRIM(shop_name) != ''
        ) ranked_shop
        WHERE rn = 1
    ) sd ON CAST(p.shop_id AS BIGINT) = CAST(sd.shop_id AS BIGINT)
    
    -- 订单数据：修复支付逻辑，与其他类保持一致
    LEFT JOIN (
        SELECT
          oi.prod_id,
          COUNT(DISTINCT o.user_id) AS place_order_person,
          COUNT(DISTINCT CASE WHEN o.is_payed = 1 THEN o.user_id END) AS pay_person,
          SUM(oi.prod_count) AS place_order_num,
          SUM(CASE WHEN o.is_payed = 1 THEN oi.prod_count ELSE 0 END) AS pay_num,
          SUM(oi.actual_total) AS place_order_amount,
          SUM(CASE WHEN o.is_payed = 1 THEN oi.actual_total ELSE 0 END) AS pay_amount
        FROM mall_bbc.t_dwd_order_item_full oi
        JOIN mall_bbc.t_dwd_order_full o ON oi.order_number = o.order_number
        WHERE o.create_time >= '$startTime' AND o.create_time <= '$endTime'
        GROUP BY oi.prod_id
    ) order_data ON p.prod_id = order_data.prod_id

    -- 退款数据：基于申请时间和成功时间统计（参考MySQL版本逻辑）
    LEFT JOIN (
        SELECT 
            prod_id,
            COUNT(*) AS refund_num,
            COUNT(DISTINCT user_id) AS refund_person,
            COUNT(CASE WHEN return_money_sts = 5 THEN 1 END) AS refund_success_num,
            COUNT(DISTINCT CASE WHEN return_money_sts = 5 THEN user_id END) AS refund_success_person,
            SUM(CASE WHEN return_money_sts = 5 THEN refund_amount ELSE 0 END) AS refund_success_amount
        FROM (
            -- 方式1：通过order_item_id关联
            SELECT 
                oi.prod_id,
                r.refund_id,
                r.user_id,
                r.return_money_sts,
                oi.actual_total AS refund_amount
            FROM mall_bbc.t_dwd_order_refund_full r
            JOIN mall_bbc.t_dwd_order_item_full oi ON r.order_item_id = oi.order_item_id
            WHERE r.refund_type = 2 
              AND r.apply_time >= '$startTime' 
              AND r.apply_time <= '$endTime'
            
            UNION ALL
            
            -- 方式2：通过order_id关联
            SELECT 
                oi.prod_id,
                r.refund_id,
                r.user_id,
                r.return_money_sts,
                oi.actual_total AS refund_amount
            FROM mall_bbc.t_dwd_order_refund_full r
            JOIN mall_bbc.t_dwd_order_full o ON r.order_id = o.order_id
            JOIN mall_bbc.t_dwd_order_item_full oi ON o.order_number = oi.order_number
            WHERE r.refund_type = 1 
              AND r.apply_time >= '$startTime' 
              AND r.apply_time <= '$endTime'
        ) refund_detail
        GROUP BY prod_id
    ) refund_data ON p.prod_id = refund_data.prod_id
    
    -- 曝光数据LEFT JOIN
    LEFT JOIN product_exposure pe ON p.prod_id = pe.prod_id AND p.shop_id = pe.shop_id

    WHERE (
            ($statusFilter = 0 AND p.status > -1) OR  -- 全部商品（除删除外）
            ($statusFilter = 1 AND p.status = 1) OR   -- 出售中
            ($statusFilter = 2 AND p.status = 0) OR   -- 仓库中
            ($statusFilter = 3 AND p.status = 3)      -- 已售空
        )
      AND p.status IS NOT NULL
    ORDER BY CAST('$statusFilter' AS INT), COALESCE(pe.expose_count, 0) DESC
    """
  }


  /**
   * 获取今天的日期字符串
   */
  def getTodayDate(): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(Calendar.getInstance().getTime())
  }

  /**
   * 获取当前时间字符串
   */
  def getCurrentTime(): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(Calendar.getInstance().getTime())
  }
}