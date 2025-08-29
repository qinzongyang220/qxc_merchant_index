package bo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import dao.MyHive
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Hive实时商品综合统计查询作业
 * 处理今天实时数据，只使用DWD Inc表（不使用full表）
 * 参考HiveProductComprehensiveStatsJob的模式和结构
 * 时间范围：读取今天的数据
 * 写入表名：tz_bd_platform_product_analysis_0
 */
object HiveRealtimeProductComprehensiveStatsJob {

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "HiveRealtimeProductComprehensiveStats"
    val spark: SparkSession = MyHive.conn

    try {
      // 设置Spark配置，增加显示字段数和连接稳定性
      spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
      // 设置Hive分区管理配置，解决MetaStore分区查询问题
      spark.conf.set("spark.sql.hive.manageFilesourcePartitions", "false")
      // 设置Hive连接相关配置，提高稳定性
      spark.conf.set("spark.sql.adaptive.enabled", "false")
      spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
      spark.conf.set("hive.metastore.client.socket.timeout", "300")
      spark.conf.set("hive.metastore.connect.retries", "3")
      
      println("成功连接到Hive")
      println("已设置Hive分区管理配置: spark.sql.hive.manageFilesourcePartitions=false")
      
      // 使用mall_bbc数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")
      
      // 刷新表缓存，解决文件不存在的问题（只刷新inc表，用于今天实时数据）
      try {
        spark.sql("REFRESH TABLE t_dwd_order_inc")
        spark.sql("REFRESH TABLE t_dwd_order_refund_inc") 
        spark.sql("REFRESH TABLE t_dwd_order_item_inc")
        spark.sql("REFRESH TABLE t_dwd_prod_inc")
        spark.sql("REFRESH TABLE t_dwd_shop_detail_inc")
      } catch {
        case e: Exception => println(s"刷新表缓存时出现警告: ${e.getMessage}")
      }
      
      // 定义状态分区
      val statusFilters = List(0, 1, 2, 3) // 0:全部, 1:出售中, 2:仓库中, 3:已售空
      
      println("开始执行Hive实时商品综合统计查询...")
      println(s"状态分区: ${statusFilters.mkString(", ")}")
      
      // 显示时间范围说明
      displayTimeRanges()
      
      // 处理实时数据
      processRealtimeData(spark, statusFilters)
      
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
   * 显示时间范围说明（跨天处理逻辑）
   */
  def displayTimeRanges(): Unit = {
    val (startTime, endTime, processDate, currentHour) = getTimeRange()
    
    println("\n======= 时间范围说明 =======")
    println(s"实时数据 (跨天处理): $startTime 至 $endTime")
    println(s"处理日期: $processDate (当前小时: $currentHour)")
    println("==========================\n")
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
   * 写入DataFrame到MySQL表（实时数据，每次全量替换）
   */
  def writeToMySQL(df: DataFrame, tableName: String, statDate: String): Unit = {
    try {
      println(s"开始写入实时数据到表 $tableName")
      println(s"DataFrame数据量: ${df.count()}条")
      
      // 实时数据使用全量替换模式
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, statDate, deleteBeforeInsert = true)
      
      println("实时数据写入完成")
    } catch {
      case e: Exception =>
        println(s"写入MySQL表 $tableName 时出错: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }

  /**
   * 处理实时数据（所有状态）
   */
  def processRealtimeData(spark: SparkSession, statusFilters: List[Int]): Unit = {
    try {
      // 获取跨天处理的时间范围
      val (startTime, endTime, processDate, currentHour) = getTimeRange()
      
      println(s"分析实时数据时间范围: $startTime 至 $endTime")
      println(s"处理日期: $processDate (当前小时: $currentHour)")
      
      // 修复：一次性查询所有数据，避免重复计算订单数据
      val allStatusSQL = generateRealtimeProductComprehensiveStatsSQLForAllStatus(startTime, endTime, processDate, statusFilters)
      
      // 执行查询，增加重试机制
      val allProductStatsDF = executeWithRetry(spark, allStatusSQL, maxRetries = 3)
      val totalCount = allProductStatsDF.count()
      
      if (totalCount > 0) {
        // 过滤掉所有关键指标都为0的记录（除了曝光）
        val filteredDF = filterNonZeroData(allProductStatsDF)
        val filteredCount = filteredDF.count()
        
        println(s"原始数据: $totalCount 条，过滤后: $filteredCount 条")
        
        if (filteredCount > 0) {
          println(s"\n======= 实时商品综合统计数据 (共 $filteredCount 条) =======")
          filteredDF.show(20, false)
          
          // 写入MySQL（实时数据写入固定表名）
          writeToMySQL(filteredDF, "tz_bd_platform_product_analysis_0", processDate)
        } else {
          println("过滤后无数据")
        }
      } else {
        println("无实时数据")
      }
      
    } catch {
      case e: Exception => 
        println(s"处理实时数据时出错: ${e.getMessage}")
        e.printStackTrace()
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
   * 获取当前时间
   */
  def getCurrentDateTime(): String = {
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    timeFormat.format(Calendar.getInstance().getTime())
  }
  
  /**
   * 过滤掉所有关键指标都为0的记录（除了曝光）
   * 参考MySQLProductComprehensiveStatsJob的filterNonZeroData函数
   */
  def filterNonZeroData(df: DataFrame): DataFrame = {
    df.filter(
      col("place_order_person") > 0 || 
      col("place_order_num") > 0 || 
      col("place_order_amount") > 0 || 
      col("pay_person") > 0 ||
      col("pay_num") > 0 || 
      col("pay_amount") > 0 || 
      col("refund_num") > 0 || 
      col("refund_success_amount") > 0 ||
      col("add_cart_person") > 0 || 
      col("add_cart") > 0
    )
  }
  
  /**
   * 生成实时商品综合统计SQL - 一次性处理所有状态（修复重复计算问题）
   */
  def generateRealtimeProductComprehensiveStatsSQLForAllStatus(startTime: String, endTime: String, dateStr: String, statusFilters: List[Int]): String = {
    
    s"""
    -- 修复版本：一次性查询所有状态，避免订单数据重复计算
    WITH active_prod_ids AS (
        SELECT DISTINCT prod_id FROM (
            -- 下单数据中的商品ID（今天数据用dwd_inc表）
            SELECT DISTINCT CAST(oi.prod_id AS BIGINT) AS prod_id
            FROM mall_bbc.t_dwd_order_item_inc oi
            JOIN mall_bbc.t_dwd_order_inc o ON oi.order_number = o.order_number
            WHERE o.create_time >= '$startTime' 
              AND o.create_time <= '$endTime'
              AND o.dt = '$dateStr'
              AND oi.dt = '$dateStr'
            
            UNION
            
            -- 支付数据中的商品ID（今天数据用dwd_inc表）
            SELECT DISTINCT CAST(oi.prod_id AS BIGINT) AS prod_id
            FROM mall_bbc.t_dwd_order_item_inc oi
            JOIN mall_bbc.t_dwd_order_inc o ON oi.order_number = o.order_number
            WHERE o.pay_time >= '$startTime' 
              AND o.pay_time <= '$endTime'
              AND o.is_payed = 1
              AND o.dt = '$dateStr'
              AND oi.dt = '$dateStr'
            
            UNION
            
            -- 退款数据中的商品ID（今天数据）
            SELECT DISTINCT CAST(oi.prod_id AS BIGINT) AS prod_id
            FROM mall_bbc.t_dwd_order_refund_inc r
            JOIN mall_bbc.t_dwd_order_item_inc oi ON r.order_item_id = oi.order_item_id
            WHERE r.apply_time >= '$startTime' 
              AND r.apply_time < '$endTime'
              AND r.dt = '$dateStr'
              AND oi.dt = '$dateStr'
            
            UNION
            
            SELECT DISTINCT CAST(oi.prod_id AS BIGINT) AS prod_id
            FROM mall_bbc.t_dwd_order_refund_inc r
            JOIN mall_bbc.t_dwd_order_inc o ON r.order_id = o.order_id
            JOIN mall_bbc.t_dwd_order_item_inc oi ON o.order_number = oi.order_number
            WHERE r.apply_time >= '$startTime' 
              AND r.apply_time < '$endTime'
              AND r.dt = '$dateStr'
              AND o.dt = '$dateStr'
              AND oi.dt = '$dateStr'
        ) all_prod_ids
    ),

    -- 曝光数据（新埋点）- 简化时间过滤，只使dt分区
    exposure_data AS (
        SELECT 
            CAST(prodid AS BIGINT) AS prod_id,
            COUNT(*) AS expose,
            COUNT(DISTINCT cid) AS expose_person_num
        FROM user_tag.t_ods_app_logdata
        WHERE dt = '$dateStr'
          AND action = 'enter'
          AND page_id = '1005'
          AND prodid IS NOT NULL
          AND prodid != ''
        GROUP BY CAST(prodid AS BIGINT)
    ),

    -- 订单数据（修复支付逻辑，与MySQL保持一致）- 只计算一次
    order_data AS (
        SELECT 
            CAST(oi.prod_id AS BIGINT) AS prod_id,
            COUNT(DISTINCT o.user_id) AS place_order_person,
            COUNT(DISTINCT CASE WHEN o.is_payed = 1 THEN o.user_id END) AS pay_person,
            SUM(oi.prod_count) AS place_order_num,
            SUM(CASE WHEN o.is_payed = 1 THEN oi.prod_count ELSE 0 END) AS pay_num,
            SUM(oi.actual_total) AS place_order_amount,
            SUM(CASE WHEN o.is_payed = 1 THEN oi.actual_total ELSE 0 END) AS pay_amount
        FROM mall_bbc.t_dwd_order_item_inc oi
        JOIN mall_bbc.t_dwd_order_inc o ON oi.order_number = o.order_number
        WHERE o.create_time >= '$startTime' 
          AND o.create_time <= '$endTime'
          AND o.dt = '$dateStr'
          AND oi.dt = '$dateStr'
        GROUP BY CAST(oi.prod_id AS BIGINT)
    ),

    -- 退款数据（今天实时数据：使用DWD Inc表）
    refund_data AS (
        SELECT
            prod_id,
            COUNT(DISTINCT refund_id) AS refund_num,
            COUNT(DISTINCT user_id) AS refund_person,
            COUNT(DISTINCT CASE WHEN return_money_sts = '5' THEN refund_id END) AS refund_success_num,
            COUNT(DISTINCT CASE WHEN return_money_sts = '5' THEN user_id END) AS refund_success_person,
            SUM(CASE WHEN return_money_sts = '5' THEN refund_amount ELSE 0 END) AS refund_success_amount
        FROM (
            SELECT 
                CAST(oi.prod_id AS BIGINT) AS prod_id,
                r.refund_id,
                r.user_id,
                r.return_money_sts,
                oi.actual_total AS refund_amount
            FROM mall_bbc.t_dwd_order_refund_inc r
            JOIN mall_bbc.t_dwd_order_item_inc oi ON r.order_item_id = oi.order_item_id
            WHERE r.refund_type = '2' 
              AND r.apply_time >= '$startTime' 
              AND r.apply_time < '$endTime'
              AND r.dt = '$dateStr'
              AND oi.dt = '$dateStr'
            
            UNION ALL
            
            SELECT 
                CAST(oi.prod_id AS BIGINT) AS prod_id,
                r.refund_id,
                r.user_id,
                r.return_money_sts,
                oi.actual_total AS refund_amount
            FROM mall_bbc.t_dwd_order_refund_inc r
            JOIN mall_bbc.t_dwd_order_inc o ON r.order_id = o.order_id
            JOIN mall_bbc.t_dwd_order_item_inc oi ON o.order_number = oi.order_number
            WHERE r.refund_type = '1' 
              AND r.apply_time >= '$startTime' 
              AND r.apply_time < '$endTime'
              AND r.dt = '$dateStr'
              AND o.dt = '$dateStr'
              AND oi.dt = '$dateStr'
        ) ra
        GROUP BY prod_id
    ),

    -- 商品状态展开：为每个状态生成一条记录
    status_expanded AS (
        SELECT 
            p.prod_id,
            p.prod_name,
            p.price,
            p.supplier_price,
            p.status,
            p.shop_id,
            p.pic,
            sf.status_filter,
            CASE 
                WHEN sf.status_filter = 0 AND p.status > -1 THEN 1  -- 全部商品（除删除外）
                WHEN sf.status_filter = 1 AND p.status = 1 THEN 1   -- 出售中
                WHEN sf.status_filter = 2 AND p.status = 0 THEN 1   -- 仓库中
                WHEN sf.status_filter = 3 AND p.status = 3 THEN 1   -- 已售空
                ELSE 0
            END AS status_match
        FROM (
            -- 优先使用full表，inc表作为补充
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
                CAST(prod_id AS BIGINT) AS prod_id,
                prod_name,
                price,
                supplier_price,
                status,
                shop_id,
                pic,
                ROW_NUMBER() OVER (PARTITION BY CAST(prod_id AS BIGINT) ORDER BY priority ASC) as rn
              FROM (
                -- 先从full表获取商品信息
                SELECT
                  prod_id,
                  prod_name,
                  price,
                  supplier_price,
                  status,
                  shop_id,
                  pic,
                  1 as priority
                FROM mall_bbc.t_dwd_prod_full
                WHERE status > -1
                
                UNION ALL
                
                -- 再从inc表获取今天更新的商品信息
                SELECT
                  prod_id,
                  prod_name,
                  price,
                  supplier_price,
                  status,
                  shop_id,
                  pic,
                  2 as priority
                FROM mall_bbc.t_dwd_prod_inc
                WHERE status > -1
                  AND dt = '$dateStr'
              ) all_prod
            ) ranked_prod
            WHERE rn = 1
        ) p
        -- 只查询有数据的商品
        JOIN active_prod_ids api ON p.prod_id = api.prod_id
        -- 交叉联接状态过滤器
        CROSS JOIN (
            ${statusFilters.map(sf => s"SELECT $sf as status_filter").mkString(" UNION ALL ")}
        ) sf
        WHERE (
            (sf.status_filter = 0 AND p.status > -1) OR  -- 全部商品（除删除外）
            (sf.status_filter = 1 AND p.status = 1) OR   -- 出售中
            (sf.status_filter = 2 AND p.status = 0) OR   -- 仓库中
            (sf.status_filter = 3 AND p.status = 3)      -- 已售空
        )
    )

    SELECT 
        se.prod_id,
        CAST(se.price AS DECIMAL(18,2)) AS price,
        CAST(se.supplier_price AS DECIMAL(18,2)) AS supplier_price,
        
        -- 曝光指标
        COALESCE(exposure.expose, 0) AS expose,
        COALESCE(exposure.expose_person_num, 0) AS expose_person_num,
        
        -- 加购指标
        0 AS add_cart_person,
        0 AS add_cart,
        
        -- 订单指标（每个商品只计算一次）
        COALESCE(order_data.place_order_person, 0) AS place_order_person,
        COALESCE(order_data.pay_person, 0) AS pay_person,
        COALESCE(order_data.place_order_num, 0) AS place_order_num,
        COALESCE(order_data.pay_num, 0) AS pay_num,
        COALESCE(order_data.place_order_amount, 0) AS place_order_amount,
        COALESCE(order_data.pay_amount, 0) AS pay_amount,
        
        -- 转化率指标
        CASE 
            WHEN COALESCE(exposure.expose_person_num, 0) > 0 
            THEN ROUND(COALESCE(order_data.pay_person, 0) / COALESCE(exposure.expose_person_num, 0) * 100, 2)
            ELSE 0 
        END AS single_prod_rate,
        
        -- 退款指标
        COALESCE(refund_data.refund_num, 0) AS refund_num,
        COALESCE(refund_data.refund_person, 0) AS refund_person,
        COALESCE(refund_data.refund_success_num, 0) AS refund_success_num,
        COALESCE(refund_data.refund_success_person, 0) AS refund_success_person,
        COALESCE(refund_data.refund_success_amount, 0) AS refund_success_amount,
        CASE 
            WHEN COALESCE(refund_data.refund_num, 0) > 0 
            THEN ROUND(COALESCE(refund_data.refund_success_num, 0) / COALESCE(refund_data.refund_num, 0) * 100, 2)
            ELSE 0 
        END AS refund_success_rate,
        
        -- 商品状态和过滤
        CAST(se.status AS INT) AS status,
        CASE 
            WHEN se.status = '-1' THEN '删除'
            WHEN se.status = '0' THEN '商家下架'
            WHEN se.status = '1' THEN '上架'
            WHEN se.status = '2' THEN '平台下架'
            WHEN se.status = '3' THEN '违规下架待审核'
            WHEN se.status = '6' THEN '待审核'
            WHEN se.status = '7' THEN '草稿状态'
            ELSE ''
        END AS prod_status,
        CAST(se.status_filter AS STRING) AS status_filter,
        
        -- 维度和时间
        '$dateStr' AS stat_date,
        
        -- 商品信息
        se.prod_name,
        se.pic AS prod_url,
        sd.shop_name

    FROM status_expanded se
    
    LEFT JOIN (
        -- 优先使用full表，inc表作为补充
        SELECT 
            shop_id, 
            shop_name
        FROM (
            SELECT
                CAST(shop_id AS BIGINT) AS shop_id,
                shop_name,
                ROW_NUMBER() OVER (PARTITION BY CAST(shop_id AS BIGINT) ORDER BY priority ASC) as rn
            FROM (
                -- 先从full表获取店铺信息
                SELECT
                    shop_id,
                    shop_name,
                    1 as priority
                FROM mall_bbc.t_dwd_shop_detail_full
                WHERE shop_name IS NOT NULL
                  AND shop_name != ''
                  AND TRIM(shop_name) != ''
                
                UNION ALL
                
                -- 再从inc表获取今天更新的店铺信息
                SELECT
                    shop_id,
                    shop_name,
                    2 as priority
                FROM mall_bbc.t_dwd_shop_detail_inc
                WHERE dt = '$dateStr'
                  AND shop_name IS NOT NULL
                  AND shop_name != ''
                  AND TRIM(shop_name) != ''
            ) all_shop
        ) ranked_shop
        WHERE rn = 1
    ) sd ON CAST(se.shop_id AS BIGINT) = sd.shop_id
    LEFT JOIN exposure_data exposure ON se.prod_id = exposure.prod_id
    LEFT JOIN order_data ON se.prod_id = order_data.prod_id
    LEFT JOIN refund_data ON se.prod_id = refund_data.prod_id
    
    WHERE (COALESCE(order_data.place_order_person, 0) > 0
        OR COALESCE(order_data.pay_person, 0) > 0
        OR COALESCE(refund_data.refund_num, 0) > 0)
    
    ORDER BY se.status_filter ASC, COALESCE(exposure.expose, 0) DESC
    """
  }

  /**
   * 生成实时商品综合统计SQL（使用dwd_inc表）- 保留原方法作为备用
   */
  def generateRealtimeProductComprehensiveStatsSQL(startTime: String, endTime: String, dateStr: String, statusFilter: Int): String = {
    
    // 根据状态过滤条件生成WHERE子句
    val statusCondition = statusFilter match {
      case 0 => "p.status > -1"  // 全部商品（除删除外）
      case 1 => "p.status = 1"   // 出售中
      case 2 => "p.status = 0"   // 仓库中
      case 3 => "p.status = 3"   // 已售空
      case _ => "p.status > -1"
    }
    
    s"""
    -- 优化的实现：先获取有数据的商品ID，再查询商品信息
    WITH active_prod_ids AS (
        SELECT DISTINCT prod_id FROM (
            -- 曝光数据中的商品ID（新埋点）
            SELECT CAST(prodid AS BIGINT) AS prod_id
            FROM user_tag.t_ods_app_logdata
            WHERE dt = '$dateStr'
              AND action = 'enter'
              AND page_id = '1005'
              AND prodid IS NOT NULL
            
            UNION
            
            -- 下单数据中的商品ID（今天数据用dwd_inc表）
            SELECT DISTINCT oi.prod_id FROM mall_bbc.t_dwd_order_item_inc oi
            JOIN mall_bbc.t_dwd_order_inc o ON oi.order_number = o.order_number
            WHERE o.create_time >= '$startTime' 
              AND o.create_time <= '$endTime'
              AND o.dt = '$dateStr'
              AND oi.dt = '$dateStr'
            
            UNION
            
            -- 支付数据中的商品ID（今天数据用dwd_inc表）
            SELECT DISTINCT oi.prod_id FROM mall_bbc.t_dwd_order_item_inc oi
            JOIN mall_bbc.t_dwd_order_inc o ON oi.order_number = o.order_number
            WHERE o.pay_time >= '$startTime' 
              AND o.pay_time <= '$endTime'
              AND o.is_payed = 1
              AND o.dt = '$dateStr'
              AND oi.dt = '$dateStr'
            
            UNION
            
            -- 退款数据中的商品ID（今天数据）
            SELECT DISTINCT oi.prod_id FROM mall_bbc.t_dwd_order_refund_inc r
            JOIN mall_bbc.t_dwd_order_item_inc oi ON r.order_item_id = oi.order_item_id
            WHERE r.apply_time >= '$startTime' 
              AND r.apply_time < '$endTime'
              AND r.dt = '$dateStr'
              AND oi.dt = '$dateStr'
            
            UNION
            
            SELECT DISTINCT oi.prod_id FROM mall_bbc.t_dwd_order_refund_inc r
            JOIN mall_bbc.t_dwd_order_inc o ON r.order_id = o.order_id
            JOIN mall_bbc.t_dwd_order_item_inc oi ON o.order_number = oi.order_number
            WHERE r.apply_time >= '$startTime' 
              AND r.apply_time < '$endTime'
              AND r.dt = '$dateStr'
              AND o.dt = '$dateStr'
              AND oi.dt = '$dateStr'
        ) all_prod_ids
    ),

    -- 曝光数据（新埋点）- 简化时间过滤，只使dt分区
    exposure_data AS (
        SELECT 
            CAST(prodid AS BIGINT) AS prod_id,
            COUNT(*) AS expose,
            COUNT(DISTINCT cid) AS expose_person_num
        FROM user_tag.t_ods_app_logdata
        WHERE dt = '$dateStr'
          AND action = 'enter'
          AND page_id = '1005'
          AND prodid IS NOT NULL
          AND prodid != ''
        GROUP BY CAST(prodid AS BIGINT)
    ),

    -- 订单数据（修复支付逻辑，与MySQL保持一致）
    order_data AS (
        SELECT 
            oi.prod_id,
            COUNT(DISTINCT o.user_id) AS place_order_person,
            COUNT(DISTINCT CASE WHEN o.is_payed = 1 THEN o.user_id END) AS pay_person,
            SUM(oi.prod_count) AS place_order_num,
            SUM(CASE WHEN o.is_payed = 1 THEN oi.prod_count ELSE 0 END) AS pay_num,
            SUM(oi.actual_total) AS place_order_amount,
            SUM(CASE WHEN o.is_payed = 1 THEN oi.actual_total ELSE 0 END) AS pay_amount
        FROM mall_bbc.t_dwd_order_item_inc oi
        JOIN mall_bbc.t_dwd_order_inc o ON oi.order_number = o.order_number
        WHERE o.create_time >= '$startTime' 
          AND o.create_time <= '$endTime'
          AND o.dt = '$dateStr'
          AND oi.dt = '$dateStr'
        GROUP BY oi.prod_id
    ),

    -- 退款数据（今天实时数据：使用DWD Inc表）
    refund_data AS (
        SELECT
            prod_id,
            COUNT(DISTINCT refund_id) AS refund_num,
            COUNT(DISTINCT user_id) AS refund_person,
            COUNT(DISTINCT CASE WHEN return_money_sts = '5' THEN refund_id END) AS refund_success_num,
            COUNT(DISTINCT CASE WHEN return_money_sts = '5' THEN user_id END) AS refund_success_person,
            SUM(CASE WHEN return_money_sts = '5' THEN refund_amount ELSE 0 END) AS refund_success_amount
        FROM (
            SELECT 
                oi.prod_id,
                r.refund_id,
                r.user_id,
                r.return_money_sts,
                oi.actual_total AS refund_amount
            FROM mall_bbc.t_dwd_order_refund_inc r
            JOIN mall_bbc.t_dwd_order_item_inc oi ON r.order_item_id = oi.order_item_id
            WHERE r.refund_type = '2' 
              AND r.apply_time >= '$startTime' 
              AND r.apply_time < '$endTime'
              AND r.dt = '$dateStr'
              AND oi.dt = '$dateStr'
            
            UNION ALL
            
            SELECT 
                oi.prod_id,
                r.refund_id,
                r.user_id,
                r.return_money_sts,
                oi.actual_total AS refund_amount
            FROM mall_bbc.t_dwd_order_refund_inc r
            JOIN mall_bbc.t_dwd_order_inc o ON r.order_id = o.order_id
            JOIN mall_bbc.t_dwd_order_item_inc oi ON o.order_number = oi.order_number
            WHERE r.refund_type = '1' 
              AND r.apply_time >= '$startTime' 
              AND r.apply_time < '$endTime'
              AND r.dt = '$dateStr'
              AND o.dt = '$dateStr'
              AND oi.dt = '$dateStr'
        ) ra
        GROUP BY prod_id
    )

    SELECT 
        p.prod_id,
        CAST(p.price AS DECIMAL(18,2)) AS price,
        CAST(p.supplier_price AS DECIMAL(18,2)) AS supplier_price,
        
        -- 曝光指标
        COALESCE(exposure.expose, 0) AS expose,                                  -- 曝光次数
        COALESCE(exposure.expose_person_num, 0) AS expose_person_num,            -- 曝光人数
        
        -- 加购指标
        0 AS add_cart_person,                                                    -- 加购人数（默认值）
        0 AS add_cart,                                                           -- 加购件数（默认值）
        
        -- 订单指标
        COALESCE(order_data.place_order_person, 0) AS place_order_person,        -- 下单人数
        COALESCE(order_data.pay_person, 0) AS pay_person,                        -- 支付人数
        COALESCE(order_data.place_order_num, 0) AS place_order_num,              -- 下单件数
        COALESCE(order_data.pay_num, 0) AS pay_num,                              -- 支付件数
        COALESCE(order_data.place_order_amount, 0) AS place_order_amount,        -- 下单金额
        COALESCE(order_data.pay_amount, 0) AS pay_amount,                        -- 支付金额
        
        -- 转化率指标
        CASE 
            WHEN COALESCE(exposure.expose_person_num, 0) > 0 
            THEN ROUND(COALESCE(order_data.pay_person, 0) / COALESCE(exposure.expose_person_num, 0) * 100, 2)
            ELSE 0 
        END AS single_prod_rate,
        
        -- 退款指标
        COALESCE(refund_data.refund_num, 0) AS refund_num,                       -- 申请退款订单数
        COALESCE(refund_data.refund_person, 0) AS refund_person,                 -- 申请退款人数
        COALESCE(refund_data.refund_success_num, 0) AS refund_success_num,       -- 成功退款订单数
        COALESCE(refund_data.refund_success_person, 0) AS refund_success_person, -- 成功退款人数
        COALESCE(refund_data.refund_success_amount, 0) AS refund_success_amount, -- 成功退款金额
        CASE 
            WHEN COALESCE(refund_data.refund_num, 0) > 0 
            THEN ROUND(COALESCE(refund_data.refund_success_num, 0) / COALESCE(refund_data.refund_num, 0) * 100, 2)
            ELSE 0 
        END AS refund_success_rate,
        
        -- 商品状态和过滤
        CAST(p.status AS INT) AS status,
        CASE 
            WHEN p.status = '-1' THEN '删除'
            WHEN p.status = '0' THEN '商家下架'
            WHEN p.status = '1' THEN '上架'
            WHEN p.status = '2' THEN '平台下架'
            WHEN p.status = '3' THEN '违规下架待审核'
            WHEN p.status = '6' THEN '待审核'
            WHEN p.status = '7' THEN '草稿状态'
            ELSE ''
        END AS prod_status,
        '$statusFilter' AS status_filter,
        
        -- 维度和时间
        '$dateStr' AS stat_date,
        
        -- 商品信息
        p.prod_name,
        p.pic AS prod_url,
        sd.shop_name

    FROM (
        -- 优先使用full表，inc表作为补充
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
            CAST(prod_id AS BIGINT) AS prod_id,
            prod_name,
            price,
            supplier_price,
            status,
            shop_id,
            pic,
            ROW_NUMBER() OVER (PARTITION BY CAST(prod_id AS BIGINT) ORDER BY priority ASC) as rn
          FROM (
            -- 先从full表获取商品信息
            SELECT
              prod_id,
              prod_name,
              price,
              supplier_price,
              status,
              shop_id,
              pic,
              1 as priority
            FROM mall_bbc.t_dwd_prod_full
            WHERE status > -1
            
            UNION ALL
            
            -- 再从inc表获取今天更新的商品信息
            SELECT
              prod_id,
              prod_name,
              price,
              supplier_price,
              status,
              shop_id,
              pic,
              2 as priority
            FROM mall_bbc.t_dwd_prod_inc
            WHERE status > -1
              AND dt = '$dateStr'
          ) all_prod
        ) ranked_prod
        WHERE rn = 1
    ) p
    
    -- 只查询有数据的商品
    JOIN active_prod_ids api ON p.prod_id = api.prod_id
    
    LEFT JOIN (
        -- 优先使用full表，inc表作为补充
        SELECT 
            shop_id, 
            shop_name
        FROM (
            SELECT
                CAST(shop_id AS BIGINT) AS shop_id,
                shop_name,
                ROW_NUMBER() OVER (PARTITION BY CAST(shop_id AS BIGINT) ORDER BY priority ASC) as rn
            FROM (
                -- 先从full表获取店铺信息
                SELECT
                    shop_id,
                    shop_name,
                    1 as priority
                FROM mall_bbc.t_dwd_shop_detail_full
                WHERE shop_name IS NOT NULL
                  AND shop_name != ''
                  AND TRIM(shop_name) != ''
                
                UNION ALL
                
                -- 再从inc表获取今天更新的店铺信息
                SELECT
                    shop_id,
                    shop_name,
                    2 as priority
                FROM mall_bbc.t_dwd_shop_detail_inc
                WHERE dt = '$dateStr'
                  AND shop_name IS NOT NULL
                  AND shop_name != ''
                  AND TRIM(shop_name) != ''
            ) all_shop
        ) ranked_shop
        WHERE rn = 1
    ) sd ON CAST(p.shop_id AS BIGINT) = sd.shop_id
    LEFT JOIN exposure_data exposure ON p.prod_id = exposure.prod_id
    LEFT JOIN order_data ON p.prod_id = order_data.prod_id
    LEFT JOIN refund_data ON p.prod_id = refund_data.prod_id
    
    WHERE p.prod_id IS NOT NULL
      AND $statusCondition
      AND (COALESCE(order_data.place_order_person, 0) > 0
        OR COALESCE(order_data.pay_person, 0) > 0
        OR COALESCE(refund_data.refund_num, 0) > 0)
    
    ORDER BY status_filter ASC, COALESCE(exposure.expose, 0) DESC
    """
  }
}