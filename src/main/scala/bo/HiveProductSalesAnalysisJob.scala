package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Calendar
/**
 * Hive商品销售分析作业
 * 从Hive表中读取商品销售排行和访问转化率数据，按商店ID分组
 */
object HiveProductSalesAnalysisJob {

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "HiveProductSalesAnalysis"
    val spark: SparkSession = MyHive.conn

    try {
      // 设置Spark配置，增加显示字段数
      spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
      
      println("成功连接到Hive")
      
      // 使用mall_bbc和user_tag数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")
      
      // 设置日期范围为上一周（不含今天）
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      
      val cal = Calendar.getInstance()
      cal.add(Calendar.DAY_OF_MONTH, -1) // 昨天
      val yesterday = dateFormat.format(cal.getTime())
      
      cal.add(Calendar.DAY_OF_MONTH, -6) // 从昨天开始往前推6天，得到上一周的开始日期
      val lastWeekStart = dateFormat.format(cal.getTime())
      
      val startDate = yesterday
      val endDate = yesterday
      
      println(s"分析时间范围: $startDate 至 $endDate")
      println("按商店ID分组处理所有商店数据")
      
      // 分页参数
      val pageSize = 10
      val pageOffset = 0
      
      // 临时注册埋点视图
      registerExposureView(spark, startDate, endDate)
      
      // 执行商品销售排行查询
      println("执行商品销售排行查询...")
      val productSalesRankingSQL = s"""
      -- 商品销售排行SQL
      -- 此查询用于获取商品销售排行数据，按商店ID分组
      WITH sales_data AS (
        SELECT 
            oi.prod_id, 
            p.shop_id,
            sd.shop_name AS shop_name, 
            oi.pic, 
            p.prod_name,
            SUM(CAST(oi.actual_total AS DECIMAL(18,2))) AS payAmount, 
            SUM(CAST(oi.prod_count AS INT)) AS payCount,
            ROW_NUMBER() OVER (PARTITION BY p.shop_id ORDER BY SUM(CAST(oi.actual_total AS DECIMAL(18,2))) DESC) AS rank_num
        FROM t_ods_tz_order_item oi
        LEFT JOIN t_ods_tz_order o ON o.order_number = oi.order_number
        LEFT JOIN t_ods_tz_prod p ON p.prod_id = oi.prod_id
        LEFT JOIN t_ods_tz_shop_detail sd ON sd.shop_id = p.shop_id
        WHERE o.is_payed = 'true' 
          AND p.status != '-1'
          AND o.pay_time >= '$startDate 00:00:00' 
          AND o.pay_time <= '$endDate 23:59:59'
          AND oi.rec_time BETWEEN '$startDate 00:00:00' AND '$endDate 23:59:59'
        GROUP BY oi.prod_id, p.shop_id, sd.shop_name, oi.pic, p.prod_name
      )
      
      SELECT 
          prod_id, 
          shop_id,
          shop_name, 
          pic, 
          prod_name, 
          payAmount, 
          payCount,
          TO_DATE('$startDate') AS stat_date
      FROM sales_data
      WHERE rank_num <= $pageSize
      ORDER BY shop_id, payAmount DESC
      """
      
      val salesRankingDF = spark.sql(productSalesRankingSQL)
      println("商品销售排行查询结果:")
      salesRankingDF.show(Int.MaxValue, false) // 显示所有行，不截断
      
      // 去重处理，避免唯一索引冲突
      val dedupSalesDF = salesRankingDF.dropDuplicates(Seq("prod_id", "shop_id", "stat_date"))
      val beforeSalesCount = salesRankingDF.count()
      val afterSalesCount = dedupSalesDF.count()
      println(s"销售排行数据去重: $beforeSalesCount -> $afterSalesCount 条记录")
      
      // 写入MySQL-商品销售排行
      val jdbcUrl = "jdbc:mysql://rm-2zedtr7h3427p19kcbo.mysql.rds.aliyuncs.com:3306/dlc_statistics?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai"
      val jdbcUser = "bigdata_statistics"
      val jdbcPassword = "Y&%20Am1!"
      val jdbcDriver = "com.mysql.cj.jdbc.Driver"
      val salesTable = "tz_bd_merchant_product_sales_rank"
      import java.sql.DriverManager
      Class.forName(jdbcDriver)
      val salesConn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      val delSalesSql = s"DELETE FROM `$salesTable` WHERE stat_date = '$startDate'"
      println(s"执行删除SQL: $delSalesSql")
      val salesStmt = salesConn.createStatement()
      val delSalesRows = salesStmt.executeUpdate(delSalesSql)
      println(s"从 $salesTable 表中删除了 $delSalesRows 行数据 (日期: $startDate)")
      salesConn.close()
      dedupSalesDF.write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", salesTable)
        .option("user", jdbcUser)
        .option("password", jdbcPassword)
        .option("driver", jdbcDriver)
        .mode("append")
        .save()
      println(s"成功写入数据到 $salesTable 表 (日期: $startDate)")
      
      // 商品访问转化率
      println("执行商品访问转化率查询...")
      val productVisitorConversionSQL = s"""
      -- 商品访问转化率SQL
      -- 此查询用于获取商品访问转化率数据，按商店ID分组
      -- 曝光表去重
      WITH visitor_data AS (
        SELECT 
          prod_id,
          shop_id,
          COUNT(*) AS visitor_num
        FROM temp_exposure
        GROUP BY prod_id, shop_id
      ),
      pay_user_data AS (
        SELECT 
          oi.prod_id,
          o.shop_id,
          COUNT(DISTINCT oi.user_id) AS pay_user_count
        FROM t_ods_tz_order_item oi
        LEFT JOIN t_ods_tz_order o ON o.order_number = oi.order_number
        WHERE oi.rec_time >= '$startDate 00:00:00' 
          AND oi.rec_time <= '$endDate 23:59:59'
        GROUP BY oi.prod_id, o.shop_id
      ),
      conversion_data AS (
        SELECT 
          p.prod_id AS prodId, 
          p.shop_id,
          sd.shop_name AS shop_name,  -- 修正：从商店表查shop_name
          p.prod_name,
          p.pic,
          COALESCE(v.visitor_num, 0) AS visitorNum,
          CASE 
            WHEN COALESCE(v.visitor_num, 0) > 0 
            THEN ROUND(COALESCE(pu.pay_user_count, 0) / COALESCE(v.visitor_num, 0), 4)
            ELSE 0
          END AS visitorToPayRate,
          ROW_NUMBER() OVER (PARTITION BY p.shop_id ORDER BY COALESCE(v.visitor_num, 0) DESC) AS rank_num
        FROM t_ods_tz_prod p
        LEFT JOIN visitor_data v ON p.prod_id = v.prod_id AND p.shop_id = v.shop_id
        LEFT JOIN pay_user_data pu ON p.prod_id = pu.prod_id AND p.shop_id = pu.shop_id
        LEFT JOIN t_ods_tz_shop_detail sd ON p.shop_id = sd.shop_id  -- 新增
        WHERE p.prod_name IS NOT NULL
          AND p.status != '-1'
        GROUP BY p.prod_id, p.shop_id, sd.shop_name, p.prod_name, p.pic, COALESCE(v.visitor_num, 0), COALESCE(pu.pay_user_count, 0)
      )
      
      SELECT 
          prodId, 
          shop_id,
          shop_name, 
          prod_name, 
          pic, 
          visitorNum, 
          visitorToPayRate,
          TO_DATE('$startDate') AS stat_date
      FROM conversion_data
      WHERE rank_num <= $pageSize
      ORDER BY shop_id, visitorNum DESC
      """
      val visitorConversionDF = spark.sql(productVisitorConversionSQL)
      println("商品访问转化率查询结果:")
      visitorConversionDF.show(Int.MaxValue, false) // 显示所有行，不截断
      
      // 列名统一，prodId -> prod_id
      val visitorConversionDF2 = visitorConversionDF.withColumnRenamed("prodId", "prod_id")
      
      // 去重处理，避免唯一索引冲突
      val dedupVisitorDF = visitorConversionDF2.dropDuplicates(Seq("prod_id", "shop_id", "stat_date"))
      val beforeCount = visitorConversionDF2.count()
      val afterCount = dedupVisitorDF.count()
      println(s"访问转化率数据去重: $beforeCount -> $afterCount 条记录")
      
      // 写入MySQL-商品访问转化率
      val visitorTable = "tz_bd_merchant_product_visitor_conversion"
      Class.forName(jdbcDriver)
      val visitorConn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      val delVisitorSql = s"DELETE FROM `$visitorTable` WHERE stat_date = '$startDate'"
      println(s"执行删除SQL: $delVisitorSql")
      val visitorStmt = visitorConn.createStatement()
      val delVisitorRows = visitorStmt.executeUpdate(delVisitorSql)
      println(s"从 $visitorTable 表中删除了 $delVisitorRows 行数据 (日期: $startDate)")
      visitorConn.close()
      dedupVisitorDF.write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", visitorTable)
        .option("user", jdbcUser)
        .option("password", jdbcPassword)
        .option("driver", jdbcDriver)
        .mode("append")
        .save()
      println(s"成功写入数据到 $visitorTable 表 (日期: $startDate)")
      
      println("数据查询完成")

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
   * 注册临时视图，用于分析埋点数据
   */
  private def registerExposureView(spark: SparkSession, startDate: String, endDate: String): Unit = {
    try {
      // 切换到user_tag数据库以获取埋点数据
      spark.sql("USE user_tag")
      println("切换到数据库: user_tag (用于查询埋点数据)")
      
      // 曝光视图 - 按商店ID分组
      val exposureSQL = s"""
        SELECT 
          CAST(prodid AS BIGINT) AS prod_id,
          CAST(shopid AS BIGINT) AS shop_id,
          COUNT(*) AS expose_count,
          COUNT(DISTINCT cid) AS expose_person_num
        FROM user_tag.t_ods_app_logdata
        WHERE dt BETWEEN '$startDate' AND '$endDate'
          AND action = 'enter'
          AND page_id = '1005'
          AND prodid IS NOT NULL
          AND shopid IS NOT NULL
        GROUP BY CAST(prodid AS BIGINT), CAST(shopid AS BIGINT)
      """
      
      // 创建临时视图
      println("创建商品曝光临时视图...")
      val exposureDF = spark.sql(exposureSQL)
      exposureDF.createOrReplaceTempView("temp_exposure")
      
      // 切换回mall_bbc数据库处理订单相关数据
      spark.sql("USE mall_bbc")
      println("切换回数据库: mall_bbc (用于查询订单数据)")
      
    } catch {
      case e: Exception => 
        println(s"创建临时视图时出错: ${e.getMessage}")
        e.printStackTrace()
    }
  }
} 