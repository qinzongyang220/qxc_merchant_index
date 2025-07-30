package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
object HiveHourlyStatsJob {

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
        .option("dbtable", "tz_bd_merchant_hourly_stats")
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
    implicit val jobName: String = "HiveHourlyStats"
    val spark: SparkSession = MyHive.conn

    try {
      // 设置时间范围为最近一小时
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val hourFormat = new SimpleDateFormat("HH")
      val dayFormat = new SimpleDateFormat("yyyy-MM-dd")

      val now = new Date()
      val currentDay = dayFormat.format(now)
      val currentHour = hourFormat.format(now)

      val cal = Calendar.getInstance()
      cal.add(Calendar.HOUR_OF_DAY, -1) // 减去1小时
      val oneHourAgo = dateFormat.format(cal.getTime())
      val currentTime = dateFormat.format(now)

      println(s"处理时间范围: $oneHourAgo 至 $currentTime")
      println(s"当前小时: $currentHour")

      // 切换到mall_bbc数据库
      spark.sql("USE mall_bbc")

      // 按小时统计订单支付金额查询 - 最近一小时的数据
      val hourlyOrderPaymentSQL = s"""
        SELECT 
            FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS order_date, 
            FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'HH') AS hour_of_day,
            shop_id, 
            COUNT(*) AS order_count,
            SUM(CAST(actual_total AS DECIMAL(18,2))) as pay_actual_total,
            TO_DATE(pay_time) AS stat_date
        FROM t_ods_tz_order
        WHERE is_payed = 'true'
        AND pay_time >= '$oneHourAgo'
        AND pay_time <= '$currentTime'
        GROUP BY FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd'),
                 FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'HH'),
                 shop_id, TO_DATE(pay_time)
        ORDER BY order_date, CAST(hour_of_day AS INT) ASC, shop_id
      """

      val hourlyOrderPaymentDF = spark.sql(hourlyOrderPaymentSQL)
      println("小时订单支付数据:")
      hourlyOrderPaymentDF.show(50, false)
      
      // 写入MySQL
      val currentDate = dayFormat.format(now)
      writeToMySQL(hourlyOrderPaymentDF, "tz_bd_merchant_hourly_stats", currentDate)
      
      println(s"小时($currentDay $currentHour:00)的订单支付数据处理完成")

    } catch {
      case e: Exception => 
        println(s"错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("Spark会话已关闭")
    }
  }
} 