package bo

import dao.MyHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * 商城按小时统计作业
 * 按小时分区统计昨天一天的订单支付数据，生成24小时的统计数据
 */
object HiveHourlyStatsJob {

  /**
   * 写入DataFrame到MySQL表
   */
  def writeToMySQL(df: DataFrame, tableName: String, statDate: String): Unit = {
    try {
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, statDate, deleteBeforeInsert = true)
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
    implicit val jobName: String = "HiveHourlyStats"
    val spark: SparkSession = MyHive.conn

    try {
      // 设置时间范围为昨天一天，按小时分区
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dayFormat = new SimpleDateFormat("yyyy-MM-dd")

      val cal = Calendar.getInstance()
      cal.add(Calendar.DAY_OF_MONTH, -1) // 减去1天，获取昨天
      val yesterday = dayFormat.format(cal.getTime())
      
      val startTime = s"$yesterday 00:00:00"
      val endTime = s"$yesterday 23:59:59"

      println(s"处理昨天按小时分区数据，日期: $yesterday")
      println(s"时间范围: $startTime 至 $endTime")

      // 切换到mall_bbc数据库
      spark.sql("USE mall_bbc")

      // 按小时统计订单支付金额查询 - 昨天一天的数据，按小时分区
      val hourlyOrderPaymentSQL = s"""
        SELECT 
            FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS order_date, 
            FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'HH') AS hour_of_day,
            shop_id, 
            COUNT(*) AS order_count,
            SUM(CAST(actual_total AS DECIMAL(18,2))) as pay_actual_total,
            TO_DATE('$yesterday') AS stat_date
        FROM t_ods_tz_order
        WHERE is_payed = 'true'
        AND pay_time >= '$startTime'
        AND pay_time <= '$endTime'
        GROUP BY FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd'),
                 FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'HH'),
                 shop_id, TO_DATE(pay_time)
        ORDER BY order_date, CAST(hour_of_day AS INT) ASC, shop_id
      """

      val hourlyOrderPaymentDF = spark.sql(hourlyOrderPaymentSQL)
      println("小时订单支付数据:")
      hourlyOrderPaymentDF.show(50, false)
      
      // 写入MySQL
      writeToMySQL(hourlyOrderPaymentDF, "tz_bd_merchant_hourly_stats", yesterday)
      
      println(s"昨天($yesterday)按小时分区的订单支付数据处理完成")

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