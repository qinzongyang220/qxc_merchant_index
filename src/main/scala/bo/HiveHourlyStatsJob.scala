package bo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import dao.MyHive
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * 商城按小时统计作业
 * 按小时分区统计订单支付数据，生成24小时的统计数据
 * 使用增量数据(inc)进行统计，每小时调度一次
 * 表中保持滚动2天数据（昨天+今天）
 */
object HiveHourlyStatsJob {

  /**
   * 显示时间范围说明
   */
  def displayTimeRanges(startTime: String, endTime: String): Unit = {
    println("\n======= 时间范围说明 =======")
    println(s"按小时统计: $startTime 至 $endTime")
    println("注意: 每小时调度模式，读取今天从00:00到当前时间的所有数据")
    println("数据保留策略: 滚动保留最近2天（昨天+今天）")
    println("==========================\n")
  }
  
  /**
   * 写入DataFrame到MySQL表，适配每小时调度的滚动2天数据策略
   */
  def writeToMySQLWithHourlySchedule(df: DataFrame, tableName: String, today: String): Unit = {
    try {
      val connection = Constants.DatabaseUtils.getWriteConnection
      val stmt = connection.createStatement()
      
      // 计算前天日期
      val cal = Calendar.getInstance()
      val dayFormat = new SimpleDateFormat("yyyy-MM-dd")
      cal.setTime(dayFormat.parse(today))
      cal.add(Calendar.DAY_OF_MONTH, -2) // 前天
      val dayBeforeYesterday = dayFormat.format(cal.getTime())
      
      // 获取当前小时
      val hourFormat = new SimpleDateFormat("HH")
      val currentHour = hourFormat.format(Calendar.getInstance().getTime())
      
      // 只在每天第一小时运行时删除前天数据（避免每小时都执行删除）
      if (currentHour == "00" || currentHour == "01") {
        val deleteOldSQL = s"DELETE FROM `$tableName` WHERE stat_date = '$dayBeforeYesterday'"
        println(s"执行滚动删除SQL（前天数据）: $deleteOldSQL")
        val deletedOldRows = stmt.executeUpdate(deleteOldSQL)
        println(s"从 $tableName 表中删除了 $deletedOldRows 行前天($dayBeforeYesterday)的数据")
      }
      
      // 每次运行都删除今天已有的数据，然后写入完整的今天数据
      val deleteTodaySQL = s"DELETE FROM `$tableName` WHERE stat_date = '$today'"
      println(s"执行今天数据更新SQL: $deleteTodaySQL")
      val deletedTodayRows = stmt.executeUpdate(deleteTodaySQL)
      println(s"从 $tableName 表中删除了 $deletedTodayRows 行今天($today)的旧数据")
      
      stmt.close()
      connection.close()
      
      // 写入今天的完整数据
      Constants.DatabaseUtils.writeDataFrameToMySQL(df, tableName, today, deleteBeforeInsert = false)
      println(s"写入今天($today)的完整小时级数据")
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
      // 跨天处理逻辑
      val (startTime, endTime, processDate, currentHour) = getTimeRange()
      
      println(s"小时级统计分析: $processDate")
      println(s"时间范围: $startTime 至 $endTime (当前小时: $currentHour)")
      
      println(s"处理按小时分区数据，日期: $processDate")
      // 显示时间范围说明
      displayTimeRanges(startTime, endTime)

      // 切换到mall_bbc数据库
      spark.sql("USE mall_bbc")

      // 按小时统计订单支付金额查询 - 今天的增量数据，按小时分区
      val hourlyOrderPaymentSQL = s"""
        SELECT 
            FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS order_date, 
            FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'HH') AS hour_of_day,
            shop_id, 
            COUNT(*) AS order_count,
            SUM(CAST(actual_total AS DECIMAL(18,2))) as pay_actual_total,
            '$processDate' AS stat_date
        FROM mall_bbc.t_dwd_order_inc
        WHERE is_payed = '1'
        AND pay_time >= '$startTime'
        AND pay_time <= '$endTime'
        AND shop_id IS NOT NULL
        AND shop_id != ''
        AND dt = '$processDate'
        GROUP BY FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd'),
                 FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'HH'),
                 shop_id
        ORDER BY order_date, CAST(hour_of_day AS INT) ASC, shop_id
      """

      val hourlyOrderPaymentDF = spark.sql(hourlyOrderPaymentSQL)
      val totalCount = hourlyOrderPaymentDF.count()
      
      if (totalCount > 0) {
        println(s"今天按小时订单支付数据 (共 $totalCount 条):")
        hourlyOrderPaymentDF.show(50, false)
        
        // 写入MySQL，使用每小时调度的滚动删除策略
        writeToMySQLWithHourlySchedule(hourlyOrderPaymentDF, "tz_bd_merchant_hourly_stats", processDate)
        println(s"处理日期($processDate)按小时分区的增量订单支付数据处理完成")
        println("表中现在保持最近2天的小时级数据")
      } else {
        println(s"处理日期($processDate)无增量订单支付数据")
      }

    } catch {
      case e: Exception => 
        println(s"错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("Spark会话已关闭")
    }
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