package bo

import java.sql.{Connection, DriverManager, ResultSet, Statement, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Calendar}
import dao.MyHive
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

/**
 * user_tag库中t_ods_app_logdata表读取器
 */
object AppLogReader {
  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 使用MyHive连接器连接Hive
    implicit val jobName: String = "AppLogReader"
    val spark: SparkSession = MyHive.conn

    try {
      println("成功连接到Hive")

      // 使用mall_bbc数据库
      spark.sql("USE mall_bbc")
      println("当前使用数据库: mall_bbc")

      // 获取昨天的日期
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

      val cal = Calendar.getInstance()
      cal.add(Calendar.DAY_OF_MONTH, -1) // 减去1天，获取昨天的日期
      val yesterday = dateFormat.format(cal.getTime())

      println(s"处理日期: $yesterday")

      // 订单支付数据主查询 - 昨天的数据
      println("执行订单支付数据主查询...")
      val orderPaymentDF = spark.sql(
        s"""
    SELECT
        shop_id,
        -- 基础支付数据
        SUM(CAST(actual_total AS DECIMAL(18,2))) AS payActualTotal,
        COUNT(DISTINCT user_id) AS payUserCount,
        COUNT(*) AS payOrderCount,
        -- 客单价计算
        CASE
            WHEN COUNT(DISTINCT user_id) > 0
            THEN SUM(CAST(actual_total AS DECIMAL(18,2))) / COUNT(DISTINCT user_id)
            ELSE 0
        END AS onePrice
    FROM
        t_ods_tz_order
    WHERE
        is_payed = '1'
        AND dt = '$yesterday'
        AND SUBSTR(pay_time, 1, 10) = '$yesterday'
    GROUP BY shop_id
    ORDER BY payActualTotal DESC
    """)

      // 显示结果
      println(s"昨天($yesterday)订单支付数据查询结果:")
      orderPaymentDF.show(false)

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
} 