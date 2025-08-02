package bo

/**
 * Constants used throughout the application
 */
object Constants {

  /**
   * Hive table names
   */
  object HiveTable {
    val USER_BASE_INFO = "user_base_info"
    val USER_LOC_FEATURE = "user_loc_feature"
    val USER_RFM_FEATURE = "user_rfm_feature"
    val APP_LOG_DATA = "app_log_data"
    val PHONE_PRICE = "phone_price"
    val USER_STATS_FEATURE = "user_stats_feature"
  }

  /**
   * Field names for JSON objects
   */
  object FieldName {
    val UID = "uid"
    val REGISTER_CHANNEL = "register_channel"
    val AGE_RANGE = "age_range"
    val REGISTER_TIME = "register_time"
    val GENDER = "gender"
    val PROVINCE = "province"
    val CITY = "city"
    val DISTRICT = "district"
    val DEVICE_TYPE = "device_type"
    val DEVICE_PRICE = "device_price"
    val MEMBER_LEVEL = "member_level"
    val ALGO_EXTRA = "algo_extra"
  }

  /**
   * 数据库读取配置 - 59.110.149.138:3306
   */
  object JdbcRead138Port3306 {
    val jdbcUrl = "jdbc:mysql://59.110.149.138:3306/yami_bbc?useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true"
    val username = "yami_read"
    val password = "Y&%2025Ai1!"
    val driver = "com.mysql.cj.jdbc.Driver"
  }
  
  /**
   * 数据库读取配置 - 59.110.149.138:8501 (flink用户)
   */
  object JdbcRead138Port8501 {
    val jdbcUrl = "jdbc:mysql://59.110.149.138:8501/yami_bbc?useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true"
    val username = "flink"
    val password = "Y&%2025Am!"
    val driver = "com.mysql.cj.jdbc.Driver"
  }

  /**
   * 数据库读取配置 - 192.168.3.35:3306
   */
  object JdbcRead35Port3306 {
    val jdbcUrl = "jdbc:mysql://192.168.3.35:3306/yami_bbc?useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true"
    val username = "flink"
    val password = "Y&%2025Am!"
    val driver = "com.mysql.cj.jdbc.Driver"
  }

  /**
   * 数据库读取配置 - 扩展参数版本
   */
  object JdbcReadExtended {
    val jdbcUrl = "jdbc:mysql://59.110.149.138:3306/yami_bbc?serverTimezone=Asia/Shanghai&autoReconnect=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8&socketTimeout=300000&connectTimeout=30000&maxAllowedPacket=104857600"
    val username = "flink"
    val password = "Y&%2025Am!"
    val driver = "com.mysql.cj.jdbc.Driver"
  }

  /**
   * 数据库写入配置 - 统计数据库 (修正主机名)
   */
  object JdbcWriteStatistics {

//    val jdbcUrl = "jdbc:mysql://rm-2zedtr7h3427p19kc.mysql.rds.aliyuncs.com:3306/dlc_statistics?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai"
    val jdbcUrl = "jdbc:mysql://rm-2zedtr7h3427p19kcbo.mysql.rds.aliyuncs.com:3306/dlc_statistics?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai"
    val username = "bigdata_statistics"
    val password = "Y&%20Am1!"
    val driver = "com.mysql.cj.jdbc.Driver"
  }

  /**
   * 数据库工具方法
   */
  object DatabaseUtils {
    import org.apache.spark.sql.DataFrame

    import java.sql.{Connection, DriverManager}
    import java.util.Properties
    
    /**
     * 获取写入数据库连接
     */
    def getWriteConnection: Connection = {
      Class.forName(JdbcWriteStatistics.driver)
      DriverManager.getConnection(JdbcWriteStatistics.jdbcUrl, JdbcWriteStatistics.username, JdbcWriteStatistics.password)
    }
    
    /**
     * 获取读取数据库连接 (默认使用8501端口)
     */
    def getReadConnection: Connection = {
      Class.forName(JdbcRead138Port8501.driver)
      DriverManager.getConnection(JdbcRead138Port8501.jdbcUrl, JdbcRead138Port8501.username, JdbcRead138Port8501.password)
    }
    
    /**
     * 写入DataFrame到MySQL表（分布式环境优化版本）
     */
    def writeDataFrameToMySQL(df: DataFrame, tableName: String, statDate: String, deleteBeforeInsert: Boolean = true): Unit = {
      var connection: Connection = null
      try {
        println(s"开始写入数据到表 $tableName")
        println(s"数据库URL: ${JdbcWriteStatistics.jdbcUrl}")
        println(s"数据行数: ${df.count()}")
        
        // 1. 如果需要，先删除指定日期的现有数据
        if (deleteBeforeInsert) {
          connection = getWriteConnection
          println("数据库连接成功")
          
          val deleteSql = s"DELETE FROM `$tableName` WHERE stat_date = '$statDate'"
          println(s"执行删除SQL: $deleteSql")
          val statement = connection.createStatement()
          val deletedRows = statement.executeUpdate(deleteSql)
          println(s"从 $tableName 表中删除了 $deletedRows 行数据 (日期: $statDate)")
          statement.close()
          connection.close()
          connection = null
        }

        // 2. 插入新数据
        val props = new Properties()
        props.put("user", JdbcWriteStatistics.username)
        props.put("password", JdbcWriteStatistics.password)
        props.put("driver", JdbcWriteStatistics.driver)
        
        df.write
          .mode("append")
          .jdbc(JdbcWriteStatistics.jdbcUrl, tableName, props)
          
        println(s"成功写入数据到 $tableName 表 (日期: $statDate)")
      } catch {
        case e: Exception =>
          println(s"写入MySQL表 $tableName 时出错: ${e.getMessage}")
          e.printStackTrace()
          throw e
      } finally {
        if (connection != null) {
          try {
            connection.close()
          } catch {
            case e: Exception => println(s"关闭连接时出错: ${e.getMessage}")
          }
        }
      }
    }
  }
}