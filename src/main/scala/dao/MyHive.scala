package dao

import org.apache.spark.sql.SparkSession

/**
 * Hive连接工具类
 * 提供创建和配置Spark会话的方法，用于连接Hive数据仓库
 */
object MyHive {
  /**
   * 创建Spark会话连接到Hive
   * 首先尝试创建标准连接，如果失败则创建本地连接
   *
   * @param jobNname 作业名称，用于Spark应用标识
   * @return 配置好的SparkSession实例
   */
  def conn(implicit jobNname: String = "DefaultName"): SparkSession = {
    try {
      // 尝试创建标准Spark会话
      SparkSession.builder()
        .appName(jobNname)
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
        .config("hive.exec.max.dynamic.partitions", "20000")
        .enableHiveSupport()
        .getOrCreate()
    } catch {
      case _: Exception =>
        // 如果标准连接失败，创建本地连接
        SparkSession.builder()
          .appName(jobNname)
          .master("local[*]")
          .config("hive.metastore.uris", "thrift://cdh02:9083")
          //          .config("hive.metastore.uris", "thrift://172.16.1.57:9083,thrift://172.16.1.60:9083,thrift://172.16.1.59:9083")
          .config("hive.exec.max.dynamic.partitions", "20000")
          //.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

          //.config("spark.sql.warehouse.dir", "hdfs://cdh01:10000,hdfs://cdh02:10000,hdfs://cdh03:10000")
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
          /*  .config("spark.mongodb.read.connection.uri", "mongodb://59.110.149.138:27017")
            .config("spark.mongodb.read.database", "track")
            .config("spark.mongodb.read.collection", "login_track")
            .config("spark.mongodb.read.connection.uri", "mongodb://user:pass@host:port/?authSource=admin")
  */
          .enableHiveSupport()
          .getOrCreate()
    }
  }
}
