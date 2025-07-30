package dao

import org.apache.spark.sql.SparkSession

/**
 * Hive连接工具类
 * 提供创建和配置Spark会话的方法，用于连接Hive数据仓库
 */
object MyHive {
  /**
   * 创建Spark会话连接到Hive
   * @param jobName 作业名称，用于Spark应用标识
   * @param masterUrl master URL (可选，默认null)
   * @return 配置好的SparkSession实例
   */
  def conn(implicit jobName: String = "DefaultName", masterUrl: String = null): SparkSession = {
    try {
      println(s"正在创建SparkSession，应用名称: $jobName")

      // 检查是否已有活跃的SparkSession
      SparkSession.getActiveSession match {
        case Some(existingSession) =>
          println("发现现有SparkSession，复用现有会话")
          return existingSession
        case None =>
          println("未发现现有SparkSession，创建新会话")
      }

      // 使用优化配置，防止YARN ApplicationMaster超时
      val spark = SparkSession.builder()
        .appName(jobName)
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
        .config("hive.exec.max.dynamic.partitions", "20000")
        .config("hive.metastore.uris", "thrift://cdh02:9083")
        // 增加YARN相关配置，防止超时和内存溢出
        .config("spark.yarn.am.waitTime", "300s")
        .config("spark.yarn.am.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.yarn.executor.memoryFraction", "0.6")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        // 增加网络和超时配置
        .config("spark.network.timeout", "800s")
        .config("spark.rpc.askTimeout", "600s")
        .config("spark.sql.broadcastTimeout", "36000")
        // 优化内存管理
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000")
        .getOrCreate()

      println(s"SparkSession创建成功，应用ID: ${spark.sparkContext.applicationId}")
      spark
    } catch {
      case e: Exception =>
        println(s"创建SparkSession失败: ${e.getMessage}")
        println(s"异常类型: ${e.getClass.getSimpleName}")
        e.printStackTrace()
        throw new RuntimeException("无法创建SparkSession，请检查配置", e)
    }
  }
}