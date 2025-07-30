package dao

object HiveDashboardStatsSqlPrintJob {
  def main(args: Array[String]): Unit = {
    // 关闭大部分Spark日志
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)



    // 连接Hive
    implicit val jobName: String = "HiveDashboardStatsSqlPrintJob"
    val spark = MyHive.conn
    spark.sql("USE mall_bbc")


//    val yesterday = "2025-07-15"
//    val dayBeforeYesterday = "2025-07-14"
//    val firstDayOfMonth = "2025-07-01"
//    // 1. 订单支付数据主查询
//    val mainSql =
//      s"""
//         |SELECT
//         |    o.shop_id,
//         |    NVL(SUM(CAST(o.actual_total AS DECIMAL(18,2))), 0) AS pay_actual_total,
//         |    COUNT(DISTINCT o.user_id) AS pay_user_count,
//         |    COUNT(*) AS pay_order_count,
//         |    CASE
//         |        WHEN COUNT(DISTINCT o.user_id) > 0
//         |        THEN NVL(SUM(CAST(o.actual_total AS DECIMAL(18,2))), 0) / COUNT(DISTINCT o.user_id)
//         |        ELSE 0
//         |    END AS one_price,
//         |    NVL(MAX(CAST(refund.refund_sum AS DECIMAL(18,2))), 0) AS refund,
//         |    NVL(MAX(CAST(yesterday_pay.actual_total_sum AS DECIMAL(18,2))), 0) AS yesterday_pay_actual_total,
//         |    NVL(MAX(CAST(yesterday_pay.user_count AS INT)), 0) AS yesterday_pay_user_count,
//         |    NVL(MAX(CAST(yesterday_pay.order_count AS INT)), 0) AS yesterday_pay_order_count,
//         |    CASE
//         |        WHEN NVL(MAX(CAST(yesterday_pay.user_count AS INT)), 0) > 0
//         |        THEN NVL(MAX(CAST(yesterday_pay.actual_total_sum AS DECIMAL(18,2))), 0) / NVL(MAX(CAST(yesterday_pay.user_count AS INT)), 0)
//         |        ELSE 0
//         |    END AS yesterday_one_price,
//         |    NVL(MAX(CAST(yesterday_refund.refund_sum AS DECIMAL(18,2))), 0) AS yesterday_refund,
//         |    TO_DATE('$yesterday') AS stat_date
//         |FROM
//         |    t_ods_tz_order o
//         |LEFT JOIN (
//         |    SELECT shop_id, SUM(CAST(refund_amount AS DECIMAL(18,2))) AS refund_sum
//         |    FROM t_ods_tz_order_refund
//         |    WHERE return_money_sts = 5
//         |    AND refund_time LIKE '$yesterday%'
//         |    GROUP BY shop_id
//         |) refund ON o.shop_id = refund.shop_id
//         |LEFT JOIN (
//         |    SELECT
//         |        shop_id,
//         |        SUM(CAST(actual_total AS DECIMAL(18,2))) AS actual_total_sum,
//         |        COUNT(DISTINCT user_id) AS user_count,
//         |        COUNT(*) AS order_count
//         |    FROM t_ods_tz_order
//         |    WHERE is_payed = 'true'
//         |    AND pay_time LIKE '$dayBeforeYesterday%'
//         |    GROUP BY shop_id
//         |) yesterday_pay ON o.shop_id = yesterday_pay.shop_id
//         |LEFT JOIN (
//         |    SELECT shop_id, SUM(CAST(refund_amount AS DECIMAL(18,2))) AS refund_sum
//         |    FROM t_ods_tz_order_refund
//         |    WHERE return_money_sts = 5
//         |    AND refund_time LIKE '$dayBeforeYesterday%'
//         |    GROUP BY shop_id
//         |) yesterday_refund ON o.shop_id = yesterday_refund.shop_id
//         |WHERE
//         |    o.is_payed = 'true'
//         |    AND o.pay_time LIKE '$yesterday%'
//         |GROUP BY
//         |    o.shop_id
//         |ORDER BY
//         |    o.shop_id
//         |""".stripMargin
//    val mainDF = spark.sql(mainSql)
//    println("\n--- 1. 订单支付数据主查询 ---")
//    mainDF.show(false)
//
//    // 2. 订单支付统计查询
//    val orderPaySql =
//      s"""
//         |SELECT
//         |    a.shop_id,
//         |    a.pay_order_count,
//         |    a.pay_actual_total,
//         |    NVL(b.today_amount, 0) AS today_amount,
//         |    NVL(c.month_amount, 0) AS month_amount,
//         |    TO_DATE('$yesterday') AS stat_date
//         |FROM (
//         |    SELECT shop_id, COUNT(*) AS pay_order_count, SUM(CAST(actual_total AS DECIMAL(18,2))) AS pay_actual_total
//         |    FROM t_ods_tz_order
//         |    WHERE is_payed = 'true' AND pay_time LIKE '$yesterday%'
//         |    GROUP BY shop_id
//         |    UNION ALL
//         |    SELECT '' AS shop_id, 0 AS pay_order_count, 0 AS pay_actual_total
//         |) a
//         |LEFT JOIN (
//         |    SELECT shop_id, SUM(CAST(actual_total AS DECIMAL(18,2))) AS today_amount
//         |    FROM t_ods_tz_order
//         |    WHERE is_payed = 'true' AND pay_time LIKE '$yesterday%'
//         |    GROUP BY shop_id
//         |) b ON a.shop_id = b.shop_id
//         |LEFT JOIN (
//         |    SELECT shop_id, SUM(CAST(actual_total AS DECIMAL(18,2))) AS month_amount
//         |    FROM t_ods_tz_order
//         |    WHERE is_payed = 'true'
//         |    AND pay_time BETWEEN '$firstDayOfMonth 00:00:00'
//         |                   AND '$yesterday 23:59:59'
//         |    GROUP BY shop_id
//         |) c ON a.shop_id = c.shop_id
//         |ORDER BY a.shop_id
//         |""".stripMargin
//    val orderPayDF = spark.sql(orderPaySql)
//    println("\n--- 2. 订单支付统计查询 ---")
//    orderPayDF.show(false)
//
//    // 3. 退款商品排行榜查询
//    val refundProdRankSql =
//      s"""
//         |SELECT
//         |    toi.prod_id,
//         |    p.shop_id,
//         |    NVL(SUM(NVL(CAST(tor.goods_num AS INT), CAST(toi.prod_count AS INT))),0) refund_count,
//         |    toi.prod_name AS refund_prod_name,
//         |    p.pic,
//         |    TO_DATE('$yesterday') AS stat_date
//         |FROM t_ods_tz_order_item toi
//         |LEFT JOIN t_ods_tz_order_refund tor ON toi.order_item_id = tor.order_item_id
//         |LEFT JOIN t_ods_tz_prod p ON toi.prod_id = p.prod_id
//         |WHERE (toi.order_item_id IN (
//         |    SELECT order_item_id FROM t_ods_tz_order_refund
//         |    WHERE return_money_sts = 5 AND refund_type = 2
//         |    AND TO_DATE(refund_time) = '$yesterday'
//         |)
//         |OR toi.order_number IN (
//         |    SELECT order_number FROM t_ods_tz_order WHERE order_id IN (
//         |        SELECT order_id FROM t_ods_tz_order_refund
//         |        WHERE return_money_sts = 5 AND refund_type = 1
//         |        AND TO_DATE(refund_time) = '$yesterday'
//         |    )
//         |))
//         |GROUP BY toi.prod_id, p.shop_id, toi.prod_name, p.pic
//         |ORDER BY refund_count DESC
//         |""".stripMargin
//    val refundProdRankDF = spark.sql(refundProdRankSql)
//    println("\n--- 3. 退款商品排行榜查询 ---")
//    refundProdRankDF.show(false)
//
//    // 4. 退款原因排行榜查询
//    val refundReasonRankSql =
//      s"""
//         |WITH refund_total AS (
//         |  SELECT
//         |    shop_id,
//         |    SUM(CAST(refund_amount AS DECIMAL(18,2))) AS total_refund_amount
//         |  FROM t_ods_tz_order_refund
//         |  WHERE return_money_sts = 5
//         |    AND TO_DATE(refund_time) = '$yesterday'
//         |  GROUP BY shop_id
//         |),
//         |refund_data AS (
//         |  SELECT
//         |    a.buyer_reason,
//         |    b.prod_name,
//         |    a.refund_amount,
//         |    a.shop_id,
//         |    a.order_item_id,
//         |    rt.total_refund_amount
//         |  FROM t_ods_tz_order_refund a
//         |  JOIN t_ods_tz_order b ON a.order_id = b.order_id
//         |  LEFT JOIN refund_total rt ON a.shop_id = rt.shop_id
//         |  WHERE a.return_money_sts = 5
//         |    AND TO_DATE(a.refund_time) = '$yesterday'
//         |),
//         |product_pics AS (
//         |  SELECT DISTINCT
//         |    oi.order_item_id,
//         |    p.pic
//         |  FROM t_ods_tz_order_item oi
//         |  JOIN t_ods_tz_prod p ON oi.prod_id = p.prod_id
//         |)
//         |SELECT
//         |  rd.buyer_reason AS buyer_reason,
//         |  MAX(rd.prod_name) AS refund_prod_name,
//         |  COUNT(rd.buyer_reason) AS refund_count,
//         |  SUM(rd.refund_amount) AS pay_actual_total,
//         |  CASE
//         |    WHEN MAX(rd.total_refund_amount) = 0 THEN 0.0
//         |    ELSE ROUND(SUM(rd.refund_amount) / MAX(rd.total_refund_amount) * 100, 1)
//         |  END AS percent_amount,
//         |  MAX(pp.pic) AS pic,
//         |  rd.shop_id,
//         |  TO_DATE('$yesterday') AS stat_date
//         |FROM refund_data rd
//         |LEFT JOIN product_pics pp ON rd.order_item_id = pp.order_item_id
//         |GROUP BY rd.buyer_reason, rd.shop_id
//         |ORDER BY refund_count DESC, pay_actual_total DESC
//         |""".stripMargin
//    val refundReasonRankDF = spark.sql(refundReasonRankSql)
//    println("\n--- 4. 退款原因排行榜查询 ---")
//    refundReasonRankDF.show(false)
//
//    // 5. 综合退款统计查询
//    val refundStatsSql =
//      s"""
//         |WITH refund_stats AS (
//         |    SELECT
//         |        shop_id AS refund_shop_id,
//         |        SUM(CAST(refund_amount AS DECIMAL(18,2))) AS refund_amount,
//         |        COUNT(DISTINCT order_id) AS refund_count
//         |    FROM t_ods_tz_order_refund
//         |    WHERE return_money_sts = 5
//         |    AND TO_DATE(refund_time) = '$yesterday'
//         |    GROUP BY shop_id
//         |),
//         |order_stats AS (
//         |    SELECT
//         |        shop_id AS order_shop_id,
//         |        COUNT(order_id) AS order_count
//         |    FROM t_ods_tz_order
//         |    WHERE status >= 2
//         |    AND TO_DATE(pay_time) = '$yesterday'
//         |    GROUP BY shop_id
//         |),
//         |all_shops AS (
//         |    SELECT refund_shop_id AS shop_id FROM refund_stats
//         |    UNION
//         |    SELECT order_shop_id AS shop_id FROM order_stats
//         |)
//         |SELECT
//         |    TO_DATE('$yesterday') AS refund_date,
//         |    '$yesterday' AS refund_date_to_string,
//         |    COALESCE(s.shop_id, 0) AS shop_id,
//         |    COALESCE(r.refund_amount, 0) AS pay_actual_total,
//         |    COALESCE(r.refund_count, 0) AS refund_count,
//         |    COALESCE(o.order_count, 0) AS pay_order_count,
//         |    CASE
//         |        WHEN COALESCE(r.refund_count, 0) > 0 AND COALESCE(o.order_count, 0) = 0 THEN 100.0000
//         |        WHEN COALESCE(o.order_count, 0) > 0 THEN COALESCE(r.refund_count, 0) / o.order_count * 100
//         |        ELSE 0
//         |    END AS refund_rate,
//         |    TO_DATE('$yesterday') AS stat_date
//         |FROM
//         |    all_shops s
//         |LEFT JOIN
//         |    refund_stats r ON s.shop_id = r.refund_shop_id
//         |LEFT JOIN
//         |    order_stats o ON s.shop_id = o.order_shop_id
//         |ORDER BY
//         |    s.shop_id
//         |""".stripMargin
//    val refundStatsDF = spark.sql(refundStatsSql)
//    println("\n--- 5. 综合退款统计查询 ---")
//    refundStatsDF.show(false)
//
//    val statDate = "2025-07-15"
//    val hourlySql =
//      s"""
//         |SELECT
//         |    FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS order_date,
//         |    FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'HH') AS hour_of_day,
//         |    shop_id,
//         |    COUNT(*) AS order_count,
//         |    SUM(CAST(actual_total AS DECIMAL(18,2))) as pay_actual_total,
//         |    TO_DATE(pay_time) AS stat_date
//         |FROM t_ods_tz_order
//         |WHERE is_payed = 'true'
//         |  AND TO_DATE(pay_time) = '$statDate'
//         |GROUP BY FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd'),
//         |         FROM_UNIXTIME(UNIX_TIMESTAMP(pay_time, 'yyyy-MM-dd HH:mm:ss'), 'HH'),
//         |         shop_id, TO_DATE(pay_time)
//         |ORDER BY order_date, CAST(hour_of_day AS INT) ASC, shop_id
//         |""".stripMargin
//
//    val df = spark.sql(hourlySql)
//    println(s"--- $statDate 0-23点每小时订单支付数据 ---")
//    df.show(100, false) // 可根据需要调整显示行数

    // 6. 商品分析查询（HiveProductAnalysisQueryJob主SQL）
    val productAnalysisSql =
      s"""
         |WITH product_exposure AS (
         |  SELECT 
         |    CAST(prodid AS BIGINT) AS prod_id,
         |    CAST(shopid AS BIGINT) AS shop_id,
         |    COUNT(*) AS expose_count,
         |    COUNT(DISTINCT userid) AS expose_person_num
         |  FROM user_tag.t_ods_app_logdata
         |  WHERE dt = '2025-07-15'
         |    AND event = 'AppViewExpose'
         |    AND module = '产品展示区'
         |    AND prodid IS NOT NULL
         |    AND shopid IS NOT NULL
         |  GROUP BY CAST(prodid AS BIGINT), CAST(shopid AS BIGINT)
         |),
         |order_data AS (
         |  SELECT
         |    oi.prod_id,
         |    o.shop_id,
         |    COUNT(DISTINCT o.user_id) AS order_user_count,
         |    SUM(CAST(oi.prod_count AS INT)) AS order_item_count,
         |    SUM(CAST(oi.actual_total AS DECIMAL(18,2))) AS order_amount
         |  FROM mall_bbc.t_ods_tz_order o
         |  JOIN mall_bbc.t_ods_tz_order_item oi ON o.order_number = oi.order_number
         |  WHERE o.create_time BETWEEN '2025-07-15 00:00:00' AND '2025-07-15 23:59:59'
         |    AND oi.rec_time BETWEEN '2025-07-15 00:00:00' AND '2025-07-15 23:59:59'
         |  GROUP BY oi.prod_id, o.shop_id
         |),
         |pay_data AS (
         |  SELECT
         |    oi.prod_id,
         |    o.shop_id,
         |    COUNT(DISTINCT o.user_id) AS pay_user_count,
         |    SUM(CAST(oi.prod_count AS INT)) AS pay_num,
         |    SUM(CAST(oi.actual_total AS DECIMAL(18,2))) AS pay_amount
         |  FROM mall_bbc.t_ods_tz_order o
         |  JOIN mall_bbc.t_ods_tz_order_item oi ON o.order_number = oi.order_number
         |  WHERE o.is_payed = 'true'
         |    AND o.pay_time BETWEEN '2025-07-15 00:00:00' AND '2025-07-15 23:59:59'
         |    AND oi.rec_time BETWEEN '2025-07-15 00:00:00' AND '2025-07-15 23:59:59'
         |  GROUP BY oi.prod_id, o.shop_id
         |),
         |refund_single AS (
         |  SELECT
         |    oi.prod_id,
         |    oi.shop_id,
         |    r.refund_id,
         |    r.user_id,
         |    r.return_money_sts,
         |    r.refund_amount
         |  FROM mall_bbc.t_ods_tz_order_refund r
         |  LEFT JOIN mall_bbc.t_ods_tz_order_item oi ON r.order_item_id = oi.order_item_id
         |  WHERE r.apply_time BETWEEN '2025-07-15 00:00:00' AND '2025-07-15 23:59:59'
         |    AND r.refund_type = '2'
         |    AND oi.rec_time BETWEEN '2025-07-15 00:00:00' AND '2025-07-15 23:59:59'
         |),
         |refund_order AS (
         |  SELECT
         |    oi.prod_id,
         |    oi.shop_id,
         |    r.refund_id,
         |    r.user_id,
         |    r.return_money_sts,
         |    r.refund_amount
         |  FROM mall_bbc.t_ods_tz_order_refund r
         |  JOIN mall_bbc.t_ods_tz_order o ON r.order_id = o.order_id
         |  JOIN mall_bbc.t_ods_tz_order_item oi ON o.order_number = oi.order_number
         |  WHERE r.apply_time BETWEEN '2025-07-15 00:00:00' AND '2025-07-15 23:59:59'
         |    AND r.refund_type = '1'
         |    AND o.create_time BETWEEN '2025-07-15 00:00:00' AND '2025-07-15 23:59:59'
         |    AND oi.rec_time BETWEEN '2025-07-15 00:00:00' AND '2025-07-15 23:59:59'
         |),
         |refund_data AS (
         |  SELECT
         |    prod_id,
         |    shop_id,
         |    COUNT(DISTINCT refund_id) AS refund_num,
         |    COUNT(DISTINCT user_id) AS refund_person,
         |    COUNT(DISTINCT CASE WHEN return_money_sts = '5' THEN refund_id END) AS refund_success_num,
         |    COUNT(DISTINCT CASE WHEN return_money_sts = '5' THEN user_id END) AS refund_success_person,
         |    SUM(CAST(refund_amount AS DECIMAL(18,2))) AS refund_success_amount
         |  FROM (
         |    SELECT * FROM refund_single
         |    UNION ALL
         |    SELECT * FROM refund_order
         |  ) combined
         |  GROUP BY prod_id, shop_id
         |)
         |SELECT
         |  p.prod_id AS prod_id,
         |  p.shop_id AS shop_id,
         |  sd.shop_name AS shop_name,
         |  p.prod_name AS prod_name,
         |  CAST(p.price AS DECIMAL(18,2)) AS price,
         |  COALESCE(pe.expose_count, 0) AS expose,
         |  COALESCE(pe.expose_person_num, 0) AS expose_person_num,
         |  COALESCE(od.order_user_count, 0) AS place_order_person,
         |  COALESCE(od.order_item_count, 0) AS place_order_num,
         |  COALESCE(od.order_amount, 0) AS place_order_amount,
         |  COALESCE(pd.pay_user_count, 0) AS pay_person,
         |  COALESCE(pd.pay_num, 0) AS pay_num,
         |  COALESCE(pd.pay_amount, 0) AS pay_amount,
         |  COALESCE(rd.refund_num, 0) AS refund_num,
         |  COALESCE(rd.refund_person, 0) AS refund_person,
         |  COALESCE(rd.refund_success_num, 0) AS refund_success_num,
         |  COALESCE(rd.refund_success_person, 0) AS refund_success_person,
         |  COALESCE(rd.refund_success_amount, 0) AS refund_success_amount,
         |  CASE
         |    WHEN COALESCE(pe.expose_person_num, 0) > 0
         |    THEN ROUND(COALESCE(pd.pay_user_count, 0) / COALESCE(pe.expose_person_num, 0) * 100, 2)
         |    ELSE 0
         |  END AS single_prod_rate,
         |  CASE
         |    WHEN COALESCE(rd.refund_num, 0) > 0
         |    THEN ROUND(COALESCE(rd.refund_success_num, 0) / COALESCE(rd.refund_num, 0) * 100, 2)
         |    ELSE 0
         |  END AS refund_success_rate,
         |  p.status AS status,
         |  TO_DATE('2025-07-15') AS stat_date
         |FROM mall_bbc.t_ods_tz_prod p
         |LEFT JOIN product_exposure pe ON p.prod_id = pe.prod_id AND p.shop_id = pe.shop_id
         |LEFT JOIN order_data od ON p.prod_id = od.prod_id AND p.shop_id = od.shop_id
         |LEFT JOIN pay_data pd ON p.prod_id = pd.prod_id AND p.shop_id = pd.shop_id
         |LEFT JOIN refund_data rd ON p.prod_id = rd.prod_id AND p.shop_id = rd.shop_id
         |LEFT JOIN mall_bbc.t_ods_tz_shop_detail sd ON sd.shop_id = p.shop_id
         |WHERE p.status > '-1'
         |ORDER BY expose DESC
         |""".stripMargin
    val productAnalysisDF = spark.sql(productAnalysisSql)
    println("\n--- 6. 商品分析查询（HiveProductAnalysisQueryJob主SQL） ---")
    productAnalysisDF.show(false)

    spark.stop()
  }
} 