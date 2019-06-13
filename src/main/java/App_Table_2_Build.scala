import org.apache.spark.sql.{SaveMode, SparkSession}

object App_Table_2_Build {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(name = s"${this.getClass.getSimpleName}")
      .config("spark.debug.maxToStringFields", "100")
      .config("spark.yarn.executor.memoryOverhead", "2048")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    val this_month = args(0)
    sql(sqlText = "use ods")

    sql(sqlText =
      s"""
        |select serv_number,cur_income_month,statis_month,brand,
        |is_income,product_type,
        |user_id,channel_type,cur_product
        |from ods_user_profile_m
        |where statis_month = $this_month and is_income = '1' and
        |channel_type in ('1030500','1030100','1030300','1030400','2030100','2030300')
        |and length(serv_number) = 11
      """.stripMargin)
      .join(sql(sqlText =
        s"""
          |select distinct serv_number,sex,age,province_code from ods_user_info_m
          |where statis_month = $this_month and length(serv_number) = 11
        """.stripMargin ), usingColumn = "serv_number")
      .createTempView(viewName = "tmp_view")

    val app_df = sql(sqlText = "select serv_number,cur_income_month,statis_month,sex,brand,is_income,product_type, " +
      "user_id,channel_type,cur_product,age,province_code, "+
      "case when substring(user_id,1,1) = 'A' then 'cbss' " +
      "when substring(user_id,1,1) = 'B' then 'bss' " +
      "else 'except' end as category from tmp_view")

    sql(sqlText = "use ads_mb")
    app_df.write.mode(SaveMode.Overwrite).saveAsTable(tableName = s"ads_app_non_2i_${this_month}_m")
    spark.stop()
  }
}
