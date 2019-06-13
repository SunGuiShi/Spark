import org.apache.spark.sql.{SaveMode, SparkSession}

object App_Table_Build {
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
    val app_code = Array("C1223", "C545", "C4453")
    val brand_code = Array("C10135", "C10035", "C10150", "C15195", "C10385", "C10235", "C10619", "C10199")
    val circulation = Array("1030500", "1030100", "1030300", "1030400", "2030100", "2030300") //商品渠道
    //is_income 是否出账 product_type 除了2 非2i2c用户 user_id A为CB B为B
    val pvc_code = Array("010", "011", "013", "017", "018", "019", "030", "031", "034",
      "036", "038", "050", "051", "059", "070", "071", "074", "075", "076",
      "079", "081", "083", "084", "085", "086", "087", "088", "089", "090", "091", "097")
    sql(sqlText = "use ods")

//
//    val app_df = sql(sqlText = s"select distinct t2.serv_number ,t1.channel_type ,t2.sex, " +
//      s"case when t1.user_id like concat('A','%') = true then 'cbss' " +
//      s"when t1.user_id like concat('B','%') = true then 'bss' " +
//      s"else 'except' end as category " +
//      s"from (select serv_number,cur_income_month,statis_month,brand,is_income,product_type,user_id,channel_type,category " +
//      s"from ods_user_profile_m " +
//      s"where statis_month = $this_month and is_income = '1' and " +
//      s"channel_type in ('1030500','1030100','1030300','1030400','2030100','2030300') " +
//      s"and length(serv_number) = 11) t1 join " +
//      s"(select serv_number,sex,age,province_code from ods_user_info_m where statis_month = $this_month and length(serv_number) = 11) t2 " +
//      s"on t1.serv_number = t2.serv_number")


    spark.stop()
  }
}
