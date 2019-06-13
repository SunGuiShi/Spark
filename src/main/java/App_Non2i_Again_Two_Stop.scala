import org.apache.spark.sql.{SaveMode, SparkSession}

object App_Non2i_Again_Two_Stop {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(name = s"${this.getClass.getSimpleName}")
      .config("spark.debug.maxToStringFields", "100")
      .config("spark.yarn.executor.memoryOverhead", "2048")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    import spark.implicits._

    val this_month = args(0)
    val brand_code = Array("C10135","C10035","C10150","C15195","C10385","C10235","C10619","C10199")
    val circulation = Array("1030500","1030100","1030300","1030400","2030100","2030300") //商品渠道
    //is_income 是否出账 product_type 除了2 非2i2c用户 user_id A为CB B为B
    val pvc_code = Array("010", "011", "013", "017", "018", "019", "030", "031", "034",
      "036", "038", "050", "051", "059", "070", "071", "074", "075", "076",
      "079", "081", "083", "084", "085", "086", "087", "088", "089", "090", "091", "097")
    val arr = Array("bss","cbss","except")
    sql(sqlText = "use ods")

    val user_df = sql(sqlText = "select distinct serv_number,age,sex,province_code " +
      s"from ods_user_info_m where statis_month = $this_month  and length(serv_number) = 11")
    //添加性别 年龄
    //    user_df_1.createTempView(viewName = "user_view")

    val profile_df = sql(sqlText = "select distinct serv_number,acc_flow_month,brand,cur_income_month,pack_outer_call_time" +
      ",pack_inner_call_time,cur_product,case when substring(user_id,1,1) = 'A' then 'cbss' " +
      "when substring(user_id,1,1) = 'B' then 'bss' " +
      "else 'except' end as category " +
      "from ods_user_profile_m " +
      s"where statis_month = $this_month and length(serv_number) = 11 " +
      " and is_income = '1' and product_type <> '1' and channel_type in ('1030500','1030100','1030300','1030400'," +
      "'2030100','2030300') ")
      .cache()
    //     profile_df.createTempView(viewName = "profile_view")
    val app_score_df = sql(sqlText =s"select distinct serv_number,visit_cnt,total_flow,prod_id,prod_name from ods_app_score_m where statis_month = $this_month")

    val user_profile_df = profile_df.join(user_df, usingColumns =Seq("serv_number"),"left")

    //    val app_profile_df = app_score_df.join(profile_df,usingColumns =Seq("serv_number"),"full")
    val app_profile_df = profile_df.join(app_score_df,usingColumns =Seq("serv_number"),"inner")

    sql(sqlText = "use ads_mb")
    profile_df.write.mode(SaveMode.Overwrite).saveAsTable(tableName = s"ads_app_non_2i_basic_${this_month}_m")
    user_profile_df.write.mode(SaveMode.Overwrite).saveAsTable(tableName = s"ads_app_non_2i_1_${this_month}_m")
    app_profile_df.write.mode(SaveMode.Overwrite).saveAsTable(tableName = s"ads_app_non_2i_2_${this_month}_m")


    /**
      * 年龄段
      */
    //age age 0-120
    //sex 判断不男不女
    //    判断空 或者 age '/t'
    sql(sqlText = s"select category,count(distinct serv_number) m0 from " +
      s"ads_app_non_2i_1_${this_month}_m where age <= 14 and age > 0 group by category")
      .createTempView(viewName = "d_t_mf0")
    sql(sqlText = s"select category,count(distinct serv_number) m1 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 14 and age <= 18  group by category")
      .createTempView(viewName = "d_t_mf1")
    sql(sqlText = s"select category,count(distinct serv_number) m2 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 18 and age <= 35  group by category")
      .createTempView(viewName = "d_t_mf2")
    sql(sqlText = s"select category,count(distinct serv_number) m3 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 35 and age <= 55  group by category")
      .createTempView(viewName = "d_t_mf3")
    sql(sqlText = s"select category,count(distinct serv_number) m4 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 55 and age <= 70  group by category")
      .createTempView(viewName = "d_t_mf4")
    sql(sqlText = s"select category,count(distinct serv_number) m5 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 70 and age <= 120 group by category")
      .createTempView(viewName = "d_t_mf5")
    sql(sqlText = s"select category,count(distinct serv_number) m6 from " +
      s"ads_app_non_2i_1_${this_month}_m where age < 0 or age > 120 group by category")
      .createTempView(viewName = "d_t_mf6")   //年龄脏数据

    sql(sqlText = "select * from d_t_mf0")
      .join(sql(sqlText = "select * from d_t_mf1"), usingColumn = "category")
      .join(sql(sqlText = "select * from d_t_mf2"), usingColumn = "category")
      .join(sql(sqlText = "select * from d_t_mf3"), usingColumn = "category")
      .join(sql(sqlText = "select * from d_t_mf4"), usingColumn = "category")
      .join(sql(sqlText = "select * from d_t_mf5"), usingColumn = "category")
      .join(sql(sqlText = "select * from d_t_mf6"), usingColumn = "category")
      .write.mode(SaveMode.Overwrite).saveAsTable(tableName = s"ads_app_non_2i_sex_${this_month}_m")

    /**
      * 男女年龄对比
      */
    sql(sqlText = s"select category,count(distinct serv_number) m0 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 0 and age <= 14 and sex = 01  group by category")
      .createTempView(viewName = "t_m0")
    sql(sqlText = s"select category,count(distinct serv_number) m1 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 14 and age <= 18 and sex = 01  group by category")
      .createTempView(viewName = "t_m1")
    sql(sqlText = s"select category,count(distinct serv_number) m2 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 18 and age <= 35 and sex = 01  group by category")
      .createTempView(viewName = "t_m2")
    sql(sqlText = s"select category,count(distinct serv_number) m3 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 35 and age <= 55 and sex = 01  group by category")
      .createTempView(viewName = "t_m3")
    sql(sqlText = s"select category,count(distinct serv_number) m4 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 55 and age <= 70 and sex = 01  group by category")
      .createTempView(viewName = "t_m4")
    sql(sqlText = s"select category,count(distinct serv_number) m5 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 70 and age <= 120 and sex = 01  group by category")
      .createTempView(viewName = "t_m5")


    sql(sqlText = s"select category,count(distinct serv_number) f0 from " +
      s"ads_app_non_2i_1_${this_month}_m where age <= 14 and age >0 and sex = 02  group by category")
      .createTempView(viewName = "t_f0")
    sql(sqlText = s"select category,count(distinct serv_number) f1 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 14 and age <= 18 and sex = 02  group by category")
      .createTempView(viewName = "t_f1")
    sql(sqlText = s"select category,count(distinct serv_number) f2 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 18 and age <= 35 and sex = 02  group by category")
      .createTempView(viewName = "t_f2")
    sql(sqlText = s"select category,count(distinct serv_number) f3 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 35 and age <= 55 and sex = 02  group by category")
      .createTempView(viewName = "t_f3")
    sql(sqlText = s"select category,count(distinct serv_number) f4 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 55 and age <= 70 and sex = 02  group by category")
      .createTempView(viewName = "t_f4")
    sql(sqlText = s"select category,count(distinct serv_number) f5 from " +
      s"ads_app_non_2i_1_${this_month}_m where age > 70 and age <= 120 and sex = 02  group by category")
      .createTempView(viewName = "t_f5")
    sql(sqlText = s"select category,count(distinct serv_number) mf6 from " +
      s"ads_app_non_2i_1_${this_month}_m where age < 0 or age > 120 or sex <> 02 or sex <> 01 group by category")
      .createTempView(viewName = "t_mf0") //

    sql(sqlText = "select * from t_m0")
      .join(sql(sqlText = "select * from t_f0"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_m1"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_f1"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_m2"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_f2"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_m3"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_f3"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_m4"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_f4"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_m5"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_f5"), usingColumn = "category")
      .join(sql(sqlText = "select * from t_mf0"), usingColumn = "category")
      .write.mode(SaveMode.Overwrite).saveAsTable(tableName = s"ads_app_non_2i_sex_sct_${this_month}_m")

    /**
      * APP人群地区分布统计
      */

    for (i <- pvc_code.indices) {
      sql(sqlText = s"select category," +
        s"count(distinct serv_number) ct_${pvc_code(i)} from " +
        s"ads_app_non_2i_1_${this_month}_m where province_code = '${pvc_code(i)}' " +
        s"group by category")
        .createTempView(viewName = s"t_${pvc_code(i)}")
    }

    sql(sqlText = s"select * from t_${pvc_code(0)}")
      .join(sql(sqlText = s"select * from t_${pvc_code(1)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(2)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(3)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(4)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(5)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(6)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(7)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(8)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(9)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(10)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(11)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(12)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(13)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(14)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(15)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(16)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(17)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(18)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(19)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(20)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(21)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(22)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(23)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(24)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(25)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(26)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(27)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(28)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(29)}"),
        usingColumns = Seq("category"),"full_outer")
      .join(sql(sqlText = s"select * from t_${pvc_code(30)}"),
        usingColumns = Seq("category"),"full_outer")
      .write.mode(SaveMode.Overwrite).saveAsTable(tableName = s"ads_app_non_2i_province_${this_month}_m")

        /**
          * APP 用户的个人平均流量
          */
        //ods.tb_app_score_m.total_flow
      //0-100 G
        sql(sqlText = "select category,count(distinct serv_number) c_user,sum(acc_flow_month) s_flow from " +
          s"ads_app_non_2i_1_${this_month}_m where acc_flow_month > 0 and acc_flow_month < 102400 group by category ")
          .write.mode(SaveMode.Overwrite).saveAsTable(tableName = s"ads_app_non_2i_flow_${this_month}_m")

        /**
          * App终端品牌分布
          */
        // for 循环
        for( a <- brand_code.indices){
          sql(sqlText = s"select category,count(distinct serv_number) c_user from ads_app_non_2i_1_${this_month}_m " +
            s"where brand ='${brand_code(a)}' group by category") .write.mode(SaveMode.Append).saveAsTable(tableName = s"ads_app_non_2i_brand_${this_month}_m")
        }

        /**
          * App Arpu值分段
          */
    //300-1000

        sql(sqlText = s"select category,count(distinct serv_number) m0 from "+
          s"ads_app_non_2i_1_${this_month}_m where cur_income_month >=0.0 and cur_income_month <= 30.0 group by category")
          .createTempView(viewName = "t_m0_a")

        sql(sqlText = s"select category,count(distinct serv_number) m1 from "+
          s"ads_app_non_2i_1_${this_month}_m where cur_income_month > 30 and cur_income_month <= 50 group by category")
          .createTempView(viewName = "t_m1_a")

        sql(sqlText = s"select category,count(distinct serv_number) m2 from "+
          s"ads_app_non_2i_1_${this_month}_m where cur_income_month > 50 and cur_income_month <= 100 group by category")
          .createTempView(viewName = "t_m2_a")

        sql(sqlText = s"select category,count(distinct serv_number) m3 from "+
          s"ads_app_non_2i_1_${this_month}_m where cur_income_month > 100 and cur_income_month <= 150 group by category")
          .createTempView(viewName = "t_m3_a")

        sql(sqlText = s"select category,count(distinct serv_number) m4 from "+
          s"ads_app_non_2i_1_${this_month}_m where cur_income_month > 150 and cur_income_month <= 200 group by category")
          .createTempView(viewName = "t_m4_a")

        sql(sqlText = s"select category,count(distinct serv_number) m5 from "+
          s"ads_app_non_2i_1_${this_month}_m where cur_income_month > 200 and cur_income_month <= 250 group by category")
          .createTempView(viewName = "t_m5_a")

        sql(sqlText = s"select category,count(distinct serv_number) m6 from "+
          s"ads_app_non_2i_1_${this_month}_m where cur_income_month > 250 and cur_income_month <= 300 group by category")
          .createTempView(viewName = "t_m6_a")

        sql(sqlText = s"select category,count(distinct serv_number) m7 from "+
          s"ads_app_non_2i_1_${this_month}_m where cur_income_month > 300 and cur_income_month <= 1000 group by category")
          .createTempView(viewName = "t_m7_a")

        sql(sqlText = s"select category,count(distinct serv_number) m8 from "+
          s"ads_app_non_2i_1_${this_month}_m where cur_income_month < 0 or cur_income_month > 1000 group by category")
          .createTempView(viewName = "t_m8_a")

        sql(sqlText = "select * from t_m0_a")
          .join(sql(sqlText = "select * from t_m1_a"), usingColumn = "category")
          .join(sql(sqlText = "select * from t_m2_a"), usingColumn = "category")
          .join(sql(sqlText = "select * from t_m3_a"), usingColumn = "category")
          .join(sql(sqlText = "select * from t_m4_a"), usingColumn = "category")
          .join(sql(sqlText = "select * from t_m5_a"), usingColumn = "category")
          .join(sql(sqlText = "select * from t_m6_a"), usingColumn = "category")
          .join(sql(sqlText = "select * from t_m7_a"), usingColumn = "category")
          .join(sql(sqlText = "select * from t_m8_a"), usingColumn = "category")
          .write.mode(SaveMode.Overwrite).saveAsTable(tableName = s"ads_app_non_2i_arpu_${this_month}_m")

//        /**
//          * 八、套餐分析（全量、CB、B出三个表）
//          */
//
//
//        sql(sqlText = s"select cur_product,count(distinct serv_number) as ct0 from ads_app_non_2i_1_${this_month}_m " +
//          "where category = 'bss' group by cur_product").
//          union(sql(sqlText = s"select cur_product,count(distinct serv_number) as ct1 from ads_app_non_2i_1_${this_month}_m " +
//          "where category = 'cbss' group by cur_product"))
//
//          .write.mode(SaveMode.Overwrite)
//          .saveAsTable(tableName = s"ads_app_non_2i_plan_${this_month}_m")
//
        /**
          * 四、用户平均通话时长（分钟）
          */
      //分钟数 套内or套外 0-10000
        sql(sqlText = "select category,count(distinct serv_number) c_user," +
          s"sum((pack_outer_call_time + pack_inner_call_time)) s_call from ads_app_non_2i_1_${this_month}_m " +
          "where pack_outer_call_time > 0 and pack_outer_call_time < 10000 and " +
          "pack_inner_call_time > 0 and pack_inner_call_time < 10000 and " +
          "pack_outer_call_time is not null and pack_inner_call_time is not null group by category")
          .write.mode(SaveMode.Overwrite)
          .saveAsTable(tableName = s"ads_app_non_2i_call_${this_month}_m")


    /**
      * 七、用户访问行为分析（全量、CB、B出三个表）
      */
    //    app_df
    //      .groupBy("category").count()
    //      .orderBy($"count".desc)
    //      .limit(5)
    //      .collect()
    //      .map(arr += _.get(0))
    //    user_profile_join.select("cur_product", "prod_id").orderBy($"visit_cnt".desc).limit(10)

    //    ads_app_non_2i_3_${this_month}_m
    //
    //        app_df.groupBy("category").count()
    //          .orderBy($"visit_cnt".desc).limit(10)
    //          .join(sql(sqlText = "select category,sum(visit_cnt)/count(distinct serv_number) avg_vst_cnt from " +
    //            s"ads_app_non_2i_${this_month}_m group by category"), usingColumn = "category")
    //          .join(sql(sqlText = "select category,sum(total_flow)/count(distinct serv_number) avg_flow from " +
    //            s"ads_app_non_2i_${this_month}_m group by category"), usingColumn = "category")
    //          .write.mode(SaveMode.Overwrite).saveAsTable(tableName = s"ads_app_non_2i_action_${this_month}_m")
    //
    //        for (i <- arr.indices) {
    //          app_df
    //            .select("category", "prod_id")
    //            .distinct()
    //            .where(s"category = ${arr(i)}")
    //            .orderBy($"visit_cnt".desc)
    //            .limit(10)
    //            .join(sql(sqlText = "select category,sum(visit_cnt)/count(distinct serv_number) avg_vst_cnt from " +
    //              s"ads_app_non_2i_${this_month}_m group by category"), usingColumns = Seq("category"),"full_outer")
    //            .join(sql(sqlText = "select category,sum(total_flow)/count(distinct serv_number) avg_flow from " +
    //              s"ads_app_non_2i_${this_month}_m group by category"), usingColumns = Seq("category"),"full_outer")
    //            .write.mode(SaveMode.Append).saveAsTable(tableName = s"ads_app_non_2i_action_${this_month}_m")
    //        }

    spark.stop()
  }
}
