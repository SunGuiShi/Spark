import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}

object test {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\App_Pro\\hadoop");
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    import spark.sql
    import spark.implicits._
    spark.read.option("header", "true").csv("D:\\My_Project\\data\\telecom_churn.csv").createTempView("table")

    val frame2 = sql("select count(1) from table")

    frame2.show()
    spark.stop()
  }
}
