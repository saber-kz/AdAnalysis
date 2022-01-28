package report

import com.fasterxml.jackson.databind.ObjectMapper
import load.Log
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{collect_list, concat_ws}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import schema.{Click, Impression}

import java.io.{File, FileWriter, PrintWriter}
import scala.collection.mutable.ListBuffer

class Report2Recommendation(var impressions:ListBuffer[Impression],var clicks:ListBuffer[Click]) {
  var dResult:List[Map[String,Any]]=null;
def Execute(): List[Map[String,Any]] = {

  System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0");
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  Log.Print("INFO",s"Report2Recommendation Exc Start")

  var llClicks: ListBuffer[Row] = ListBuffer[Row]();
  clicks.foreach(f => llClicks.addOne(Row(f.tfImpressionID, f.tfRevenue)))

  val dataClicks = llClicks.toList

  val schemaClicks = StructType(List(
    StructField("impression_id", StringType, true),
    StructField("revenue", DoubleType, true)
  ))

  val rddClicks = spark.sparkContext.parallelize(dataClicks)
  val dfClicks = spark.createDataFrame(rddClicks, schemaClicks)
  dfClicks.cache()
  dfClicks.createOrReplaceTempView("Clicks")

  var llImpressions: ListBuffer[Row] = ListBuffer[Row]();
  impressions.foreach(f => llImpressions.addOne(Row(f.tfID, f.tfAppID, f.tfCountryCode, f.tfAdvertiserID)))

  val dataImpressions = llImpressions.toList

  val schemaImpressions = StructType(List(
    StructField("id", StringType, true),
    StructField("app_id", IntegerType, true),
    StructField("country_code", StringType, true),
    StructField("advertiser_id", IntegerType, true),
  ))

  val rddImpressions = spark.sparkContext.parallelize(dataImpressions)
  val dfImpressions = spark.createDataFrame(rddImpressions, schemaImpressions)
  dfImpressions.cache()
  dfImpressions.createOrReplaceTempView("Impressions")

  /*Duplicate ID with different appID and CountryCode can impact result */
  if(spark.sql("SELECT ID,COUNT(1) CN FROM (SELECT DISTINCT ID,country_code,app_id FROM Impressions  where ID IS NOT NULL) GROUP BY ID HAVING COUNT(1)>1").count()>0)
    {Log.Print("ERROR",s"Duplicate ID with different appID and CountryCode")
      throw new Exception("Duplicate ID with different appID and CountryCode");}

  val dfResult=spark.sql(
    """SELECT *  FROM (
             SELECT app_id,country_code,advertiser_id,sum_rev/cn rate,sum_rev,cn, row_number() over (partition by app_id,country_code ORDER BY sum_rev/cn DESC) row_n
             FROM (
                    SELECT app_id,country_code,advertiser_id,SUM(sum_rev) sum_rev,SUM(cn) cn
                    FROM (
                          SELECT app_id,country_code,advertiser_id,id,COUNT(1) cn FROM Impressions GROUP BY app_id,country_code,advertiser_id,id
                          ) A
                    LEFT JOIN (
                          SELECT impression_id,SUM(revenue) sum_rev FROM Clicks GROUP BY impression_id
                          ) B
                    ON A.id=B.impression_id
                    GROUP BY app_id,country_code,advertiser_id
                    )
              )
              WHERE row_n<5 ORDER BY 1,2,row_n""").groupBy("app_id", "country_code")
    .agg(concat_ws(",", collect_list("advertiser_id")).alias("recommended_advertiser_ids"))
    var mpResult=dfResult.collect.map(r => Map(dfResult.columns.zip(r.toSeq):_*))
  dResult=mpResult.toList;

  Log.Print("INFO",s"Report2Recommendation Exc done")
/*
    spark.sql("""SELECT app_id,country_code,SUM(sum_rev) sum_rev,SUM(cn) cn,sum(cn_rev) cn_rev
      FROM (
      SELECT app_id,country_code,id,COUNT(1) cn FROM Impressions GROUP BY app_id,country_code,id
    ) A
      LEFT JOIN (
      SELECT impression_id,SUM(revenue) sum_rev,count(1) cn_rev  FROM Clicks GROUP BY impression_id
    ) B
      ON A.id=B.impression_id
  GROUP BY app_id,country_code
  order by 3 DESC""").show(1000)
*/
  //spark.sql("SELECT * FROM (SELECT * FROM Impressions where app_id=28 and  country_code is  null) A LEFT JOIN Clicks B ON A.id=B.impression_id").show(1000,false)
  //spark.sql("SELECT * FROM Clicks  WHERE impression_id IN (SELECT DISTINCT ID FROM Impressions where app_id=28 and  country_code is  null)").show(1000,false)
  //spark.sql("SELECT app_id,country_code,count(DISTINCT ID),COUNT(1) FROM Impressions  GROUP BY app_id,country_code").show(1000,false)
  //spark.sql("SELECT ID,COUNT(1) CN FROM (SELECT DISTINCT ID,country_code,app_id FROM Impressions where ID IS NOT NULL) GROUP BY ID HAVING COUNT(1)>1").show(1000,false)
  //spark.sql("SELECT country_code,app_id,count(distinct advertiser_id),count(1) FROM Impressions GROUP by country_code,app_id").show(1000,false)
  spark.close();
  return dResult;
}
  def ExportResult(filePath:String) :Unit=
  {
    var mpResult=dResult
    var sResult="[\n";
    mpResult.foreach(m=>{
      var appID:java.lang.String=m.getOrElse("app_id", null).toString;
      var countryCode:String=m.getOrElse("country_code", null).asInstanceOf[String];
      var recommendedAdvertiserIds:String=m.getOrElse("recommended_advertiser_ids", null).asInstanceOf[String];
      sResult=sResult+s"{\"app_id\": ${appID},\"country_code\": \"${countryCode}\",\"recommended_advertiser_ids\": [${recommendedAdvertiserIds}]},\n";
    })
    sResult=sResult.dropRight(2)+"\n]";

    /*Validate Json file*/
    val objectMapper = new  ObjectMapper();
    objectMapper.readTree(sResult)

    val pw = new PrintWriter(new File(filePath))
    try pw.write(sResult) finally pw.close()
    Log.Print("INFO",s"Report2Recommendation result exported to ${filePath}")
  }
}
