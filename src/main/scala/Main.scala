
import schema.{Click, Impression}
import report._
import load._

object Main extends App {

  /* WorkDirectory that contain all source files and Reports will export there*/
  var sWorkingDir="D:\\AD\\app\\"

  /* parse impressions.json data and return them as list of Impression object*/
  var pImpressions=new JsonParser[Impression](sWorkingDir+"impressions.json");
  var lImpressions=pImpressions.Parse();

  /* parse clicks.json data and return them as list of Clicks object*/
  var pClicks=new JsonParser[Click](sWorkingDir+"clicks.json");
  var lClicks=pClicks.Parse();

  /* use list of Impression and Clicks to generate summery report (Report1)*/
  var report1Summery=new Report1Summery(lImpressions,lClicks)
  report1Summery.Execute();
  report1Summery.ExportResult(sWorkingDir+"Report1.json")

  /* use list of Impression and Clicks to generate recommendation report (Report1) this process use spark*/
  var report2Recommendation=new Report2Recommendation(lImpressions,lClicks);
  report2Recommendation.Execute();
  report2Recommendation.ExportResult(sWorkingDir+"Report2.json")


}



