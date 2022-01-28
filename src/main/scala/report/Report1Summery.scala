package report

import com.fasterxml.jackson.databind.ObjectMapper
import load.Log
import schema._

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map



class Report1Summery(var impressions:ListBuffer[Impression],var clicks:ListBuffer[Click]) {


  class Result(appID: Int,countryCode:String,id:String,impressions:Int,clicks:Int,revenue:Double)
  {
    var vAppID=appID;
    var vCountryCode=countryCode;
    var vID=id;
    var vImpressions=impressions;
    var vClicks=clicks;
    var vRevenue=revenue;
    override def toString: String = s"{\"app_id\": ${if(appID==null)"-" else appID}, \"country_code\": \"${if(countryCode==null)"-" else countryCode}\", \"impressions\": ${if(impressions==null)"-" else impressions }, \"clicks\": ${if(clicks==null)"-" else clicks }, \"revenue\": ${if(revenue==null)"-" else revenue }}";
  }


  var lResult:ListBuffer[Result]=ListBuffer[Result]();


  def Execute(): ListBuffer[Result] ={

    Log.Print("INFO",s"Report1Summery Exc start")

    var mpImpression:Map[String,ListBuffer[Impression]]=Map[String,ListBuffer[Impression]]();
    impressions.foreach(f =>{
      var tmpList=mpImpression.getOrElse(f.tfID,ListBuffer[Impression]());
      tmpList.addOne(f);
      mpImpression(f.tfID)=tmpList;
    })

    var mpClick:Map[String,ListBuffer[Click]]=Map[String,ListBuffer[Click]]();
    clicks.foreach(f =>{
      var tmpList=mpClick.getOrElse(f.tfImpressionID,ListBuffer[Click]());
      tmpList.addOne(f);
      mpClick(f.tfImpressionID)=tmpList;
    })

    var rList:ListBuffer[Result]=ListBuffer[Result]();
    var mpResult:Map[String,Result]=Map[String,Result]();
    mpImpression.foreach(im =>
    {
      var tmpList=mpImpression.getOrElse(im._1,null);
      if(tmpList!=null)
      {

        var appID:java.lang.Integer=null;
        var countryCode:java.lang.String=null;
        var id:java.lang.String=null;
        var impressions:Int=0;
        var clicks:Int=0;
        var revenue:Double=0;
        var check=0;

        im._2.foreach(iml=>{
          if(check==1 && id!=null &&(appID!=iml.tfAppID || id!=iml.tfID))
          {throw new Exception("Duplicate ID with diffrent appID and CountryCode");}
          appID=iml.tfAppID;
          countryCode=iml.tfCountryCode;
          id=iml.tfID;
          check=1;
          impressions+=1;
        })

        var tmpClicks=mpClick.getOrElse(im._1,null)

        if(tmpClicks!=null) {
          tmpClicks.foreach(ss=>{
            clicks+=1;
            revenue+=ss.tfRevenue;
          })

        }

        var tmp=mpResult.getOrElse(appID+"|"+countryCode,null);
        var res:Result=null;
        if(tmp!=null)
          {
            res=new Result(appID,countryCode,id,tmp.vImpressions+impressions,tmp.vClicks+clicks,tmp.vRevenue+revenue)
          }
          else {
            res = new Result(appID,countryCode, id,  impressions,  clicks,  revenue)
          }
        mpResult(appID+"|"+countryCode)=res;

      }
    })
    mpResult.foreach(m=>{
      rList.addOne(m._2);

    })
    lResult=rList;

    Log.Print("INFO",s"Report1Summery Exc done")

    return rList;
  }


  def ExportResult(filePath:String) :Unit=
  {
    var sResult="[\n";
    sResult+=lResult.mkString(",\n")
    sResult+="\n]";

    /*Validate Json file*/
    val objectMapper = new  ObjectMapper();
    objectMapper.readTree(sResult)

    val pw = new PrintWriter(new File(filePath));
    try pw.write(sResult) finally pw.close()
    Log.Print("INFO",s"Report1Summery result exported to ${filePath}")
  }


}
