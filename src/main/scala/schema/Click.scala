package schema

import com.fasterxml.jackson.annotation.JsonProperty

class Click(@JsonProperty(value = "impression_id", required = true) impressionID: String,
            @JsonProperty(value = "revenue", required = true) revenue: String) extends Schema {

  var rowImpressionID:String=impressionID;
  var rowRevenue:String=revenue

  var tfImpressionID:java.lang.String=null;
  var tfRevenue:java.lang.Double=null;

  override def Transform(): Unit ={

    tfImpressionID= if(rowImpressionID==null || rowImpressionID.toUpperCase()=="NULL" || rowImpressionID=="" || rowImpressionID.toUpperCase()=="NA") null else rowImpressionID;
    /*Reject wrong nodes*/
    if (tfImpressionID==null) throw new Exception(s"ImpressionID: is not valid")
    try {
      tfRevenue = if(rowRevenue!=null &&(rowRevenue.toUpperCase()=="NULL" || rowRevenue=="" || rowRevenue.toUpperCase()=="NA")) null else rowRevenue.toDouble}
    catch {
      case nfe: NumberFormatException => tfRevenue = null}
    /*Reject wrong nodes*/
    if (tfRevenue==null) throw new Exception(s"Revenue: is not valid")
  }

  override def toString: String = "{" + "'impression_id'='" + impressionID  + "', revenue='" + revenue+"'"+super.toString()+"'}";
}