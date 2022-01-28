package schema

import com.fasterxml.jackson.annotation.JsonProperty

class Impression(@JsonProperty(value = "id", required = false) id: String,
                 @JsonProperty(value = "app_id", required = false) appID: String,
                 @JsonProperty(value = "advertiser_id", required = false) advertiserID: String,
                 @JsonProperty(value = "country_code", required = false) countryCode: String ) extends Schema {

  var rowID:String=id;
  var rowAppID:String=appID;
  var rowAdvertiserID:String=advertiserID;
  var rowCountryCode:String=countryCode;

  var tfID:java.lang.String=null;
  var tfAppID:java.lang.Integer=null;
  var tfAdvertiserID:java.lang.Integer=null;
  var tfCountryCode:java.lang.String=null;

  override def Transform(): Unit ={
    tfID= if(rowID!=null &&(rowID==null || rowID.toUpperCase()=="NULL" || rowID=="" || rowID.toUpperCase()=="NA")) null else rowID;

    try {
      tfAppID = if(rowAppID!=null &&(rowAppID.toUpperCase()=="NULL" || rowAppID=="" || rowAppID.toUpperCase()=="NA")) null else rowAppID.toInt;
    }
    catch {case nfe: NumberFormatException => tfAppID = null}

    try {
      tfAdvertiserID = if(rowAdvertiserID!=null &&(rowAdvertiserID.toUpperCase()=="NULL" || rowAdvertiserID=="" || rowAdvertiserID.toUpperCase()=="NA")) null else rowAdvertiserID.toInt}
    catch {
      case nfe: NumberFormatException => tfAdvertiserID = null}

    tfCountryCode = if(rowCountryCode!=null &&(rowCountryCode.toUpperCase()=="NULL" || rowCountryCode=="" || rowCountryCode.toUpperCase()=="NA")) null else rowCountryCode

  }

  override def toString: String = "{" + "'id='" + id  + "', 'app_id=" + appID+", advertiser_id=" + advertiserID+", country_code='" + countryCode+"'"+super.toString()+"'}";
}