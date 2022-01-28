package schema

import com.fasterxml.jackson.annotation.JsonProperty

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class Schema()  {
  var processingTime:String=LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
  var dataSource:String="";
  def Transform(): Unit ={}
  override def toString: String = ",'processing_time'='" + processingTime  + "', data_Source='" + dataSource+"'";
}