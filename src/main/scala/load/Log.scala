package load
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
object Log {
  def Print(lType:String,lText:String)=
  {
    var processingTime:String=LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"));
    println(s"${processingTime} | ${lType.toUpperCase} | ${lText}")
  }
}
