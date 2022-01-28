package load

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import scala.reflect.classTag
import scala.collection.mutable.ListBuffer
import schema.Schema
import java.io.File

class JsonParser[T <:Schema ](var filePath:String)(implicit m: Manifest[T]) {

  def Parse():ListBuffer[T]= {
    Log.Print("INFO",s"Start parsing ${filePath}")
    var list: ListBuffer[T] = new ListBuffer[T];
    try {/* Handle file level exceptions */
      val mapper = new ObjectMapper();
      val matrix: JsonNode = mapper.readValue(new File(filePath), classOf[JsonNode]);

      var totalRow=0;
      var successRow=0;
      var rejectedRow=0;
      matrix.forEach(node => {
        totalRow+=1;
        try { /* Handle node level exceptions */
          val objectMapper = new ObjectMapper();
          var nod = objectMapper.readValue(node.toString, classTag[T].runtimeClass).asInstanceOf[T];
          nod.Transform();
          nod.dataSource = filePath;
          list.addOne(nod);
          successRow += 1;
        } catch {case e: Exception => {rejectedRow+=1
          Log.Print("WARNING",s"Record Rejected Node: ${totalRow} --- object:{ ${node.toString} }--- Error: ${e.toString}")
        }}
      })
      Log.Print("INFO",s"totalRow ${totalRow}, successRow ${successRow}, rejectedRow ${rejectedRow}")

    } catch {
      //case e: com.fasterxml.jackson.core.io.JsonEOFException => print(e.toString)
      case e: Exception => {Log.Print("ERROR",s"Start parsing ${e.toString}"); throw new Exception(e)}
    }
    Log.Print("INFO",s"Finish parsing ${filePath}")
    return list;
  }
}
