import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scalaj.http._
import scala.xml.XML
import java.io.File
import scala.util.parsing.json._

object caseindex {
  def main(args:Array[String]) {

    //create the index and the mapping
    val CIindex = Http("http://localhost:9200/legal_idx?pretty").method("PUT").asString
    val data:String = """{"cases":{"properties":{"id":{"type":"text"},"name":{"type":"text"},"url":{"type":"text"},"person":{"type":"text"},"location":{"type":"text"},"organization":{"type":"text"},"catchphrase":{"type":"text"},"sentence":{"type":"text"}}}}"""
    val CImapping = Http("http://localhost:9200/legal_idx/cases/_mapping?pretty").postData(data).method("PUT").header("content-type", "application/json").asString

    //get the file_names in a iterator format
    val path: File = new File(args(0))
    if(path.exists() && path.isDirectory()){
      // set the XML files into RDD
      val conf = new SparkConf().setAppName("CaseIndex").setMaster("local")
      val sc = new SparkContext(conf)
      val Files = path.listFiles().filter{ f => f.getName.endsWith(".xml")}.toList.mkString(",")
      val SCFile = sc.wholeTextFiles(Files,1)

      // collect the files info and transform to XML format
      val XMLFile = SCFile.map(line => (new File(line._1.trim()).getName(),XML.loadString(line._2)))
      
      // process the RDD to the output format
      val Result = XMLFile.map(line => (line._1,XmlProc(line)))
      
      // the new doc to caes
      for(x <- Result) {
        val NewResult = Http("http://localhost:9200/legal_idx/cases/"+x._1+"?pretty").postData(x._2).method("PUT").header("Content-Type", "application/json").timeout(connTimeoutMs = 1000, readTimeoutMs = 60000).asString 
      }
    } else {
      return
    }
  }
  
  //define a XmlProc which process the XML file to final result format
  def XmlProc(a:(String, scala.xml.Elem)):String = {
    val name = (a._2 \\ "name").text
    val url = (a._2 \\ "AustLII").text
    val catchphrase = (a._2 \\ "catchphrases").text.trim().split("\n").map(x => x.trim()).mkString(" ").replace("\"","\\\"")
    val sentence = (a._2 \\ "sentences").text.trim().split("\n").map(x => x.trim()).mkString(" ").replace("\"","\\\"")
    
    // post the sentence to the NLP server and get the result,due to read time out, we need to set the timeout to 60s
    val NLPResult = Http("""http://localhost:9000/?properties=%7B'annotators':'ner','outputFormat':'json'%7D""").postData(sentence).method("POST").header("Content-Type", "application/json").timeout(connTimeoutMs = 1000, readTimeoutMs = 60000).asString.body
    
    //set the NLPResult to Json and collect the result for the cases
    val JResult = JSON.parseFull(NLPResult).get.asInstanceOf[Map[String, List[Map[String, Any]]]]
    var person = ""
    var location = ""
    var organization = ""
    for(x <- JResult("sentences")){
      val tokens = x("tokens").asInstanceOf[List[Map[String, String]]]
      for(y <- tokens) {
        if (y("ner") == "PERSON") {
          person = person + y("originalText") + " "
        } else if (y("ner") == "LOCATION") {
          location = location + y("originalText") + " "
        } else if (y("ner") == "ORGANIZATION") {
          organization = organization + y("originalText") + " "
        }
      }
    }
    val FileName = "f"+a._1
    val FinalJson = 
			s"""{"id":"$FileName","name":"$name",
			"url":"$url",
			"person":"$person",
			"location":"$location",
			"organization":"$organization",
			"catchphrases":"$catchphrase",
			"sentence":"$sentence"}"""
  return FinalJson
  }
}
