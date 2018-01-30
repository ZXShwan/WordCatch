package ngram

import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkException
import org.apache.spark.api.python.Converter
import org.json4s._
import org.json4s.jackson.JsonMethods._

class StatInfo(example: List[String],pos: String, count: Int)

class NgramCellConverter extends Converter[Any, Cell] {

  def convertToCell(obj:String):Cell = {
    implicit val formats = org.json4s.DefaultFormats
    val immediate_json = parse(obj)
    val extracted_json = immediate_json.extract[Map[String,Map[String,Map[String,StatInfo]]]]
    val saltedRowKey = extracted_json.keysIterator.next()
    val columnFamily = extracted_json.valuesIterator.next().keysIterator.next()
    val candidate = extracted_json.valuesIterator.next().valuesIterator.next().keysIterator.next()
    val column = extracted_json.valuesIterator.next().valuesIterator.next().keysIterator.next()
    val cellData = compact(render(immediate_json \ saltedRowKey \ columnFamily \ candidate))
    val cell = new KeyValue(
      Bytes.toBytes(saltedRowKey),
      Bytes.toBytes(columnFamily),  // column familily
      Bytes.toBytes(column), // column qualifier (i.e. cell name)
      Bytes.toBytes(cellData)
    )
    cell
  }

  override def convert(obj: Any): Cell = obj match {
    case cell: Cell => cell
    case s: String => convertToCell(s)
    case other => throw new SparkException(
      s"Data of type ${other.getClass.getName} cannot be used")
  }
}

object NgramValueUtil {
  def main(args: Array[String]): Unit = {
    val json = """{"3:used,to": {"1": {"is": {"pos": "VBZ", "count": 7, "example": [" \"Libungan\" is used to refer to the river located in the area by the Manobo which means cheater", " This move is used to test defender's defensive weaknesses and stance", "  A pull buoy or \"leg float\" is used to focus exercise on the arms"]}}}}"""
    val converter = new NgramCellConverter
    val cell = converter.convert(json)
  }
}
