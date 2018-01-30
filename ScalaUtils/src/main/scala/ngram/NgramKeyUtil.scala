package ngram

import org.apache.spark.SparkException
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.api.python.Converter

/**
  * A converter that converts common types to [[org.apache.hadoop.io.Writable]]. Note that array
  * types are not supported since the user needs to subclass [[org.apache.hadoop.io.ArrayWritable]]
  * to set the type properly. See [[org.apache.spark.api.python.DoubleArrayWritable]] and
  * [[org.apache.spark.api.python.DoubleArrayToWritableConverter]] for an example. They are used in
  * PySpark RDD `saveAsNewAPIHadoopFile` doctest.
  */

class NgramKeyConverter extends Converter[Any, ImmutableBytesWritable] {

  /**
    * Converts common data types to [[org.apache.hadoop.io.Writable]]. Note that array types are not
    * supported out-of-the-box.
    */
  def convertToImmutableBytesWritable(obj: Any): ImmutableBytesWritable = {
    obj match {
      case s: java.lang.String => new ImmutableBytesWritable(Bytes.toBytes(s))
      case aob: Array[Byte] => new ImmutableBytesWritable(aob)
      case other => throw new SparkException(
        s"Data of type ${other.getClass.getName} cannot be used")
    }
  }

  override def convert(obj: Any): ImmutableBytesWritable = obj match {
    case writable: ImmutableBytesWritable => writable
    case other => convertToImmutableBytesWritable(other)
  }
}



object NgramKeyUtil {
  def main(args: Array[String]): Unit = {
    println()
  }
}