import java.io.{BufferedWriter, ObjectInputStream, ObjectOutputStream, OutputStreamWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.{IntWritable, LongWritable, SequenceFile, Text}
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.column.page._
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetWriter}
import org.apache.parquet.io.api.{Binary, PrimitiveConverter}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{PrimitiveType, Types}
import org.apache.parquet.schema.Types.MessageTypeBuilder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
  * Created by cscyliu on 7/12/2016.
  */

object ParquetAnalyzer {

  val _attribute = "attribute"
  var _rowgroup = "rowgroup"
  val _value = "vale"
  val _count = "count"

  def main(args: Array[String]): Unit = {

    val parquetFilesUri = args(0)
    val outputFilesUri = args(1)

    val spark = SparkSession.builder().getOrCreate()

    //Hadoop read all parquet file
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val listFileCursor = fs.listFiles(new Path(parquetFilesUri), true)
    val fileList = scala.collection.mutable.ArrayBuffer.empty[String]

    val hadoopConf = spark.sparkContext.broadcast(new SerializableConfiguration(spark.sparkContext.hadoopConfiguration))
      .asInstanceOf[Broadcast[SerializableConfiguration]]

    while(listFileCursor.hasNext){
      val file = listFileCursor.next()
      if(file.getPath.toUri.toString.contains(".parquet"))
        fileList += file.getPath.toUri.toString
    }

    //Parallel the parquet file
    spark.sparkContext.parallelize(fileList).foreach({ path =>
      val conf = hadoopConf.value.value
      val fs = FileSystem.get(conf)
      val parquetReader = ParquetFileReader.open(conf, new Path(path))
      var pageReadStore: PageReadStore = null
      val columnDescriptors = parquetReader.getFooter.getFileMetaData.getSchema.getColumns

      val fileName = parquetReader.getPath.getName
      var rowGroupCount = 0

      while({pageReadStore = parquetReader.readNextRowGroup(); pageReadStore != null}) {

        //Start process a row group
        val columnConverters = new CountValueConvertor(parquetReader.getFileMetaData.getSchema)
        val columnStore = new ColumnReadStoreImpl(pageReadStore, columnConverters, parquetReader.getFileMetaData.getSchema, parquetReader.getFileMetaData.getCreatedBy)

        columnDescriptors.asScala.zipWithIndex.foreach({case (columnDescriptor: ColumnDescriptor, ind :Int ) =>

          val columnConverter = columnConverters.getConverter(ind).asInstanceOf[PrimitiveConverter]
          //Start process a column
          val column_reader = columnStore.getColumnReader(columnDescriptor)

          for( i <- 0L until column_reader.getTotalValueCount){
            //start process a column
            columnDescriptor.getType match {
              case PrimitiveTypeName.BINARY =>
                columnConverter.addBinary(column_reader.getBinary())
              case PrimitiveTypeName.BOOLEAN =>
                columnConverter.addBoolean(column_reader.getBoolean())
              case PrimitiveTypeName.INT64 =>
                columnConverter.addLong(column_reader.getLong())
              case PrimitiveTypeName.INT32 =>
                columnConverter.addInt(column_reader.getInteger())
            }
            column_reader.consume()
          }
        })

        //write the result from multiset (value, count)
        for ( x <- 0 until columnDescriptors.size()){
          val column = columnConverters.getConverter(x).asInstanceOf[CountSetConverter]
          val columnName = column.getColumnName
          val rowgroupId = s"${fileName}_$rowGroupCount"

          val tableOutputUri = new Path(outputFilesUri)
          val attributeUri = new Path(outputFilesUri, new Path(_attribute+"="+columnName))
          val rowGroupUri = new Path(attributeUri, new Path(_rowgroup+"="+rowgroupId))
          val outputFile = new Path(rowGroupUri, new Path("statistic"))

          if ( fs.exists(outputFile)) {
            fs.delete(outputFile,false)
          }

          fs.mkdirs(rowGroupUri)

          //writeParquet(column, outputFile)
          writeCSV(column, outputFile, fs, conf)
        }

        rowGroupCount += 1
        //End process a row group
      }
    })
  }
  def writeParquet(column: CountSetConverter, outputFile: Path) = {
    val schema = Types.buildMessage()
      .required(PrimitiveTypeName.BINARY).named("value")
      .required(PrimitiveTypeName.INT64).named("count")
      .named("DistinctValueCount")

    class ParquetWriterBuilder() extends
      ParquetWriter.Builder[CountSet, ParquetWriterBuilder](outputFile) {
      override def getWriteSupport(conf: Configuration) = new MuliSetWriteSupport(schema)

      override def self() = this
    }

    val writer = new ParquetWriterBuilder().build()
    writer.write(column.getCount())
    writer.close()
  }

  def writeCSV(column: CountSetConverter, outputFile: Path, fs: FileSystem, conf: Configuration) = {
    val os = fs.create(outputFile)
    val br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
    column.getCount.valueSet().foreach({ value =>
      val count = column.getCount.count(value)
      br.write(s"${value.toString}|${count}\n")
    })
    br.close()
  }
}

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    value = new Configuration(false)
    value.readFields(in)
  }
}

class CountSet(){

  private val map = scala.collection.mutable.LinkedHashMap.empty[String, Long]

  def add ( value:String ) = {
    var count = 0L
    if ( map.contains(value) ){
      count = map(value)
    }
    count += 1
    map += value -> count
  }

  def count(value:String) = {
    map(value)
  }

  def valueSet() : scala.collection.immutable.Set[String] = {
    map.keySet.toSet
  }

  def valueSetJava() : java.util.Set[String] = {
    map.keySet.asJava
  }
}