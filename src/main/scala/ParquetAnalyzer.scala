import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.column.page._
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetWriter}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{PrimitiveType, Types}
import org.apache.parquet.schema.Types.MessageTypeBuilder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.spark_project.guava.collect.Multiset

import scala.collection.JavaConversions._

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

    val spark = SparkSession.builder().master("local[4]").getOrCreate()

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
      val fs = FileSystem.get(hadoopConf.value.value)
      val parquetReader = ParquetFileReader.open(hadoopConf.value.value, new Path(path))
      var pageReadStore: PageReadStore = null
      val columnDescriptors = parquetReader.getFooter.getFileMetaData.getSchema.getColumns

      val fileName = parquetReader.getPath.getName
      var rowGroupCount = 0

      while((pageReadStore = parquetReader.readNextRowGroup()) != null ) {

        //Start process a row group
        val columnConverters = new CountValueConvertor(parquetReader.getFileMetaData.getSchema)
        val columnStore = new ColumnReadStoreImpl(pageReadStore, columnConverters, parquetReader.getFileMetaData.getSchema, parquetReader.getFileMetaData.getCreatedBy)

        // map a column to count map
        columnDescriptors.map({columnDescriptor =>
          val column_reader = columnStore.getColumnReader(columnDescriptor)
          for( i <- 0L until column_reader.getTotalValueCount)
            column_reader.consume()
        })

        //write the result from multiset (value, count)
        for ( x <- 0 until columnDescriptors.size()){
          val column = columnConverters.getConverter(x).asInstanceOf[MultiSetConverter[Object]]
          val columnName = column.getColumnName
          val rowgroupId = s"${fileName}_$rowGroupCount"

          val tableOutputUri = new Path(outputFilesUri)
          val attributeUri = new Path(outputFilesUri, new Path(_attribute+"="+columnName))
          val rowGroupUri = new Path(attributeUri, new Path(_rowgroup+"="+rowgroupId))
          val outputFile = new Path(rowGroupUri, new Path("statistic"))

          fs.mkdirs(rowGroupUri)

          val schema = Types.buildMessage()
            .required(PrimitiveTypeName.BINARY).named("value")
            .required(PrimitiveTypeName.INT64).named("count")
            .named("DistinctValueCount")

          class ParquetWriterBuilder() extends
            ParquetWriter.Builder[Multiset[Object], ParquetWriterBuilder](outputFile) {
            override def getWriteSupport(conf: Configuration) = new MuliSetWriteSupport(schema)

            override def self() = this
          }

          val writer = new ParquetWriterBuilder().build()
          writer.write(column.getCount().asInstanceOf[Multiset[Object]])
          writer.close()
        }

        rowGroupCount += 1
        //End process a row group
      }
    })

    //for each row in row group

    //init a map with [string -> map [ value -> count]

    //for each attribute
    //  increment the value in map
    //

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