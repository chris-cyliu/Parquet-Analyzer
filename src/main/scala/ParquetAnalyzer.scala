import org.apache.hadoop.fs._
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.column.page._
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetWriter}
import org.apache.spark.sql.SparkSession

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

    val spark = SparkSession.builder().getOrCreate()
    //Hadoop read all parquet file
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val listFileCursor = fs.listFiles(new Path(parquetFilesUri), true)
    val fileList = scala.collection.mutable.ArrayBuffer.empty[String]

    while(listFileCursor.hasNext){
      val file = listFileCursor.next()
      if(file.getPath.toUri.toString.contains(".parquet"))
        fileList += file.getPath.toUri.toString
    }

    //Parallel the parquet file
    spark.sparkContext.parallelize(fileList).map({ path =>
      val parquetReader = ParquetFileReader.open(spark.sparkContext.hadoopConfiguration, new Path(path))
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
          for( i <- 0 to column_reader.getTotalValueCount)
            column_reader.consume()
        })

        //write the result from multiset (value, count)
        for ( x <- 0 to columnDescriptors.size()){
          val column = columnConverters.getConverter(x).asInstanceOf[MultiSetConverter]
          val columnName = column.getColumnName
          val rowgroupId = s"${fileName}_$rowGroupCount"

          val tableOutputUri = new Path(outputFilesUri)
          val attributeUri = new Path(outputFilesUri, new Path(_attribute+"+"+columnName))
          val rowGroupUri = new Path(attributeUri, new Path(_rowgroup+"+",rowgroupId))
          val outputFile = new Path(rowGroupUri, new Path("statistic"))

          fs.mkdirs(rowGroupUri)

          val writer = new ParquetWriter(outputFile, spark.sparkContext.hadoopConfiguration, new MuliSetWriteSupport())
          ParquetWriter.Builder()

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
