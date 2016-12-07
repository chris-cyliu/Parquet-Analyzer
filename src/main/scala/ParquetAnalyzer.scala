import org.apache.hadoop.fs._
import org.apache.parquet.column.page.DataPage.Visitor
import org.apache.parquet.column.page._
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

/**
  * Created by cscyliu on 7/12/2016.
  */

object ParquetAnalyzer {

  def main(args: Array[String]): Unit = {
    val parquetFilesUri = args(0)
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
      parquetReader.
      var pageReader: PageReadStore = null
      val columnDescriptors = parquetReader.getFooter.getFileMetaData.getSchema.getColumns
      while((pageReader = parquetReader.readNextRowGroup()) != null ) {
        // map a column to count map
        columnDescriptors.map({columnDescriptor =>
          pageReader.getPageReader(columnDescriptor)
          Coli
          })
        })
      }
    })

    //for each row in row group

    //init a map with [string -> map [ value -> count]

    //for each attribute
    //  increment the value in map
    //

  }

}
