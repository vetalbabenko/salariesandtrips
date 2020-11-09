import java.io.PrintWriter
import java.util.logging.Logger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


class HadoopService(hdfsPath: String) {

  private val conf = new Configuration()
  conf.set("fs.defaultFS", hdfsPath)
  private val fs = FileSystem.get(conf)

  def save(data: String, fileName: String = "output.csv"): Unit = {
    println(s"Started saving data: $data to hdfs")
    val output = fs.create(new Path(s"/tmp/$fileName"))
    val writer = new PrintWriter(output)
    try {
      writer.write(data)
      writer.write("\n")
      println(s"Successfully save data")
    }catch {
      case e => println(s"Got an error while saving data to hdfs: $e")
    }
    finally {
      writer.close()
    }

  }
}
