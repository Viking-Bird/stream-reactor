package com.landoop.streamreactor.connect.hive.orc

import com.landoop.streamreactor.connect.hive.orc.vectors.{OrcVectorWriter, StructVectorWriter}
import com.landoop.streamreactor.connect.hive.{OrcSinkConfig, StructUtils}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.collection.JavaConverters._

class OrcSink(path: Path,
              schema: Schema,
              config: OrcSinkConfig)(implicit fs: FileSystem) extends StrictLogging {

  private val typeDescription = OrcSchemas.toOrc(schema)
  private val structWriter = new StructVectorWriter(typeDescription.getChildren.asScala.map(OrcVectorWriter.fromSchema))
  // 创建VectorizedRowBatch对象
  private val batch = typeDescription.createRowBatch(config.batchSize)
  private val vector = new StructColumnVector(batch.numCols, batch.cols: _*)
  // 创建ORC Writer对象
  private val orcWriter = createOrcWriter(path, typeDescription, config)
  private var n = 0

  /**
    * 清空ORC writer中的状态信息，下一次写入时重新记录状态信息
    */
  def flush(): Unit = {
    logger.info(s"Writing orc batch [size=$n, path=$path]")
    batch.size = n
    orcWriter.addRowBatch(batch)
    orcWriter.writeIntermediateFooter
    batch.reset()
    n = 0
  }

  /**
    * 写入ORC记录
    *
    * @param struct
    */
  def write(struct: Struct): Unit = {
    structWriter.write(vector, n, Some(StructUtils.extractValues(struct)))
    n = n + 1
    // If the batch is full, write it out and start over.
    if (n == config.batchSize)
      flush()
  }

  /**
    * 清空ORC writer中的记录，刷新数据到hdfs
    */
  def close(): Unit = {
    if (n > 0)
      flush()
    //  When the file is done, close the Writer.
    // 关闭ORC writer，刷新数据到hdfs
    orcWriter.close()
  }
}
