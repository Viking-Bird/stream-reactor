package com.landoop.streamreactor.connect.hive.sink

import com.landoop.streamreactor.connect.hive.formats.{HiveFormat, HiveWriter}
import com.landoop.streamreactor.connect.hive.sink.staging.StageManager
import com.landoop.streamreactor.connect.hive.{Offset, TopicPartition, TopicPartitionOffset}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.Schema

/**
  * Manages the lifecycle of [[HiveWriter]] instances.
  *
  * A given sink may be writing to multiple locations (partitions), and therefore
  * it is convenient to extract this to another class.
  *
  * This class is not thread safe as it is not designed to be shared between concurrent
  * sinks, since file handles cannot be safely shared without considerable overhead.
  *
  * 负责管理HiveWriter实例的生命周期。一个给定的sink可以写入多个位置(分区)，因此将其提取到另一个类中很方便。
  * 这个类不是线程安全的，因为它不是设计为在并发接收器之间共享的，因为在没有大量开销的情况下，文件句柄不能安全地共享。
  */
class HiveWriterManager(format: HiveFormat,
                        stageManager: StageManager)
                       (implicit fs: FileSystem) extends StrictLogging {

  case class WriterKey(tp: TopicPartition, dir: Path)

  private val writers = scala.collection.mutable.Map.empty[WriterKey, (Path, HiveWriter)]

  private def createWriter(dir: Path,
                           tp: TopicPartition,
                           schema: Schema): (Path, HiveWriter) = {
    val path = stageManager.stage(dir, tp)
    logger.debug(s"Staging new writer at path [$path]")
    val writer = format.writer(path, schema)
    (path, writer)
  }

  /**
    * Returns a writer that can write records for a particular topic and partition.
    * The writer will create a file inside the given directory if there is no open writer.
    *
    * 给指定的topic和分区返回可以写入记录的writer。如果没有打开的writer，则在指定的目录中创建文件
    */
  def writer(dir: Path, tp: TopicPartition, schema: Schema): (Path, HiveWriter) = {
    writers.getOrElseUpdate(WriterKey(tp, dir), createWriter(dir, tp, schema))
  }

  /**
    * Flushes the open writer for the given topic partition and directory.
    *
    * Next time a writer is requested for the given (topic,partition,directory), a new
    * writer will be created.
    *
    * The directory is required, as there may be multiple writers, one per partition.
    * The offset is required as part of the commit filename.
    *
    * 刷新指定的topic partition and directory对应的已打开的writer。下次再请求指定的topic、partition、directory对应的writer时，将创建一个新的writer。
    * 目录是必需的，因为可能有多个写入器，每个分区一个，该偏移量是提交文件名的一部分。
    */
  def flush(tpo: TopicPartitionOffset, dir: Path): Unit = {
    logger.info(s"Flushing writer for $tpo")
    val key = WriterKey(tpo.toTopicPartition, dir)
    writers.get(key).foreach { case (path, writer) =>
      writer.close() // 写入orc记录
      stageManager.commit(path, tpo) // 提交文件
      writers.remove(key) // 移除HiveWriter
    }
  }

  /**
    * Flushes all open writers.
    *
    * @param offsets the offset for each [[TopicPartition]] which is required
    *                by the commit process.
    */
  def flush(offsets: Map[TopicPartition, Offset]): Unit = {
    logger.info(s"Flushing offsets $offsets")
    // we may not have an offset for a given topic/partition if no data was written to that TP
    writers.foreach { case (key, (path, writer)) =>
      writer.close()
      offsets.get(key.tp).foreach { offset =>
        stageManager.commit(path, key.tp.withOffset(offset))
      }
    }
  }
}
