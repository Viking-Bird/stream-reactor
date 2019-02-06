package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.TopicPartitionOffset
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.Struct

import scala.concurrent.duration.FiniteDuration

/**
  * The [[CommitPolicy]] is responsible for determining when
  * a file should be flushed (closed on disk, and moved to be visible).
  *
  * Typical implementations will flush based on number of records,
  * file size, or time since the file was opened.
  *
  * 负责决定文件何时被刷新(在磁盘上关闭，以及移动到可见)，一般情况下基于记录数量、文件大小和文件被打开的时间来刷新
  */
trait CommitPolicy {

  /**
    * This method is invoked after a file has been written.
    *
    * If the output file should be committed at this time, then this
    * method should return true, otherwise false.
    *
    * Once a commit has taken place, a new file will be opened
    * for the next record.
    *
    * 该方法在文件被写入之后调用，在这时如果文件应该被提交，该方法返回true，否则返回false。一旦发生了提交，新文件将为下一个记录打开
    *
    * @param tpo   the [[TopicPartitionOffset]] of the last record written 最后一次记录的TopicPartitionOffset
    * @param path  the path of the file that the struct was written to 文件写入的路径
    * @param count the number of records written thus far to the file 到目前为止写入文件的记录数
    *
    */
  def shouldFlush(struct: Struct, tpo: TopicPartitionOffset, path: Path, count: Long)
                 (implicit fs: FileSystem): Boolean
}

/**
  * Default implementation of [[CommitPolicy]] that will flush the
  * output file under the following circumstances:
  * - file size reaches limit
  * - time since file was created
  * - number of files is reached
  *
  * CommitPolicy 的默认实现，将根据以下场景刷新输出文件：
  * 文件大小达到限制
  * 文件创建以来的时间
  * 达到文件数量
  *
  * @param interval in millis 毫秒间隔
  */
case class DefaultCommitPolicy(fileSize: Option[Long],
                               interval: Option[FiniteDuration],
                               fileCount: Option[Long]) extends CommitPolicy {
  require(fileSize.isDefined || interval.isDefined || fileCount.isDefined)
  override def shouldFlush(struct: Struct, tpo: TopicPartitionOffset, path: Path, count: Long)
                          (implicit fs: FileSystem): Boolean = {
    val stat = fs.getFileStatus(path)
    val open_time = System.currentTimeMillis() - stat.getModificationTime // 计算文件打开时间
    fileSize.exists(_ <= stat.getLen) || interval.exists(_.toMillis <= open_time) || fileCount.exists(_ <= count)
  }
}
