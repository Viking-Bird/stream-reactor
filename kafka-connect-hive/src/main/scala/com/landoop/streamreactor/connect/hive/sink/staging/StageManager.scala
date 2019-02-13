package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.{TopicPartition, TopicPartitionOffset}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import com.landoop.streamreactor.connect.hive.formats.HiveWriter

/**
  * The [[StageManager]] handles creation of new files (staging) and
  * publishing of said files (committing).
  *
  * When a [[HiveWriter]] needs to write to a new file, it will invoke
  * the stage method with the partition or table location. A new file
  * will be generated using the [[TopicPartitionOffset]] information.
  *
  * After a file has been closed, commit will be called with the path
  * and the stage manager will make the file visible.
  *
  * StageManager 负责新文件的创建(staging)和发布(committing)。当HiveWriter需要向新文件写入时，它将调用携带有partition和table location的stage方法，新文件将使用TopicPartitionOffset信息生成。
  * 文件被关闭之后，携带有path的commit方法将被调用，stage manager将会使文件可见
  */
class StageManager(filenamePolicy: FilenamePolicy) extends StrictLogging {

  /**
    * 生成临时文件的名称
    *
    * @param tp
    * @return
    */
  private def stageFilename(tp: TopicPartition) =
    s".${filenamePolicy.prefix}_${tp.topic.value}_${tp.partition}"

  /**
    * 生成最终提交的文件的名称
    *
    * @param tpo
    * @return
    */
  private def finalFilename(tpo: TopicPartitionOffset) =
    s"${filenamePolicy.prefix}_${tpo.topic.value}_${tpo.partition}_${tpo.offset.value}"

  /**
    * 获取临时文件名称
    *
    * @param dir
    * @param tp
    * @param fs
    * @return
    */
  def stage(dir: Path, tp: TopicPartition)(implicit fs: FileSystem): Path = {
    val filename = stageFilename(tp) // 获取临时文件的名称
    val stagePath = new Path(dir, filename)
    fs.delete(stagePath, false)
    stagePath
  }

  /**
    * 重命名临时文件
    *
    * @param stagePath
    * @param tpo
    * @param fs
    * @return
    */
  def commit(stagePath: Path, tpo: TopicPartitionOffset)(implicit fs: FileSystem): Path = {
    val finalPath = new Path(stagePath.getParent, finalFilename(tpo))
    logger.info(s"Commiting file $stagePath=>$finalPath")
    fs.rename(stagePath, finalPath) //将临时文件夹重命名
    finalPath
  }
}
