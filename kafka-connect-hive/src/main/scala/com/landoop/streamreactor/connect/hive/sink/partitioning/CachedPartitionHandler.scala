package com.landoop.streamreactor.connect.hive.sink.partitioning

import com.landoop.streamreactor.connect.hive.{DatabaseName, Partition, TableName}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.util.{Success, Try}

/**
  * A [[PartitionHandler]] that delegates to an underlying policy
  * and caches the results for the lifetime of this object.
  *
  * 缓存存活对象的分区信息和在HDFS中的位置信息，底层依赖于PartitionHandler
  */
class CachedPartitionHandler(partitioner: PartitionHandler) extends PartitionHandler {

  val cache = scala.collection.mutable.Map.empty[Partition, Path]

  override def path(partition: Partition,
                    db: DatabaseName,
                    tableName: TableName)
                   (client: IMetaStoreClient, fs: FileSystem): Try[Path] = {
    // 在分区cache中查找分区，如果有则返回。如果没有，则创建
    cache.get(partition) match {
      case Some(path) => Success(path)
      case _ =>
        val created = partitioner.path(partition, db, tableName)(client, fs)
        created.foreach(cache.put(partition, _))
        created
    }
  }
}
