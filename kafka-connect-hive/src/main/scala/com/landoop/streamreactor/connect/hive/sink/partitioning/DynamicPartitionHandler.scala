package com.landoop.streamreactor.connect.hive.sink.partitioning

import com.landoop.streamreactor.connect.hive.{DatabaseName, Partition, TableName}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.{StorageDescriptor, Table}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * A [[PartitionHandler]] that creates partitions
  * on the fly as required.
  *
  * The path of the partition is determined by the given
  * [[PartitionPathPolicy]] parameter. By default this will
  * be an implementation that uses the standard hive
  * paths of key1=value1/key2=value2.
  */
class DynamicPartitionHandler(pathPolicy: PartitionPathPolicy = DefaultMetastorePartitionPathPolicy)
  extends PartitionHandler with StrictLogging {

  override def path(partition: Partition,
                    db: DatabaseName,
                    tableName: TableName)
                   (client: IMetaStoreClient,
                    fs: FileSystem): Try[Path] = {

    def table: Table = client.getTable(db.value, tableName.value)

    def create(path: Path, table: Table): Unit = {
      logger.debug(s"New partition will be created at $path")

      // 设置的表的存储位置信息
      val sd = new StorageDescriptor(table.getSd)
      sd.setLocation(path.toString)

      val params = new java.util.HashMap[String, String]
      // 获取分区key的值、分区创建时间
      val values = partition.entries.map(_._2).toList.asJava
      val ts = (System.currentTimeMillis / 1000).toInt

      // 给表设置并创建新分区
      val p = new org.apache.hadoop.hive.metastore.api.Partition(values, db.value, tableName.value, ts, 0, sd, params)
      logger.debug(s"Updating hive metastore with partition $p")
      client.add_partition(p)

      logger.info(s"Partition has been created in metastore [$partition]")
    }

    // 获取分区信息
    Try(client.getPartition(db.value, tableName.value, partition.entries.toList.map(_._2).asJava)) match {
      case Success(p) => Try { // 成功则返回
        new Path(p.getSd.getLocation)
      }
      case Failure(_) => Try { // 失败则根据分区路径创建策略生成分区路径并返回
        val t = table
        val tableLocation = new Path(t.getSd.getLocation)
        val path = pathPolicy.path(tableLocation, partition)
        create(path, t)
        path
      }
    }
  }
}
