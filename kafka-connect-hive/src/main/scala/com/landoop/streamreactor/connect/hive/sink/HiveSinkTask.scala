package com.landoop.streamreactor.connect.hive.sink

import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.sink.config.{HiveSinkConfig, HiveSinkConfigConstants}
import com.landoop.streamreactor.connect.hive.sink.staging.OffsetSeeker
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class HiveSinkTask extends SinkTask with StrictLogging {

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  // this map contains all the open sinks for this task
  // They are created when the topic/partition assignment happens (the 'open' method)
  // and they are removed when unassignment happens (the 'close' method)
  // 保存topic分区和hive表以及写hive表的对象之间的关系
  private val sinks = scala.collection.mutable.Map.empty[TopicPartition, HiveSink]

  private var client: HiveMetaStoreClient = _
  private var fs: FileSystem = _
  private var config: HiveSinkConfig = _

  def this(fs: FileSystem, client: HiveMetaStoreClient) {
    this()
    this.client = client
    this.fs = fs
  }

  override def version(): String = manifest.version()

  /**
    * 只执行一次，用于启动任务和解析配置
    *
    * @param props
    */
  override def start(props: util.Map[String, String]): Unit = {

    val configs = if (context.configs().isEmpty) props else context.configs()

    // 初始化HiveMetaStoreClient和FileSystem
    if (client == null) {
      val hiveConf = new HiveConf()
      hiveConf.set("hive.metastore", configs.get(HiveSinkConfigConstants.MetastoreTypeKey))
      hiveConf.set("hive.metastore.uris", configs.get(HiveSinkConfigConstants.MetastoreUrisKey))
      client = new HiveMetaStoreClient(hiveConf)
    }

    if (fs == null) {
      val conf: Configuration = new Configuration()
      conf.addResource("src/main/resources/core-site.xml")
      conf.addResource("src/main/resources/hdfs-site.xml")
      //conf.set("fs.defaultFS", configs.get(HiveSinkConfigConstants.FsDefaultKey))
      fs = FileSystem.get(conf)
    }

    config = HiveSinkConfig.fromProps(configs.asScala.toMap)
  }

  /**
    * 发送记录到sink中，通常它是异步地发送记录并且立即返回。如果操作失败，它也许会抛出org.apache.kafka.connect.errors.RetriableException异常，然后重试一次。其他异常会导致task立即停止，
    * {@link SinkTaskContext#timeout(long)} 可以用来设置进行重试之前的最大时间
    *
    * @param records 保存记录的集合
    */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    records.asScala.foreach { record =>
      val struct = ValueConverter(record)
      val tpo = TopicPartitionOffset(
        Topic(record.topic),
        record.kafkaPartition.intValue,
        Offset(record.kafkaOffset)
      )
      val tp = tpo.toTopicPartition
      logger.info("接收到的值是----" + record.value().toString)
      // 根据TopicPartitionOffset获取HiveSink操作对象
      sinks.getOrElse(tp, sys.error(s"Could not find $tp in sinks $sinks")).write(struct, tpo)
    }
  }

  /**
    * 负责给task指定分区，直到调用SinkTask的close方法，这些分区会一直被对应的task持有
    * 在对分区重新reblance完成之后，消费者开始消费数据之前调用。该方法抛出任何异常都会使task停止
    *
    * @param partitions 已指定给task的分区列表
    */
  override def open(partitions: util.Collection[KafkaTopicPartition]): Unit = try {
    // 获取topic->partition集合
    val topicPartitions = partitions.asScala.map(tp => TopicPartition(Topic(tp.topic), tp.partition)).toSet
    open(topicPartitions)
  } catch {
    case NonFatal(e) =>
      logger.error("Error opening hive sink writer", e)
      throw e
  }

  private def open(partitions: Set[TopicPartition]): Unit = {
    val seeker = new OffsetSeeker(config.filenamePolicy)
    // we group by table name, so we only need to seek once per table
    partitions.groupBy(tp => table(tp.topic)).foreach { case (tableName, tps) =>
      // 查找表保存在hdfs中的offset信息
      val offsets = seeker.seek(config.dbName, tableName)(fs, client)
      // can be multiple topic/partitions writing to the same table
      tps.foreach { tp =>
        offsets.find(_.toTopicPartition == tp).foreach { case TopicPartitionOffset(topic, partition, offset) =>
          logger.info(s"Seeking to ${topic.value}:$partition:${offset.value}")

          /**
            * 重置指定topic分区的offset信息，在使用sink data store而不是Kafka consumer offsets管理offset的情况下SinkTasks会使用。比如，HDFS connector会保存offset到HDFS中
            * 来实现至少消费一次的功能，当topic分区对应的任务恢复时将从HDFS中重新reload offsets，从而重置消费者所属的offset。SinkTasks不自己管理offsets时，不需要这个方法
            */
          context.offset(new KafkaTopicPartition(topic.value, partition), offset.value)
        }
        // 输出hive表对应的topic以及topic分区信息
        logger.info(s"Opening sink for ${config.dbName.value}.${tableName.value} for ${tp.topic.value}:${tp.partition}")

        // 保存topic、partition与操作其的HiveSink对象的信息
        val sink = new HiveSink(tableName, config)(client, fs)
        sinks.put(tp, sink)
      }
    }
  }

  /**
    * Whenever close is called, the topics and partitions assigned to this task
    * may be changing, eg, in a rebalance. Therefore, we must commit our open files
    * for those (topic,partitions) to ensure no records are lost.
    */
  override def close(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    val topicPartitions = partitions.asScala.map(tp => TopicPartition(Topic(tp.topic), tp.partition)).toSet
    close(topicPartitions)
  }

  // closes the sinks for the given topic/partition tuples
  def close(partitions: Set[TopicPartition]): Unit = {
    partitions.foreach { tp =>
      sinks.remove(tp).foreach(_.close)
    }
  }

  /**
    * 任务停止时进行清除操作
    */
  override def stop(): Unit = {
    sinks.values.foreach(_.close)
    sinks.clear()
  }

  // returns the KCQL table name for the given topic
  private def table(topic: Topic): TableName = config.tableOptions.find(_.topic == topic) match {
    case Some(options) => options.tableName
    case _ => sys.error(s"Cannot find KCQL for topic $topic")
  }
}
