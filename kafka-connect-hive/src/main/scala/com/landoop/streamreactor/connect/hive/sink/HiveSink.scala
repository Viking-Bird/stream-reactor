package com.landoop.streamreactor.connect.hive.sink

import com.landoop.streamreactor.connect.hive
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.sink.config.HiveSinkConfig
import com.landoop.streamreactor.connect.hive.formats.HiveWriter
import com.landoop.streamreactor.connect.hive.sink.mapper.{DropPartitionValuesMapper, MetastoreSchemaAlignMapper, ProjectionMapper}
import com.landoop.streamreactor.connect.hive.sink.partitioning.CachedPartitionHandler
import com.landoop.streamreactor.connect.hive.sink.staging.{CommitPolicy, StageManager}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.util.{Failure, Success}

/**
  * A [[HiveSink]] handles writing records to a single hive table.
  *
  * It will manage a set of [[HiveWriter]] instances, each of which will handle records in a partition.
  * is not partitioned, then this will be a single file.
  *
  * The hive sink uses a [[CommitPolicy]] to determine when to close a file and start
  * a new file. When the file is due to be commited, the [[StageManager]]
  * will be invoked to commit the file.
  *
  * The lifecycle of a sink is managed independently from other sinks.
  *
  * 用于处理单表写入操作，一个HiveSink会持有多个HiveWriter实例，每个HiveWriter实例处理一个分区的记录，没有分区的情况下，HiveWriter负责处理单个文件。
  * hive sink使用CommitPolicy来决定何时关闭一个文件以及新建一个文件。使用StageManager提交文件。sink的生命周期独立于其他sink
  *
  * @param tableName the name of the table to write out to 保存记录的表名
  */
class HiveSink(tableName: TableName,
               config: HiveSinkConfig)
              (implicit client: IMetaStoreClient, fs: FileSystem) extends StrictLogging {

  // 保存表配置信息
  private val tableConfig = config.tableOptions.find(_.tableName == tableName)
    .getOrElse(sys.error(s"No table config for ${tableName.value}"))
  private val writerManager = new HiveWriterManager(tableConfig.format, config.stageManager)
  // 缓存存活对象的分区信息和在HDFS中的位置信息
  private val partitioner = new CachedPartitionHandler(tableConfig.partitioner)

  // 缓存topic、partition和offset
  private val offsets = scala.collection.mutable.Map.empty[TopicPartition, Offset]

  private var table: Table = _
  private var tableLocation: Path = _
  private var plan: Option[PartitionPlan] = _
  private var metastoreSchema: Schema = _
  private var mapper: Struct => Struct = _
  private var lastSchema: Schema = _

  private def init(schema: Schema): Unit = {
    logger.info(s"Init sink for schema $schema")

    def getOrCreateTable(): Table = {

      /**
        * 创建表
        *
        * @return
        */
      def create = {
        val partstring = if (tableConfig.partitions.isEmpty) "<no-partitions>" else tableConfig.partitions.mkString(",")
        logger.info(s"Creating table in hive [${config.dbName.value}.${tableName.value}, partitions=$partstring]")
        hive.createTable(config.dbName, tableName, schema, tableConfig.partitions, tableConfig.location, tableConfig.format)
      }

      logger.debug(s"Fetching or creating table ${config.dbName.value}.${tableName.value}")

      // 表存在的话，根据配置来决定表的创建策略
      client.tableExists(config.dbName.value, tableName.value) match {
        case true if tableConfig.overwriteTable =>
          hive.dropTable(config.dbName, tableName, true)
          create
        case true => client.getTable(config.dbName.value, tableName.value)
        case false if tableConfig.createTable => create
        case false => throw new RuntimeException(s"Table ${config.dbName.value}.${tableName.value} does not exist")
      }
    }

    table = getOrCreateTable() // 获得或创建表
    tableLocation = new Path(table.getSd.getLocation) // 获得表HDFS中的存储位置
    plan = hive.partitionPlan(table) // 获取表的分区key信息
    metastoreSchema = tableConfig.evolutionPolicy.evolve(config.dbName, tableName, HiveSchemas.toKafka(table), schema)
      .getOrElse(sys.error(s"Unable to retrieve or evolve schema for $schema")) // 根据表的schema更新策略获取schema

    val mapperFns: Seq[Struct => Struct] = Seq(
      tableConfig.projection.map(new ProjectionMapper(_)),
      Some(new MetastoreSchemaAlignMapper(metastoreSchema)),
      plan.map(new DropPartitionValuesMapper(_))
    ).flatten.map(mapper => mapper.map _)

    mapper = Function.chain(mapperFns)
  }

  /**
    * Returns the appropriate output directory for the given struct.
    *
    * If the table is not partitioned, then the directory returned
    * will be the location of the table itself, otherwise it will delegate to
    * the partitioning policy.
    *
    * 使用给定的struct返回相应的输出目录。如果表未分区，只返回表自身的位置，否则根据分区策略返回相应的表输出目录
    */
  private def outputDir(struct: Struct, plan: Option[PartitionPlan], table: Table): Path = {
    plan.fold(tableLocation) { plan =>
      val part = hive.partition(struct, plan)
      partitioner.path(part, config.dbName, tableName)(client, fs) match {
        case Failure(t) => throw t
        case Success(path) => path
      }
    }
  }

  /**
    * 保存record到hdfs中
    *
    * @param struct 包含schema、field->value的Struct对象
    * @param tpo    包含topic、partition、offset的TopicPartitionOffset对象
    */
  def write(struct: Struct, tpo: TopicPartitionOffset): Unit = {

    // if the schema has changed, or if we haven't yet had a struct, we need
    // to make sure the table exists, and that our table metadata is up to date
    // given that the table may have evolved.
    // 如果当前的schema不是最新的，更新schema到最新
    if (struct.schema != lastSchema) {
      init(struct.schema)
      lastSchema = struct.schema
    }

    // 获取hive->hdfs目录
    val dir = outputDir(struct, plan, table)
    val mapped = mapper(struct)
    // 写入记录到hdfs中，返回写入记录数
    val (path, writer) = writerManager.writer(dir, tpo.toTopicPartition, mapped.schema)
    val count = writer.write(mapped)

    // 如果文件应该被提交，则提交文件
    //    if (fs.exists(path) && tableConfig.commitPolicy.shouldFlush(struct, tpo, path, count)) {
    //      logger.info(s"Flushing offsets for $dir")
    //      writerManager.flush(tpo, dir)
    //      config.stageManager.commit(path, tpo)
    //    }

    if (fs.exists(path)) {
      logger.info(s"Flushing offsets for $dir")
      writerManager.flush(tpo, dir)
      config.stageManager.commit(path, tpo)
    }

    // 记录topic->partition->offset信息
    offsets.put(tpo.toTopicPartition, tpo.offset)

    // 每次写完记录后，获取最新的schema
    lastSchema = struct.schema()
  }

  def close(): Unit = {
    writerManager.flush(offsets.toMap)
    offsets.clear()
  }
}
