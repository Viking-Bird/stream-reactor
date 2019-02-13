package com.landoop.streamreactor.connect.hive.sink.config

import java.util.Collections

import cats.data.NonEmptyList
import com.datamountaineer.kcql.{Field, PartitioningStrategy, SchemaEvolution}
import com.landoop.streamreactor.connect.hive.formats.{HiveFormat, ParquetHiveFormat}
import com.landoop.streamreactor.connect.hive.sink.evolution.{AddEvolutionPolicy, EvolutionPolicy, IgnoreEvolutionPolicy, StrictEvolutionPolicy}
import com.landoop.streamreactor.connect.hive.sink.partitioning.{DynamicPartitionHandler, PartitionHandler, StrictPartitionHandler}
import com.landoop.streamreactor.connect.hive.sink.staging._
import com.landoop.streamreactor.connect.hive.{DatabaseName, PartitionField, TableName, Topic}
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.collection.JavaConverters._

case class HiveSinkConfig(dbName: DatabaseName,
                          filenamePolicy: FilenamePolicy = DefaultFilenamePolicy,
                          stageManager: StageManager = new StageManager(DefaultFilenamePolicy),
                          tableOptions: Set[TableOptions] = Set.empty)

case class TableOptions(tableName: TableName,
                        topic: Topic,
                        createTable: Boolean = false,
                        overwriteTable: Boolean = false,
                        partitioner: PartitionHandler = new DynamicPartitionHandler(),
                        evolutionPolicy: EvolutionPolicy = IgnoreEvolutionPolicy,
                        projection: Option[NonEmptyList[Field]] = None,
                        // when creating a new table, the table will be partitioned with the fields set below
                        partitions: Seq[PartitionField] = Nil,
                        // the format used when creating a new table, if the table exists
                        // then the format will be derived from the table parameters
                        format: HiveFormat = ParquetHiveFormat,
                        commitPolicy: CommitPolicy = DefaultCommitPolicy(Some(1000 * 1000 * 128), None, None),
                        location: Option[String] = None)

/**
  * 根据hive sink配置，创建HiveSinkConfig配置对象
  */
object HiveSinkConfig extends StrictLogging {

  def fromProps(props: Map[String, String]): HiveSinkConfig = {

    import scala.concurrent.duration._

    val config = HiveSinkConfigDefBuilder(props.asJava)

    // 根据Kcql实体类中定义的字段顺序来解析kcql，设置表配置信息
    val tables = config.getKCQL.map { kcql =>
      // 要查询的字段
      val fields = Option(kcql.getFields).getOrElse(Collections.emptyList).asScala.toList
      val projection = if (fields.size == 1 && fields.head.getName == "*") None else NonEmptyList.fromList(fields)

      // 提交文件到HDFS中的记录数
      val flushSize = Option(kcql.getWithFlushSize).filter(_ > 0)
      // 提交文件的时间间隔，以毫秒表示
      val flushInterval = Option(kcql.getWithFlushInterval).filter(_ > 0).map(_.seconds)
      // 未提交文件到HDFS中的记录数
      val flushCount = Option(kcql.getWithFlushCount).filter(_ > 0)

      // we must have at least one way of committing files
      val finalFlushSize = if (flushSize.isEmpty && flushInterval.isEmpty && flushCount.isEmpty) Some(1000L * 1000 * 128) else None

      // 根据kcql中的StoredAs参数获取hive表存储格式
      val format: HiveFormat = HiveFormat(Option(kcql.getStoredAs).map(_.toLowerCase).getOrElse("parquet"))

      TableOptions(
        TableName(kcql.getTarget),
        Topic(kcql.getSource),
        kcql.isAutoCreate, // 是否自动创建表
        kcql.getWithOverwrite, // 是否允许覆盖已存在的表
        Option(kcql.getWithPartitioningStrategy).getOrElse(PartitioningStrategy.STRICT) match { // 设置表分区handler
          case PartitioningStrategy.DYNAMIC => new DynamicPartitionHandler()
          case PartitioningStrategy.STRICT => StrictPartitionHandler
        },
        format = format, // 表存储格式
        projection = projection,
        evolutionPolicy = Option(kcql.getWithSchemaEvolution).getOrElse(SchemaEvolution.MATCH) match { // schema 更新策略
          case SchemaEvolution.ADD => AddEvolutionPolicy
          case SchemaEvolution.IGNORE => IgnoreEvolutionPolicy
          case SchemaEvolution.MATCH => StrictEvolutionPolicy
        },
        partitions = Option(kcql.getPartitionBy).map(_.asScala).getOrElse(Nil).map(name => PartitionField(name)).toVector, // 分区字段
        commitPolicy = DefaultCommitPolicy( // 提交策略
          fileSize = finalFlushSize,
          interval = flushInterval,
          fileCount = flushCount
        ),
        location = Option(kcql.getWithTableLocation) // 表在HDFS中的存储位置
      )
    }

    logger.info(s"TableOptions：$tables")

    HiveSinkConfig(
      dbName = DatabaseName(props(HiveSinkConfigConstants.DatabaseNameKey)),
      filenamePolicy = DefaultFilenamePolicy,
      stageManager = new StageManager(DefaultFilenamePolicy),
      tableOptions = tables
    )
  }
}
