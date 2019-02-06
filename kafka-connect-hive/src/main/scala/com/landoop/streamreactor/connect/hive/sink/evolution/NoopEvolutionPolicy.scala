package com.landoop.streamreactor.connect.hive.sink.evolution

import com.landoop.streamreactor.connect.hive.{DatabaseName, TableName}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.kafka.connect.data.Schema

import scala.util.Try

/**
  * An implementation of [[EvolutionPolicy]] that peforms no checks.
  *
  * This means that invalid data may be written and/or exceptions may be thrown.
  *
  * This policy can be useful in tests but should be avoided in production code.
  *
  * 不执行任何schema操作。也就是说垃圾数据会被写入，或者异常在写入过程中会抛出异常。该策略用在测试环境中吗，在生产环境中应该避免使用
  */
object NoopEvolutionPolicy extends EvolutionPolicy with StrictLogging {
  override def evolve(dbName: DatabaseName,
                      tableName: TableName,
                      metastoreSchema: Schema,
                      inputSchema: Schema)
                     (implicit client: IMetaStoreClient): Try[Schema] = Try(metastoreSchema)
}
