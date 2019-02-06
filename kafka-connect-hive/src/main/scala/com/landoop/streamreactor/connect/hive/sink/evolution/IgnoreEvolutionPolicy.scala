package com.landoop.streamreactor.connect.hive.sink.evolution

import com.landoop.streamreactor.connect.hive.{DatabaseName, HiveSchemas, TableName}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.kafka.connect.data.Schema

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * An implementation of [[EvolutionPolicy]] that requires the
  * input schema be equal or a superset of the metastore schema.
  *
  * This means that every field in the metastore schema must be
  * present in the incoming records, but the records may have
  * additional fields. These additional fields will be dropped
  * before the data is written out.
  *
  * 要求input schema与metastore schema相等或者是metastore schema的超集。也就是说在metastore schema中的每个字段必须在输入记录中存在，在写入数据之前，输入记录中多余的字段会被drop掉
  */
object IgnoreEvolutionPolicy extends EvolutionPolicy with StrictLogging {

  override def evolve(dbName: DatabaseName,
                      tableName: TableName,
                      metastoreSchema: Schema,
                      inputSchema: Schema)
                     (implicit client: IMetaStoreClient): Try[Schema] = Try {
    HiveSchemas.toKafka(client.getTable(dbName.value, tableName.value))
  }.map { schema =>
    val compatible = schema.fields().asScala.forall { field =>
      inputSchema.field(field.name) != null ||
        field.schema().isOptional ||
        field.schema().defaultValue() != null
    }
    if (compatible) schema else sys.error("Input Schema is not compatible with the metastore")
  }
}
