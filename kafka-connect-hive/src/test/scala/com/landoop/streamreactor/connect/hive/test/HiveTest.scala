package com.landoop.streamreactor.connect.hive.test

import java.util

import com.landoop.streamreactor.connect.hive.HiveTestConfig
import com.landoop.streamreactor.connect.hive.formats.ParquetHiveFormat
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.{FieldSchema, SerDeInfo, StorageDescriptor, Table}
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Created by wangpeng
  * Date: 2019-02-02
  * Time: 11:28
  */
class HiveTest extends HiveTestConfig {

  /**
    * 建表测试
    */
  @Test
  def testCreateTable: Unit = {
    // 定义表信息
    val table = new Table()
    table.setDbName(dbName)
    table.setTableName("non_partitioned_table")
    table.setOwner("hive")
    table.setTableType(TableType.EXTERNAL_TABLE.name())
    table.setCreateTime((System.currentTimeMillis / 1000).toInt)
    table.setRetention(0)
    table.setParameters(new util.HashMap())

    // 定义列信息
    val cols = Seq(
      new FieldSchema("a", "string", "lovely field"),
      new FieldSchema("b", "int", null),
      new FieldSchema("c", "boolean", null)
    )

    // 定义存储格式信息，使用Parquet格式存储
    val sd = new StorageDescriptor()
    sd.setCompressed(false)
    sd.setLocation(client.getDatabase("default").getLocationUri + "/non_partitioned_table")
    sd.setInputFormat(ParquetHiveFormat.serde.inputFormat)
    sd.setOutputFormat(ParquetHiveFormat.serde.outputFormat)
    sd.setSerdeInfo(new SerDeInfo(null, ParquetHiveFormat.serde.serializationLib, ParquetHiveFormat.serde.params.asJava))
    sd.setCols(cols.asJava)
    table.setSd(sd)

    client.createTable(table)
  }
}
