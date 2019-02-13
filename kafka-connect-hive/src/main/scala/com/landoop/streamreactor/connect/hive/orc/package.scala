package com.landoop.streamreactor.connect.hive

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.Schema
import org.apache.orc.OrcFile.EncodingStrategy
import org.apache.orc._

package object orc {

  /**
    * 创建写ORC数据的Writer对象
    * @param path
    * @param schema
    * @param config
    * @param fs
    * @return
    */
  def createOrcWriter(path: Path, schema: TypeDescription, config: OrcSinkConfig)
                     (implicit fs: FileSystem): Writer = {

    // 设置写ORC文件时的配置
    val options = OrcFile.writerOptions(null, fs.getConf).setSchema(schema)

    options.compress(config.compressionKind)
    options.encodingStrategy(config.encodingStrategy)
    options.blockPadding(config.blockPadding)
    options.version(OrcFile.Version.V_0_12)

    config.bloomFilterColumns.map(_.mkString(",")).foreach(options.bloomFilterColumns)
    config.rowIndexStride.foreach(options.rowIndexStride)
    config.blockSize.foreach(options.blockSize)
    config.stripeSize.foreach(options.stripeSize)

    // 如果指定的path已存在于HDFS中，且overwrite为true，则删除已存在的path
    if (config.overwrite && fs.exists(path))
      fs.delete(path, false)

    // 创建Writer对象
    OrcFile.createWriter(path, options)
  }

  def source(path: Path, config: OrcSourceConfig)
            (implicit fs: FileSystem) = new OrcSource(path, config)

  def sink(path: Path, schema: Schema, config: OrcSinkConfig)
          (implicit fs: FileSystem) = new OrcSink(path, schema, config)
}

case class OrcSourceConfig()

case class OrcSinkConfig(overwrite: Boolean = false,
                         batchSize: Int = 5, // orc default is 1024
                         encodingStrategy: EncodingStrategy = EncodingStrategy.COMPRESSION,
                         compressionKind: CompressionKind = CompressionKind.SNAPPY,
                         blockPadding: Boolean = true,
                         blockSize: Option[Long] = None,
                         stripeSize: Option[Long] = None,
                         bloomFilterColumns: Seq[String] = Nil,
                         rowIndexStride: Option[Int] = None)

