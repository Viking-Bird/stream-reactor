package com.datamountaineeer.streamreactor.connect.blockchain.source

import java.util

import com.datamountaineeer.streamreactor.connect.blockchain.config.{BlockchainConfig, BlockchainSettings}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.common.config.ConfigException
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class BlockchainSourceTask extends SourceTask with StrictLogging {

  private var taskConfig: Option[AbstractConfig] = None
  private var blockchainManager: Option[BlockchainManager] = None

  /**
    * Starts the Blockchain source, parsing the options and setting up the reader.
    *
    * @param props A map of supplied properties.
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/ascii.txt")).mkString)
    logger.info(
      s"""
         |Configuration for task
         |${props.asScala}
      """.stripMargin)
    //get configuration for this task
    taskConfig = Try(new AbstractConfig(BlockchainConfig.config, props)) match {
      case Failure(f) => throw new ConfigException("Couldn't start BlockchainSource due to configuration error.", f)
      case Success(s) => Some(s)
    }

    val settings = BlockchainSettings(taskConfig.get)

    blockchainManager = Some(new BlockchainManager(settings))
    blockchainManager.foreach(_.start())
    logger.info("Data manager started")
  }

  /**
    * Called by the Framework
    *
    * Checks the Blockchain manager for records of SourceRecords.
    *
    * @return A util.List of SourceRecords.
    **/
  override def poll(): util.List[SourceRecord] = {
    logger.info("Polling for Blockchain records...")
    val records = blockchainManager.map(_.get()).getOrElse(new util.ArrayList[SourceRecord]())
    logger.info(s"Returning ${records.size()} record(-s) from Blockchain source")
    records
  }

  /**
    * Stop the task and close readers.
    *
    **/
  override def stop(): Unit = {
    logger.info("Stopping Blockchain source...")
    blockchainManager.foreach(_.close())
    logger.info("Blockchain data retriever stopped.")
  }

  /**
    * Gets the version of this sink.
    *
    * @return
    */
  override def version(): String = getClass.getPackage.getImplementationVersion

}