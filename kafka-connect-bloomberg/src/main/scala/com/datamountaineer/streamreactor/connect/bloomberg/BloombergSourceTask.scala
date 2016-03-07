package com.datamountaineer.streamreactor.connect.bloomberg

import java.util

import com.bloomberglp.blpapi._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._

/**
  * <h1>BloombergSourceTask</h1>
  *
  * Kafka Connect Cassandra source task. Called by framework to get the records to be sent over kafka to the sink
  **/
class BloombergSourceTask extends SourceTask with StrictLogging {
  var settings: Option[BloombergSettings] = None

  var subscriptions: SubscriptionList = null
  var session: Session = null

  var subscriptionManager: BloombergSubscriptionManager = null

  /**
    * Un-subscribes the tickers and stops the Bloomberg session
    */
  override def stop(): Unit = {
    try {
      session.unsubscribe(subscriptions)
    }
    catch {
      case t: Throwable =>
        logger.error(s"Unexpected exception un-subscribing for correlation=${CorrelationIdsExtractorFn(subscriptions)}")
    }
    try {
      session.stop()
    }
    catch {
      case e: InterruptedException =>
        logger.error(s"There was an error stopping the bloomberg session for correlation=${CorrelationIdsExtractorFn(subscriptions)}")
    }
    session = null
    subscriptions.clear()
    subscriptions = null
    subscriptionManager = null
  }

  /**
    * Creates and starts the Bloomberg session and subscribes for the tickers data update
    *
    * @param map
    */
  override def start(map: util.Map[String, String]): Unit = {
    try {
      settings = Some(BloombergSettings(new ConnectorConfig(map)))
      subscriptions = SubscriptionsBuilderFn(settings.get)

      val correlationToTicketMap = subscriptions.asScala.map { s => s.correlationID().value() -> s.subscriptionString() }.toMap
      subscriptionManager = new BloombergSubscriptionManager(correlationToTicketMap)
      session = BloombergSessionCreateFn(settings.get, subscriptionManager)

      session.subscribe(subscriptions)
    }
    catch {
      case t: Throwable => throw new ConnectException("Could not start the task because of invalid configuration.", t)
    }
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  /**
    * Called by the framework. It returns all the accumulated records since the previous call.
    *
    * @return A list of records as a result of Bloomberg updates since the previous call.
    */
  override def poll(): util.List[SourceRecord] = {
    subscriptionManager.getData.map { case d =>
      val list = new util.ArrayList[SourceRecord](d.size())
      d.asScala.foreach(d => list.add(SourceRecordConverterFn(d, settings.get.kafkaTopic)))
      list
    }.orNull
  }
}

object SubscriptionsBuilderFn extends StrictLogging {
  /**
    * Creates a list of subscriptions to be made to Bloomberg
    *
    * @param settings : The connector settings containing all the subscription information
    * @return The instance of Bloomberg subscriptions
    */
  def apply(settings: BloombergSettings): SubscriptionList = {
    val subscriptions = new SubscriptionList()

    settings.subscriptions.zipWithIndex.foreach { case (config, i) =>
      val unrecognizedFields = config.fields.map(_.toUpperCase).filterNot(BloombergConstants.SubscriptionFields.contains)
      if (unrecognizedFields.nonEmpty) {
        throw new IllegalArgumentException(s"Following fileds are not recognized: ${unrecognizedFields.mkString(",")}")
      }

      val fields = config.fields.map(_.trim.toUpperCase).mkString(",")
      logger.debug(s"Creating a Bloomberg subscription for ${config.ticket} with $fields and correlation:$i")
      val subscription = new Subscription(config.ticket, fields, new CorrelationID(i))
      subscriptions.add(subscription)
    }

    subscriptions
  }
}

object BloombergSessionCreateFn {
  /**
    * Creates and starts a Bloomberg session and connects to the appropriate Bloomberg service (market data, reference data)
    *
    * @param settings : Contains all the connection details for the Bloomberg session
    * @param handler  : Instance of EventHandler providing the callbacks for Bloomberg events
    * @return The Bloomberg session
    */
  def apply(settings: BloombergSettings, handler: EventHandler) = {
    val options = new SessionOptions
    options.setKeepAliveEnabled(true)
    options.setServerHost(settings.serverHost)
    options.setServerPort(settings.serverPort)
    settings.authenticationMode.foreach(options.setAuthenticationOptions)

    val session = new Session(options, handler)

    if (!session.start())
      sys.error(s"Could not start the session for ${settings.serverHost}:${settings.serverPort}")

    if (!session.openService(settings.serviceUri))
      sys.error(s"Could not open service ${settings.serviceUri}")
    session
  }
}