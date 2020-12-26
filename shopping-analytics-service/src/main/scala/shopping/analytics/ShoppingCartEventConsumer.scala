package shopping.analytics

import akka.Done
import akka.actor.typed.ActorSystem
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.RestartSettings
import akka.stream.scaladsl.RestartSource
import com.google.protobuf.{Any => ScalaPBAny}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.slf4j.LoggerFactory
import shopping.cart.proto

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object ShoppingCartEventConsumer {
  private val log = LoggerFactory.getLogger(getClass)

  def init(system: ActorSystem[_]): Unit = {
    log.info("ShoppingCartEventConsumer.init()")
    implicit val sys: ActorSystem[_] = system
    implicit val ec: ExecutionContext = sys.executionContext

    val topic = system.settings.config.getString("shopping-analytics-service.shopping-cart-kafka-topic")
    val consumerSettings = ConsumerSettings(
      system,
      new StringDeserializer,
      new ByteArrayDeserializer).withGroupId("shopping-cart-analytics")
    val committerSettings = CommitterSettings(system)

    RestartSource
      .onFailuresWithBackoff(
        RestartSettings(
          minBackoff = 1.second,
          maxBackoff = 30.seconds,
          randomFactor = 0.1)) { () =>
        log.info("sourceFactory is called!")
        Consumer
          .committableSource(
            consumerSettings,
            Subscriptions.topics(topic)
          ).mapAsync(1) { msg =>
          handleRecord(msg.record).map(_ => msg.committableOffset)
        }.via(Committer.flow(committerSettings))
      }.run()
  }

  private def handleRecord(record: ConsumerRecord[String, Array[Byte]]): Future[Done] = {
    val bytes = record.value()
    val x = ScalaPBAny.parseFrom(bytes)
    val typeUrl = x.getTypeUrl
    log.info("typeUrl={}", typeUrl)
    try {
      val inputBytes = x.getValue.newCodedInput()
      val event =
        typeUrl match {
          case "shopping-cart-service/shoppingcart.ItemAdded" =>
            proto.ItemAdded.parseFrom(inputBytes)
          case "shopping-cart-service/shoppingcart.CheckedOut" =>
            proto.CheckedOut.parseFrom(inputBytes)
          case _ =>
            throw new IllegalArgumentException(s"unknown record type [$typeUrl]")
        }
      event match {
        case proto.ItemAdded(cartId, itemId, quantity, _) =>
          log.info("ItemAdded: {} {} to cart {}", quantity, itemId, cartId)
        case proto.CheckedOut(cartId, _) =>
          log.info("CheckedOut: cart {} checked out", cartId)
      }

      Future.successful(Done)
    } catch {
      case NonFatal(e) =>
        log.error("Could not process event of type [{}]", typeUrl, e)
        Future.successful(Done)
    }
  }
}
