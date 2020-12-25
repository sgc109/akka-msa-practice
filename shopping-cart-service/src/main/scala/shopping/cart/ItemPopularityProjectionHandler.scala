package shopping.cart

import akka.Done
import akka.actor.typed.ActorSystem
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

class ItemPopularityProjectionHandler(
    tag: String,
    system: ActorSystem[_],
    repo: ItemPopularityRepository)
    extends Handler[EventEnvelope[ShoppingCart.Event]]() {

  private val log = LoggerFactory.getLogger(getClass)
  private implicit val ec: ExecutionContext = system.executionContext

  override def process(
      envelope: EventEnvelope[ShoppingCart.Event]): Future[Done] = {
    envelope.event match {
      case ShoppingCart.ItemAdded(_, itemId, quantity) =>
        val result = repo.update(itemId, quantity)
        result.foreach(_ => logItemCount(itemId))
        result

      case _: ShoppingCart.CheckedOut =>
        Future.successful(Done)
    }
  }

  private def logItemCount(itemId: String): Unit = {
    repo.getItem(itemId).foreach { optCount =>
      log.info(
        "ItemPopularityProjectionHandler({}) item popularity for '{}': [{}]",
        tag,
        itemId,
        optCount.getOrElse(0))
    }
  }
}
