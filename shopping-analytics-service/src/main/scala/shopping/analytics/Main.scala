package shopping.analytics

import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement

object Main {

  def main(args: Array[String]): Unit = {
    ActorSystem[Nothing](Main(), "ShoppingAnalyticsService")
  }

  def apply(): Behavior[Nothing] = {
    Behaviors.setup[Nothing](context => new Main(context))
  }
}

class Main(context: ActorContext[Nothing])
  extends AbstractBehavior[Nothing](context) {
  val system: ActorSystem[_] = context.system

  AkkaManagement(system).start()
  ClusterBootstrap(system).start()

  ShoppingCartEventConsumer.init(system)

  override def onMessage(msg: Nothing): Behavior[Nothing] =
    this
}
