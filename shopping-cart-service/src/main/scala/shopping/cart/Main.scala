package shopping.cart

import akka.actor.typed.scaladsl.{ AbstractBehavior, ActorContext, Behaviors }
import akka.actor.typed.{ ActorSystem, Behavior }
import akka.grpc.GrpcClientSettings
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import shopping.order.proto.{ ShoppingOrderService, ShoppingOrderServiceClient }

object Main {

  def main(args: Array[String]): Unit = {
    ActorSystem[Nothing](Main(), "ShoppingCartService")
  }

  def apply(): Behavior[Nothing] = {
    Behaviors.setup[Nothing](context => new Main(context))
  }
}

class Main(context: ActorContext[Nothing])
    extends AbstractBehavior[Nothing](context) {
  val system = context.system

  AkkaManagement(system).start()
  ClusterBootstrap(system).start()

  val session =
    CassandraSessionRegistry(system).sessionFor("akka.persistence.cassandra")
  val itemPopularityKeyspace =
    system.settings.config
      .getString("akka.projection.cassandra.offset-store.keyspace")
  val itemPopularityRepository =
    new ItemPopularityRepositoryImpl(session, itemPopularityKeyspace)(
      system.executionContext)

  ItemPopularityProjection.init(system, itemPopularityRepository)
  PublishEventsProjection.init(system)

  val grpcInterface =
    system.settings.config.getString("shopping-cart-service.grpc.interface")
  val grpcPort =
    system.settings.config.getInt("shopping-cart-service.grpc.port")
  val grpcService =
    new ShoppingCartServiceImpl(system, itemPopularityRepository)

  ShoppingCart.init(system)
  ShoppingCartServer.start(grpcInterface, grpcPort, system, grpcService)

  val orderService = orderServiceClient(system)
  SendOrderProjection.init(system, orderService)

  protected def orderServiceClient(
      system: ActorSystem[_]): ShoppingOrderService = {
    val orderServiceClientSettings = GrpcClientSettings
      .connectToServiceAt(
        system.settings.config.getString("shopping-order-service.host"),
        system.settings.config.getInt("shopping-order-service.port"))(system)
      .withTls(false)
    val orderServiceClient =
      ShoppingOrderServiceClient(orderServiceClientSettings)(system)
    orderServiceClient
  }

  override def onMessage(msg: Nothing): Behavior[Nothing] =
    this
}
