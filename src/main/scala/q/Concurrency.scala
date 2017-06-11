package q

import scala.collection.mutable
import scala.concurrent.duration._
import akka.actor.{Actor, ActorRef, ActorSystem, Inbox, Props}
import q.Q._

// todo - doc
class QActor[T](capacity: Int, burstRate: BurstRate) extends Actor {
  private val q = Q[T](capacity, burstRate)
  private val blockingProducers = mutable.Queue[(ActorRef, Item[T])]()
  private val blockingConsumers = mutable.Queue[ActorRef]()

  import QActor._

  override def receive = {
    case Enqueue(item) =>
      if (q.full) {
        blockingProducers.enqueue(sender() -> item.asInstanceOf[Item[T]])
      } else {
        enqueAck(sender(), item.asInstanceOf[Item[T]])

        if (blockingConsumers.nonEmpty) {
          blockingConsumers.dequeue ! DequeueAck(q.dequeue.get)
        }
      }

    case Dequeue =>
      if (q.empty) {
        blockingConsumers.enqueue(sender())
      } else {
        sender() ! DequeueAck(q.dequeue.get)

        if (blockingProducers.nonEmpty) {
          val (producer, item) = blockingProducers.dequeue
          enqueAck(producer, item)
        }
      }
  }

  private def enqueAck(producer: ActorRef, item: Item[T]): Unit = {
    q.enqueue(item)
    producer ! EnqueueAck
  }
}

object QActor {
  def props[T](capacity: Int, burstRate: BurstRate) =
    Props(new QActor[T](capacity, burstRate))

  case class Enqueue[T](item: Item[T])
  case object EnqueueAck

  case object Dequeue
  case class DequeueAck[T](item: Item[T])
}

object concurrent {
  private implicit val system = ActorSystem("Q")

  trait ConcurrentQ[T] { // blocking
    def enqueue(item: Item[T]): Unit
    def dequeue: Item[T]
  }

  def terminate: Unit = {
    system.terminate()
  }

  def Q[T](capacity: Int = UnlimitedCapacity, burstRate: BurstRate = DefaultBurstRate) = new {
    val q = system.actorOf(QActor.props[T](capacity, burstRate))

    def session(timeout: FiniteDuration = 10 seconds) = new ConcurrentQ[T] {
      val inbox = Inbox.create(system)

      import QActor._

      override def enqueue(item: Item[T]): Unit = {
        inbox.send(q, Enqueue(item))
        inbox.receive(timeout)
      }

      override def dequeue: Item[T] = {
        inbox.send(q, Dequeue)
        inbox.receive(timeout) match {
          case DequeueAck(item) => item.asInstanceOf[Item[T]]
          case _ => ??? // should never happen
        }
      }
    }
  }
}
