package q

import scala.collection.mutable
import scala.concurrent.duration._
import akka.actor.{Actor, ActorRef, ActorSystem, Inbox, Props}
import q.Q._

/**
  * Actor which wraps a [[q.Q]] to provide thread safe, concurrent access
  *
  * = Implementation Details =
  *
  * Actor out of the box has already provide thread safety. What is missing is the tracking of blocked callers, which is
  * implemented by maintaining two queues of caller actors, one of blocking producers (Q,,p,,), and one for blocking
  * consumers (Q,,c,,). For Q,,p,, the item to be enqueued is also held along with the producer actor.
  *
  * Every time when the item queue is full and a producer calls enqueue, the producer and the item are enqueued into
  * Q,,p,,, and this actor does not acknowledge immediately. Once any item is dequeued later, this actor acknowledges to
  * the oldest (in terms of FIFO) producer in Q,,p,, after enqueuing the originally held item.
  *
  * Every time when the item queue is empty and a consumer calls dequeue, the consumer is enqueued into Q,,c,,, and this
  * actor does not reply immediately. Once any new item is enqueued later, this actor sends the item to the oldest (in
  * terms of FIFO) consumer in Q,,c,,.
  *
  * @param capacity Capacity of the queue
  * @param burstRate Burst rate per priority
  * @tparam T Type of item value
  */
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

  // messages
  case class Enqueue[T](item: Item[T])
  case object EnqueueAck
  case object Dequeue
  case class DequeueAck[T](item: Item[T])
}

/**
  * API to the concurrent, thread safe priority queue. This complete all required features as in the assessment
  * requirements
  *
  * == Example Usage ==
  *
  * {{{
  *   import q._
  *   import scala.concurrent.ExecutionContext.Implicits.global // only needed if using Future
  *
  *   val q = concurrent.Q[Int]()
  *
  *   // a thread that consumes 100 items sequentially
  *   Future {
  *     val s = q.session()
  *     (1 to 100) map { _ =>
  *       val v = s.dequeue.value
  *       println(v)
  *     }
  *   }
  *
  *   // a thread that produces 100 items sequentially
  *   Future {
  *     val s = q.session()
  *     (1 to 100) foreach { i =>
  *       s.enqueue(Item(i))
  *     }
  *   }
  * }}}
  *
  * = Implementation Details =
  *
  * It wraps the [[QActor]] and provides access for a non-actor caller. Blocking of enqueue and dequeue calls are
  * provided too, with an option of timeout.
  *
  */
object concurrent {
  private implicit val system = ActorSystem("Q")

  /** Concurrent queue API */
  trait ConcurrentQ[T] {
    /** Enqueue the item. Caller is blocked if the queue is full */
    def enqueue(item: Item[T]): Unit

    /** Dequeue and return an item. Caller is blocked if the queue is empty */
    def dequeue: Item[T]
  }

  /** Clean up actor system resources. Need to called at application shutdown */
  def terminate: Unit = {
    system.terminate()
  }

  /**
    * Instantiate a concurrent queue, which is shared across threads, so only needs to be called once per application
    */
  def Q[T](capacity: Int = UnlimitedCapacity, burstRate: BurstRate = DefaultBurstRate) = new {
    val q = system.actorOf(QActor.props[T](capacity, burstRate))

    /**
      * Establish a session with the queue, this needs to called once per thread
      *
      * @param timeout Timeout setting for enqueue and dequeue calls, default to 10 seconds
      * @return API to enqueue and dequeue
      */
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
