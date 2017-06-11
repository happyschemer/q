package q

import scala.collection.mutable

// todo - doc
object Q {
  type Priority = Int
  type BurstRate = PartialFunction[Priority, Int]

  val UnlimitedCapacity = Int.MaxValue
  val DefaultBurstRate: BurstRate = { case _ => 2 }
}

case class Item[T](priority: Q.Priority, value: T)

object Item {
  def apply[T](value: T): Item[T] = Item(priority = 3, value = value)
}

case class Q[T](capacity: Int = Q.UnlimitedCapacity, burstRate: Q.BurstRate = Q.DefaultBurstRate) {
  // zero capacity would cause deadlock
  assert(capacity > 0)

  // Priority -> Queue of items of that Priority
  private val queues: mutable.Map[Q.Priority, mutable.Queue[T]] = mutable.Map()
  // Priority -> number of items, of that Priority, that has been dequeued (bucket resets after burst)
  private val burstMonitor: mutable.Map[Q.Priority, Int] = mutable.Map()

  /**
    * Is the queue empty? If it is, calling dequeue would return None
    *
    * @return true if empty, false otherwise
    */
  def empty: Boolean = queues.isEmpty

  /**
    * Is the queue full, i.e. having reached its capacity? If it is, calling enqueue would throw IllegalStateException
    *
    * @return true if full, false otherwise
    */
  def full: Boolean = capacity <= queues.values.map(_.length).fold(0)(_ + _)

  /**
    * Enqueue the item if the queue is not full
    *
    * @param item to be enqueued
    */
  def enqueue(item: Item[T]): Unit = {
    if (full) throw new IllegalStateException

    queues.get(item.priority) match {
      case Some(q)  => q.enqueue(item.value)
      case _        => queues(item.priority) = mutable.Queue(item.value)
    }
  }

  /**
    * Dequeue an item if the queue is not empty
    * @return the dequeued item if queue is not empty, None otherwise
    */
  def dequeue: Option[Item[T]] =
    if (empty) None
    else {
      val priorityDeque = burstMonitor.collectFirst {
        // find the priority of which there has been a burst monitored
        case (priority, count) if burstRate(priority) == count => priority
      } match {
        // yes, there was a burst
        case Some(priorityBurst) =>
          // reset the monitored count
          burstMonitor -= priorityBurst
          // find lower priorities (P+1, P+2, ...) of which there are items available
          queues.keys.filter(_ > priorityBurst) match {
            // if found any, pick one most close to P
            case ps if ps.nonEmpty => ps.min
            // otherwise, follow Constraint A
            case _ => queues.keys.min
          }
        // no burst, follow Constraint A
        case _  => queues.keys.min
      }

      Some(deq(priorityDeque))
    }

  // dequeue one item with the specific priority
  private def deq(priority: Q.Priority): Item[T] = {
    val q = queues(priority)
    val value = q.dequeue()

    // discard if empty
    if (q.isEmpty) {
      queues -= priority
    }

    // if burst rate is defined
    if (burstRate.isDefinedAt(priority)) {
      // increment the burst minor
      burstMonitor(priority) = burstMonitor.getOrElse(priority, 0) + 1
    }

    Item(priority, value)
  }
}
