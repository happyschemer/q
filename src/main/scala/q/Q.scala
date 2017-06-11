package q

import scala.collection.mutable

object Q {
  type Priority = Int

  /**
    * Generalized burst rate, as in Problem B. Observed only for priority which is defined in the partial function;
    * if not defined, Constraint B is not observed for that priority
    */
  type BurstRate = PartialFunction[Priority, Int]

  /** default burst rate of constant 2 for each and every priority (as in Problem A) */
  val DefaultBurstRate: BurstRate = { case _ => 2 }

  val UnlimitedCapacity = Int.MaxValue
}

case class Item[T](priority: Q.Priority, value: T)

object Item {
  /** Convenient if priority is not a concern */
  def apply[T](value: T): Item[T] = Item(priority = 3, value = value)
}

/**
  * A priority queue, which observes Constraint A and Constraint B (including a solution for Problem A1, and Problem B),
  * and has a capacity limitation. It is '''not''' thread safe, but is designed only for single thread use. For thread
  * safety in a concurrent setup, check [[concurrent]]
  *
  * = Implementation Details =
  *
  * One dedicated internal FIFO queue exists for each priority: e.g. Q,,p,, for priority P. An inbound item of
  * priority P, item,,p,,, is always enqueued into its dedicated queue Q,,p,,. To observe Constraint A, next outbound
  * item is always dequeued from the queue for the highest priority.
  *
  * If there is a burst rate defined for a priority (as by [[burstRate]]), the count of dequeued items of that priority
  * is tracked in order to observe Constraint B. If the count reaches the burst rate, the next outbound item is dequeued
  * from the internal queue of the highest priority which is lower than that of the just bursted one; if such item does
  * not exist, it falls back to observe Constraint A.
  *
  * @param capacity Capacity of the queue
  * @param burstRate Burst rate per priority
  * @tparam T Type of item value
  */
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
