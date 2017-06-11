package qtest

import q._
import utest._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

object Concurrency extends TestSuite {
  import scala.concurrent.ExecutionContext.Implicits.global

  val tests = this {
    val items = Seq("hello", "world")

    val q = concurrent.Q[String](capacity = 1)
    val output = ArrayBuffer[String]()

    'Blocking {
      val s = q.session()

      "Producer blocking" - {
        val f = Future {
          items.map(Item(_)) foreach {
            s.enqueue(_)
          }
        }

        continually(!f.isCompleted)
      }

      "Consumer blocking" - {
        val f = Future {
          s.dequeue
        }

        continually(!f.isCompleted)
      }
    }

    "One producer, and one consumer" - {
      Future {
        val s = q.session()
        (0 until items.length) map { _ =>
          s.dequeue.value
        }
      } onSuccess {
        case its => output ++= its
      }

      Future {
        val s = q.session()
        items.map(Item(_)) foreach {
          s.enqueue(_)
        }
      }

      eventually(
        output == items
      )
    }

    "No duplicate, no monopoly" - {
      val q = concurrent.Q[Int]()
      val b1 = ArrayBuffer[Int]()
      val b2 = ArrayBuffer[Int]()

      Future {
        val s = q.session()
        while (true) {
          b1 += s.dequeue.value
        }
      }

      Future {
        val s = q.session()
        while (true) {
          b2 += s.dequeue.value
        }
      }

      Future {
        val s = q.session()
        (1 to 10000) foreach { i =>
          s.enqueue(Item(i))
        }
      }

      continually {
        val b11 = ArrayBuffer[Int]()
        val b22 = ArrayBuffer[Int]()
        b11 ++= b1
        b22 ++= b2

        (b11 diff b22) == b11
      }

      eventually(b1.nonEmpty && b2.nonEmpty)

      continually {
        val l1 = b1.length
        val l2 = b2.length

        if (l1 > 0 && l2 > 0) {
          l1 / l2 < 10 && l2 / l1 < 10
        } else true
      }
    }

    "Never exceed capacity" - {
      val capacity = 100
      val producers = 23
      val q = concurrent.Q[Unit](capacity = capacity)
      val counts = Array.fill(producers)(0)

      (1 to producers) foreach { i =>
        Future {
          val s = q.session()
          (1 to 29) foreach { _ =>
            s.enqueue(Item(()))
            counts(i) += 1
          }
        }
      }

      continually {
        counts.sum <= capacity
      }
    }
  }
}
