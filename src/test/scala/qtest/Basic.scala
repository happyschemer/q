package qtest

import q._
import utest._

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object Basic extends TestSuite {
  val tests = this {
    val q = Q[Int]()
    val output = ArrayBuffer[Int]()

    "Example 1" - {
      Seq(4, 1, 3, 2, 1, 2) foreach { p => q.enqueue(Item(p, p)) }
      q.dequeue.get.value ==> 1
      q.dequeue.get.value ==> 1

      q.enqueue(Item(1, 1))

      q.dequeue.get.value ==> 2
      q.dequeue.get.value ==> 1
      q.dequeue.get.value ==> 2
      q.dequeue.get.value ==> 3
    }

    "Example 2" - {
      Seq(4, 1, 3, 2, 2) foreach { p => q.enqueue(Item(p, p)) }
      q.dequeue.get.value ==> 1
      q.dequeue.get.value ==> 2

      q.enqueue(Item(1, 1))

      q.dequeue.get.value ==> 1
      q.dequeue.get.value ==> 2
      q.dequeue.get.value ==> 3
      q.dequeue.get.value ==> 4
    }

    "Example 3" - {
      val input = "41321423241335213612424132152112311".toCharArray.map(_.asDigit)
      input foreach { p => q.enqueue(Item(p, p)) }
      while (!q.empty) output += q.dequeue.get.value
      output.mkString("") ==> "11211231121123411212322345233434564"
    }

    "Constraint A" - {
      var cnt = 0
      Seq(1, 1, 1, 2, 1) foreach { p => cnt += 1; q.enqueue(Item(p, cnt))}
      while (!q.empty) {
        q.dequeue.get match {
          case Item(1, v) => output += v
          case _ =>
        }
      }
      output.sorted ==> output
    }

    "Constraint B" - {
      "Problem A1" - {
        Seq(1, 1, 3) foreach { p => q.enqueue(Item(p, p)) }
        q.dequeue.get.value ==> 1
        q.dequeue.get.value ==> 1
        q.dequeue.get.value ==> 3
      }
    }

    'Capacity {
      val q = Q[Int](capacity = 3)

      "Must not be exceeded" - {
        Seq(1, 2, 2) foreach { p => q.enqueue(Item(p, p)) }
        q.full ==> true
        assert(Try(q.enqueue(Item(4, 4))).isFailure)
      }
    }

    "Burst Rates" - {
      "Burst top priority only" - {
        val q = Q[Int](burstRate = {
          case 1 => 2
        })

        Seq(1, 1, 1, 2, 2, 3) foreach { p => q.enqueue(Item(p, p)) }
        while (!q.empty) output += q.dequeue.get.value
        output ==> Seq(1, 1, 2, 1, 2, 3)
      }
      "Fall through" - {
        val q = Q[Int](burstRate = {
          case _ => 1
        })

        Seq(1, 1, 2, 2, 3, 3) foreach { p => q.enqueue(Item(p, p)) }
        while (!q.empty) output += q.dequeue.get.value
        output ==> Seq(1, 2, 3, 1, 2, 3)
      }

      "Default" - {
        val q = Q[Int](burstRate = {
          case _ => 2
        })

        Seq(1, 1, 2, 2, 3, 3) foreach { p => q.enqueue(Item(p, p)) }
        while (!q.empty) output += q.dequeue.get.value
        output ==> Seq(1, 1, 2, 2, 3, 3)
      }

    }
  }
}