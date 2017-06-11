package qtest

class Framework extends utest.runner.Framework {
  override def teardown() = {
    q.concurrent.terminate
  }
}

