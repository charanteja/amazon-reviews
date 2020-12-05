package org.amazon.reviews

import org.scalatest.{FunSpec, Matchers}

/**
  * Test to ensure that SparkSession is created correctly
  * in the test environment
  */

class SparkSessionSpec extends FunSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  describe("Spark Session Spec") {
    it("Spark Session ready") {

      List(1, 2, 3, 4, 5).toDS().map(x => x * x).collect().toList shouldEqual List(1, 4, 9, 16, 25)
    }
  }
}
