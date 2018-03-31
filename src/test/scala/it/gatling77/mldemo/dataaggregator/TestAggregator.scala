package it.gatling77.mldemo.dataaggregator

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by gatling77 on 3/20/18.
  */

@RunWith(classOf[JUnitRunner])
class TestAggregator extends FunSuite with BeforeAndAfterAll {
  //reading a sample file with 30 rows, 3 users with 10 transactions each
  lazy val sut = new Aggregator("/home/gatling77/dev/mldemo/dataggregator/src/test/resources/small_dataset.csv",Aggregator.sc)

  test("SUT can be instantiated"){
    val instantiated = try {
      sut
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiated, "Can't instantiate a StackOverflow object")
  }

  test("read raw data reads the expected number of lines"){
    val lines = sut.readRawData()
    assert(lines.count()==30)
  }

  test("transform raw data to transactions"){
    val lines = sut.readRawData()
    val transactions = sut.toTransactions(lines)
    assert(transactions.count()==30)
    //checking first transaction object
    val firstTransaction = transactions.first()
    assert(firstTransaction.userid==1)
    assert(firstTransaction.timestamp==1482619020)
    assert(firstTransaction.amount==245)
    assert(firstTransaction.lat==46.02591193654497)
    assert(firstTransaction.lon==8.885342194255443)
    assert(firstTransaction.merchant=="Turnberry Larraq store")
    assert(firstTransaction.presentationMode==5)
    assert(!firstTransaction.isFraud)
  }

  test("filter fraud transactions"){
    val lines = sut.readRawData()
    val transactions = sut.toTransactions(lines)
    val validTransactions = sut.excludeFraud(transactions)

    assert(validTransactions.count()==29)
  }


  test("calculate user stats"){
    val lines = sut.readRawData()
    val transactions = sut.toTransactions(lines)
    val validTransactions = sut.excludeFraud(transactions)
    val userStats = sut.userStats(validTransactions)

    val user1 = userStats.filter(_.userId==1).first()


    assert(user1.numberOfMerchant==8)
    assert(user1.numberOfPresentationMode==5)
    assert(Math.abs(user1.averageAmount-419.2222)<0.0001)
    assert(Math.abs(user1.transactionsPerDay-0.07178)<0.0001)
    assert(Math.abs(user1.geographicDispersion-0.06228)<0.00001)

  }
}
