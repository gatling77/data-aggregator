package it.gatling77.mldemo.dataaggregator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Transaction(userid: Int, timestamp: Long, amount: Double, lat: Double, lon:Double, merchant: String, presentationMode:Int, isFraud: Boolean) extends Serializable
case class UserStats(userId:Int, transactionsPerDay: Double, averageAmount: Double, numberOfMerchant: Int, numberOfPresentationMode: Int, geographicDispersion: Double)

object Aggregator{
    def main(args: Array[String]): Unit = {
      val file = "src/main/resources/data.csv"; //replace with arg
      val aggregator = new Aggregator(file)


    }

}

/**
  * Created by gatling77 on 3/19/18.
  */
class Aggregator(file: String) extends Serializable{

  lazy val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Data-aggregator")
  lazy val sc = new SparkContext(sparkConf)

  def readRawData():RDD[String] = {
    sc.textFile(file)
  }


  def toTransactions(lines: RDD[String]): RDD[Transaction] =
    lines.map(line=>{
        val data = line.split(";")
        Transaction(
          data(0).toInt,
          data(1).toLong,
          data(2).toDouble,
          data(3).toDouble,
          data(4).toDouble,
          data(5),//.toString
          data(6).toInt,
          data(7).toBoolean
        )
      }
    )

  def excludeFraud(transactions: RDD[Transaction]): RDD[Transaction] ={
    transactions.filter(t  => !t.isFraud)
  }

  def userStats(transactions: RDD[Transaction]):RDD[UserStats]={
      val transactionsPerUser = transactions.groupBy(_.userid).cache()

    val transactionsPerDay =  transactionsPerUser.mapValues(tx=>tx.size * 86400d / (tx.maxBy(_.timestamp).timestamp - tx.minBy(_.timestamp).timestamp))

    val averageAmount = transactionsPerUser.mapValues(tx=>tx.foldLeft(0d)((sum,t)=>sum+t.amount)/tx.size)
    val geographicDispersionIndex = transactionsPerUser
      .mapValues(tx=>(tx,tx.map(_.lat).sum/tx.size,tx.map(_.lon).sum/tx.size))
      .mapValues{case (tx,mlat,mlon)=>Math.sqrt(tx.foldLeft(0d)((sum,t)=>sum+Math.pow(t.lat-mlat,2)+Math.pow(t.lon-mlon,2))/tx.size)}
    val countMerchant = transactions.map(t=>(t.userid,t.merchant)).distinct().groupByKey().mapValues(m=>m.size)
    val countPresentationMode = transactions.map(t=>(t.userid,t.presentationMode)).distinct().groupByKey().mapValues(p=>p.size)


    transactionsPerDay.join(averageAmount).join(geographicDispersionIndex).join(countMerchant).join(countPresentationMode).map{
      case (id,((((txPerDay,avgAmount),geo),countMerchant),countPres))=> UserStats(id,txPerDay,avgAmount,countMerchant,countPres,geo)
    }

  }


}