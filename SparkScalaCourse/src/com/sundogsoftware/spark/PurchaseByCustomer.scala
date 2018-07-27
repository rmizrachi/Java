package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PurchaseByCustomer {
  
  def parsedPurchase (line:String) = {
    
    val fields = line.split(",")
    
    val customerId = fields(0)
    val purchasedAmnt = fields(2).toFloat
    (customerId, purchasedAmnt)
  }
  
  def main(args: Array [String]){
    
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
       // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "AvgSpend")
    
    val orders = sc.textFile("../SparkScala/customer-orders.csv")
    
    val parsedOrders = orders.map(parsedPurchase)
    //val sumOrders = parsedOrders.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 +y._1,x._2 + y._2) )
    val sumOrders = parsedOrders.reduceByKey((x,y) => x+ y ) 
    val sumOrdersSorted = sumOrders.map( x => (x._2, x._1)).sortByKey(false)
    
    //val avgOrderPerCust = sumOrders.mapValues(x => x._1/ x._2)
    
    //val results = avgOrderPerCust.collect()
     //val results = sumOrders.collect()
    
     for(result <- sumOrdersSorted)
     {
       val custId = result._2
       val amount = result._1
       
       println(s"$custId $amount")
     }
    //results.sorted.foreach(println)
  
    
  }
  
}