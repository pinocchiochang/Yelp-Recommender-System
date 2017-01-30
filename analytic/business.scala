/**
* This file is used to analyze the business dataset
* by extracting the attributions from business json object to create a vector for every business
* The business-attribute vector is represented like a binary vector, if the restaurant has this attribute, set 1
* if the restaurant not have this attribute, set 0
* We got a user-preference vector after analytic from user-review dataset,
* and use this vector multiply to every business-attribute vector, and get a value to represent how this user like this restaurant
* and then generate a list of tuple, (business-id, value), and sort them reversely by the value
* That is this user's mostly like restaurant list 
*/

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive
import scala.collection.mutable.ListBuffer

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val path = "/Users/yuchangchen/Documents/Courant/BD/project/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json"

val business = sqlContext.read.json(path)

val restaurants = business.select($"*").where(array_contains(business("categories"), "Restaurants"))

//this rest_keys are filtered by user-review dataset to extract categories "restaurant" from all business
val rest_keys = sc.textFile("/Users/yuchangchen/Documents/Courant/BD/project/key_set").toDF("b_id": String)
val rest_vegas = restaurants.join(rest_keys, $"business_id" === $"b_id")

val att = rest_vegas.select($"business_id", $"attributes.*")

val attnew = att.select($"business_id", $"Accepts Credit Cards", $"Accepts Insurance", $"Ages Allowed", $"Alcohol", $"Attire", $"BYOB", $"BYOB/Corkage", $"By Appointment Only", $"Caters", $"Coat Check", $"Corkage", $"Delivery", $"Dogs Allowed", $"Drive-Thru", $"Good For Dancing", $"Good For Groups", $"Good for Kids", $"Happy Hour", $"Has TV", $"Noise Level", $"Open 24 Hours", $"Order at Counter", $"Outdoor Seating", $"Price Range", $"Smoking", $"Take-out", $"Takes Reservations", $"Waiter Service", $"Wheelchair Accessible", $"Wi-Fi", $"Ambience.*", $"Dietary Restrictions.*", $"Good For.*", $"Music.*", $"Parking.*")

//save the business vector to a temp file
val b = attnew.rdd
b.saveAsTextFile("/Users/yuchangchen/Documents/Courant/BD/project/business_attr")

object BusinessFunction {
  def translate(s: String) : List[Int] = s match {
    case "true" => List(1)
    case "none" => List(0)
    case "null" => List(0)
    case "false" => List(0)
    case "318plus" => List(1, 0, 0)
    case "321plus" => List(0, 1, 0)
    case "3allages" => List(0, 0, 1)
    case "3null" => List(0, 0, 0)
    case "4beer_and_wine" => List(1, 0)
    case "4full_bar" => List(0, 1)
    case "4none" => List(0, 0)
    case "4null" => List(0, 0)
    case "5casual" => List(1, 0, 0)
    case "5formal" => List(0, 1, 0)
    case "5dressy" => List(0, 0, 1)
    case "5null" => List(0, 0, 0)
    case "7no" => List(1, 0, 0)
    case "7yes_free" => List(0, 1, 0)
    case "7yes_corkage" => List(0, 0, 1)
    case "7null" => List(0, 0, 0)
    case "20very_loud" => List(1, 0, 0, 0)
    case "20loud" => List(0, 1, 0, 0)
    case "20quiet" => List(0, 0, 1, 0)
    case "20average" => List(0, 0, 0, 1)
    case "20null" => List(0, 0, 0, 0)
    case "241" => List(1, 0, 0, 0)
    case "242" => List(0, 1, 0, 0)
    case "243" => List(0, 0, 1, 0)
    case "244" => List(0, 0, 0, 1)
    case "24null" => List(0, 0, 0, 0)
    case "25outdoor" => List(1, 0)
    case "25yes" => List(0, 1)
    case "25no" => List(0, 0)
    case "25null" => List(0, 0)
    case "30free" => List(1, 0)
    case "30paid" => List(0, 1)
    case "30no" => List(0, 0)
    case "30null" => List(0, 0)
  };
  def mapToKeyValue(row: String): (String, List[Int]) = {
    val arr = row.substring(1, row.size-1).split(",")
    var res : List[Int] = List()
    val indexes = Set(3,4,5,7,20,24,25,30)
    for( a <- 1 to 63){ 
      if (indexes(a)) {
        res = res ++ translate(a.toString + arr(a))
      } else {
        res = res ++ translate(arr(a))
      }      
    }
    (arr(0), res)
  };
}

// read the business vector from temp file
val ba = sc.textFile("/Users/yuchangchen/Documents/Courant/BD/project/business_attr")
val bv = ba.map(BusinessFunction.mapToKeyValue)

val bvMap = bv.zipWithUniqueId.collectAsMap

//this tupleList is contained by tuple (business_id, value)
val tupleList = bvMap.keySet.toList


object BusinessSort {
    def rangeRest(userAttrs: List[Double]): List[(String, Double)] = {
        val listBuff = new ListBuffer[(String, Double)]()
        for (tuple <- tupleList) {
            val id: String = tuple._1
            val list = tuple._2
            var total: Double = (list, userAttrs).zipped.map {case (list, userAttrs) => list * userAttrs}.sum
            val t = (id, total)
            listBuff += t
        }
        listBuff.toList.sortWith(_._2 > _._2)
    }
}

val userAttrs = List(0.42, 0, 0, 0, 0, 0, 0.31, 0.1, 0, 0.4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.71, 0.86, 0, 0, 0, 0.67, 0, 0, 0, 0, 0, 0, 0.95, 0, 0, 0, 0, 0.81, 0, 0.88, 0.55, 0.37, 0, 0.15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.82, 0, 0.6, 0, 0, 0, 0, 0.71, 0.5, 0, 0, 0.6)
val totalList = BusinessSort.rangeRest(userAttrs)



