import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)
// Flatten all business attributes to build a vector 
val path = "/user/jx734/yelp/yelp_academic_dataset_business.json"

// val path = "/Users/Jingjing/projects/realtime_bigdata/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json"
val business = sqlContext.read.json(path)
val restaurants = business.select($"*").where(array_contains(business("categories"), "Restaurants"))

val rest_keys = sc.textFile("/user/jx734/Vegas/rest_keys").toDF("b_id": String)
val rest_vegas = restaurants.join(rest_keys, $"business_id" === $"b_id")

val att = rest_vegas.select($"business_id", $"attributes.*")

val attnew = att.select($"business_id", $"Accepts Credit Cards", $"Accepts Insurance", $"Ages Allowed", $"Alcohol", $"Attire", $"BYOB", $"BYOB/Corkage", $"By Appointment Only", $"Caters", $"Coat Check", $"Corkage", $"Delivery", $"Dogs Allowed", $"Drive-Thru", $"Good For Dancing", $"Good For Groups", $"Good for Kids", $"Happy Hour", $"Has TV", $"Noise Level", $"Open 24 Hours", $"Order at Counter", $"Outdoor Seating", $"Price Range", $"Smoking", $"Take-out", $"Takes Reservations", $"Waiter Service", $"Wheelchair Accessible", $"Wi-Fi", $"Ambience.*", $"Dietary Restrictions.*", $"Good For.*", $"Music.*", $"Parking.*")

val b = attnew.rdd
b.count 
b.saveAsTextFile("/user/jx734/Vegas/business_att")
val ba = sc.textFile("/user/jx734/Vegas/business_att")
// business id : business vector
val bv = ba.map(BusiFunct.mapToKeyValue)


object BusiFunct {
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
// Attributes that are not boolean value
val Age_list = attnew.select("Ages Allowed").rdd.map(r => r(0)).collect().toSet // 63 + 3 index 3
Set(null, 18plus, 21plus, allages)
val Alcohol_list = attnew.select("Alcohol").rdd.map(r => r(0)).collect().toSet // 66 + 2 index 4
Set(beer_and_wine, none, null, full_bar)
val Attire_list = attnew.select("Attire").rdd.map(r => r(0)).collect().toSet // 68 + 3 index 5
Set(casual, null, formal, dressy)
val BYOB_list = attnew.select("BYOB/Corkage").rdd.map(r => r(0)).collect().toSet // 71 + 3 index 7
Set(null, no, yes_free, yes_corkage)
val Noise_list = attnew.select("Noise Level").rdd.map(r => r(0)).collect().toSet // 74 + 4 index 20
Set(null, very_loud, loud, quiet, average)
val Price_list = attnew.select("Price Range").rdd.map(r => r(0)).collect().toSet // do not need to add new column for this  index 24
Set(null, 1, 2, 3, 4)
val Smoking_list = attnew.select("Smoking").rdd.map(r => r(0)).collect().toSet // 78 + 2 index 25
Set(null, no, outdoor, yes)
val Wi_Fi_list = attnew.select("Wi-Fi").rdd.map(r => r(0)).collect().toSet // 80 + 2 only free and paid should be considered  index 30
Set(no, null, free, paid)
