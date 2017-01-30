/**
* @Important Please load the object MyFunctions before running any command 
* This file find all restaurant in Las Vegas, and map them into boxes that used in monitoring 
* twitter count. This box number could be used to retrive the twitter count from last hour, 
* which is used for adjusting the ranking of restaurant. 
*/
import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)
// Read all business into Dataframe
val path = "/user/jx734/yelp/yelp_academic_dataset_business.json"
val business = sqlContext.read.json(path)
// Find all restaurants from all business
val restaurants = business.select($"*").where(array_contains(business("categories"), "Restaurants"))
// Get location information of restaurants 
val coordinates = restaurants.select($"business_id", $"latitude", $"longitude")
// Save temporary value
coordinates.rdd.saveAsTextFile("/user/jx734/Vegas/business_coor")
// Save to Hive table 
coordinates.write.saveAsTable("coordinates")
// to load back next time
val coordinates = hiveContext.sql("FROM coordinates SELECT *")
// Further calculation could be done by reading from the intermediate results 
val rest = sc.textFile("/user/jx734/Vegas/business_coor")
// First map the string to key (business_id) value (coordinates) pairs,
// then filter with the coordinate box  
val rest_value =rest.map(MyFunctions.mapToKeyValue).filter(row => MyFunctions.coorFilter(row._2))
rest_value.count
// 5914 in this large box 

// Map the coordinate to the boxes we define in analyzing twitter data
// If it not in the box, give the count value -1, which we used to filter them out
val rest_box = rest_value.map(MyFunctions.mapToBox).filter(row => row._2 >= 0)

// Get all business_id of restaurant in Las Vegas and save them to files 
// This value is further used in analyzing business and reviews 
val rest_keys = rest_box.map(row => row._1)
rest_keys.saveAsTextFile("/user/jx734/Vegas/rest_keys")

// This is only show how to read the values back to Dataframe, so it would be convenient for joins
val rest_keys = sc.textFile("/user/jx734/Vegas/rest_keys").toDF("b_id": String)
// Save to Hive rest_keys 
rest_keys.write.saveAsTable("rest_keys")
// to load back next time
val rest_keys = hiveContext.sql("FROM rest_keys SELECT *")
// Function defined for map/reduce 
object MyFunctions {
  def mapToBox(geoinfo: (String, List[Float])): (String, Int) = {
    // divide the Las Vegas city into 100 boxes, 
    val a = ((geoinfo._2(0)-36.0)/0.0304168).floor.toInt
    val b = ((geoinfo._2(1)+115.34807)/0.034807).floor.toInt
    // location outside this box is not counted 
    if (a >= 0 && a < 10 && b >= 0 && b < 10) {
      (geoinfo._1, a*10+b)
    } else {
      (geoinfo._1, -1)
    }
  };
  def mapToKeyValue(row: String): (String, List[Float]) = {
    // Input is in the format of [business_id,latitude,longitude]
    val i1 = row.indexOf(',')
    val i2 = row.lastIndexOf(',')
    val business = row.substring(1, i1)
    val lat = row.substring(1+i1, i2).toFloat
    val lon = row.substring(i2+1, row.size-2).toFloat
    (business, List(lat, lon))
  };
  // box lat 35~37
  // long -116~-114
  def coorFilter(coor: List[Float]) : Boolean = {
    var res = false
    if (coor(0) >= 35 && coor(0) <= 37) {
      if (coor(1) >= -116 && coor(1) <= -114) {
        res = true
      }
    }
    res
  };
}
