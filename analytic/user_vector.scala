import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)
// rest_keys is calculated from locations.scala
val rest_keys = sc.textFile("/user/jx734/Vegas/rest_keys").toDF("b_id": String)
//We first read our review data in
val rpath = "/user/jx734/yelp/yelp_academic_dataset_review.json"
val review = sqlContext.read.json(rpath)
//We first join the reviews with only merchants that are Las Vegas restaurants
val joinedDF = review.join(rest_keys, $"business_id" === $"b_id")
644725
// Now user business mapping is a user to las vegas restaurants where he/she gives 3 stars or up 
val user_business = joinedDF.select($"user_id", $"stars", $"business_id").filter("stars>3")
428550
user_business.rdd.saveAsTextFile("/user/jx734/Vegas/ub")
// Write to hive table user_business
user_business.write.saveAsTable("user_business")
// to load back next time
val user_business = hiveContext.sql("FROM user_business SELECT *")
//We now collect the list of merchants(Las Vegas restautants)
val business_list = rest_keys.select("b_id").rdd.map(r => r(0)).collect()
5634
//This is the user list that wrote 644725 3 stars and up reviews
val user_list = user_business.select("user_id").rdd.map(r => r(0)).collect().toSet
173126

val ub = sc.textFile("/user/jx734/Vegas/ub")

// bv_df is calculated from business_vector file
val uv_df = user_business.join(bv_df, $"business_id" === $"bv_id").select($"user_id", $"bv")
//Generate user and its list of attributes vector, with each attributes vector generated from each merchant the user visited
val uv = uv_df.rdd.map(row=>(row(0).toString, UserVector.mapToKeyValue(row(1).toString)))
//Add the vectors elementwise, generate a aggregated preference vector for a user
val up = uv.reduceByKey(UserVector.addVector)
//filter out the users that reviewed less than 50 restaurants
val up_50 = up.filter(row => row._2(row._2.size-1) > 50)
val up_50_norm = up_50.map(UserVector.normallize)

//Generate user vector
object UserVector {
  def mapToKeyValue(row: String): List[Int] = {
    val i1 = row.indexOf('(')
    val i2 = row.lastIndexOf(')')
    val values = row.substring(i1+1, i2).replaceAll(" ", "").split(",").toList
    values.map(x => x.toInt) ++ List(1)
  };
  //Method to add two attrbutes vectors from different merchants for one user
  def addVector(v1: List[Int], v2: List[Int]): List[Int] = {
    v1.zipAll(v2, 0, 0).map { case (a, b) => a + b }
  };
  //Method to normalize the attributes vector
  def normallize(row: (String, List[Int])): (String, List[Float])  = {
    (row._1, row._2.map(x => x.toFloat/row._2(row._2.size-1).toFloat).dropRight(1))
  }
}
