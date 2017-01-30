/**
* This file is used for calculate recommendation for each user
* The similiarity between user preference vector and business vector is used as primary ranking signal
* Twitter data is used to adjust the ranking scores 
*/

import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)
// user_id for users with more than 50 reviews
val up_50_keys = up_50.map(row => row._1).toDF("u_id":String)
// find all user_id, business_id pairs in the review
val ub_50 = user_business.join(up_50_keys, $"user_id" === $"u_id").select($"user_id", $"business_id")
// Get the business vectors for these businesses
val ub_50_bv = ub_50.join(bv_df, $"business_id" === $"bv_id").select($"user_id", $"bv")
// Write to hive table ub_50_bv
ub_50_bv.write.saveAsTable("ub_50_bv")
// to load back next time
val ub_50_bv = hiveContext.sql("FROM ub_50_bv SELECT *")
// convert to int vector 
val ub_50_v = ub_50_bv.rdd.map(row=>(row(0).toString, UserVector.mapToKeyValue(row(1).toString)))
// add all vectors from the same users together and do the normallization, so that each preference value 
// is bwteen [0, 1]
val ub_50_up = ub_50_v.reduceByKey(UserVector.addVector).map(UserVector.normallize)
// find all the boxes where the business is in 
val busi_box = sc.textFile("/user/jx734/Vegas/rest_box").map(recFunct.mapToKeyValue)
// Convert to df 
val busi_box_df = busi_box.toDF("b_id":String, "box": String)
// Write to hive table ub_50_bv
busi_box_df.write.saveAsTable("busi_box_df")
// to load back next time
val busi_box_df = hiveContext.sql("FROM busi_box_df SELECT *")
// twitter boxes with tweets count 
val twitter = sc.textFile("/user/jx734/Vegas/geo_box_by_hour")
// suppose we want data at 6pm
val hour = "18"
// get only data at 6pm
val sixpm_t = twitter.filter(s => s.substring(2,4) == hour).map(recFunct.mapTwitter)
sixpm_t.take(100)
val sixpm_t_df = sixpm_t.toDF("t_box":String, "t_count": String)
// get the tweets count for each business (use the same box )
val busi_twitter = busi_box_df.join(sixpm_t_df, $"box" === $"t_box").select($"b_id", $"t_count")

// list of tuples with (business_id, twitter_count) pairs 
val btList = busi_twitter.rdd.map(row => (row(0).toString, row(1).toString.toInt)).zipWithUniqueId.collectAsMap.keySet.toList

// key value pairs of users and first 200 recommendations 
val rec_50 = ub_50_up.map(row => (row._1, rangeRestFunction.rangeRest(row._2, 200)))

// Helper functions in map/reduce 
object recFunct {
  def mapToKeyValue(row: String): (String, Int) = {
    val i1 = row.indexOf(',')
    (row.substring(1, i1), row.substring(i1+1, row.size-1).toInt)
  };
    def mapTwitter(row: String): (Int, Int) = {
    val a = row.substring(2,row.size-1)
    val i1 = a.indexOf(',')
    val i2 = a.indexOf(')')
    val i3 = a.lastIndexOf(',')
    (a.substring(i1+1, i2).toInt, a.substring(i3+1, a.size).toInt)
  };
}

// used to calculating final score together with Twitter data 
val bvMap = bv.zipWithUniqueId.collectAsMap
val tupleList = bvMap.keySet.toList
object rangeRestFunction {
    def rangeRest(userAttrs: List[Float], num: Int): List[String] = {
        val listBuff = new ListBuffer[(String, Float)]()
        for (tuple <- tupleList) {
            val id: String = tuple._1
            var twitterDiscount : Float = 0.0f;
            for (t <- btList) {
              if (t._1 == id) {
                // use 100 as denominator due to the maximun tweets count never exceeded 100 in our 
                // observation. 
                twitterDiscount = t._2.toFloat/100.0f
              }
            }
            val list = tuple._2
            var total: Float = (list, userAttrs).zipped.map((l, u) => l.toFloat * u).sum
            val t = (id, total-twitterDiscount)
            listBuff += t
        }
        listBuff.toList.sortWith(_._2>_._2).take(num).map(row => row._1)
    }
}

// helper function from user_vector file 
object UserVector {
  def mapToKeyValue(row: String): List[Int] = {
    val i1 = row.indexOf('(')
    val i2 = row.lastIndexOf(')')
    val values = row.substring(i1+1, i2).replaceAll(" ", "").split(",").toList
    values.map(x => x.toInt) ++ List(1)
  };

  def addVector(v1: List[Int], v2: List[Int]): List[Int] = {
    v1.zipAll(v2, 0, 0).map { case (a, b) => a + b }
  };

  def normallize(row: (String, List[Int])): (String, List[Float])  = {
    (row._1, row._2.map(x => x.toFloat/row._2(row._2.size-1).toFloat).dropRight(1))
  }
}
