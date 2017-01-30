/**
* This file is used to evaluation our model 
* We randomly divided the user-business data into model and test sets
* model set contains 90% of the total data
* We use vector similiarity to calculate the recommendation restaurants 
*/

import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)

// up_50 is calculated in user_vector file, which need information from business_vector file
val up_50_keys = up_50.map(row => row._1).toDF("u_id":String)
val ub_50_all = user_business.join(up_50_keys, $"user_id" === $"u_id").select($"user_id", $"business_id")
val ub_50_rdd = ub_50_all.rdd.map(row=>(row(0).toString, row(1).toString))
// 32688
ub_50_rdd.saveAsTextFile("/user/jx734/Vegas/ub_50_rdd")
// Divide all the user_business pairs into model and test sets. model contains 90% of all data. 
val Array(model_50, test_50) = ub_50_rdd.randomSplit(Array(0.9, 0.1), 1)
model_50.count()
//29448
test_50.count()
//3240
// convert to dataframe 
val model_50_df = model_50.toDF("user_id":String, "business_id":String)
// Join business vector
val model_50_df_bv = model_50_df.join(bv_df, $"business_id" === $"bv_id").select($"user_id", $"bv")
// to vector
// Write to hive table model_50_df_bv
model_50_df_bv.write.saveAsTable("model_50_df_bv")
// to load back next time
val model_50_df_bv = hiveContext.sql("FROM model_50_df_bv SELECT *")
// convert to int vector 
val model_50_v = model_50_df_bv.rdd.map(row=>(row(0).toString, UserVector.mapToKeyValue(row(1).toString)))
// add all vectors from the same users together and do the normallization, so that each preference value 
// is bwteen [0, 1]
val model_50_up = model_50_v.reduceByKey(UserVector.addVector).map(UserVector.normallize)
// Save intermediate results 
model_50_up.saveAsTextFile("/user/jx734/Vegas/model_50_up")
test_50.saveAsTextFile("/user/jx734/Vegas/test_50")

// Get business list with attributes vectors 
val bvMap = bv.zipWithUniqueId.collectAsMap
val tupleList = bvMap.keySet.toList

// user commendation list 
val rec_50 = model_50_up.map(row => (row._1, rangeRestFunction.rangeRest(row._2, 200)))

// List of (String, List[String])
val recMap = rec_50.zipWithUniqueId.collectAsMap.keySet.toList

// Evaluation results using the test data
val res_50 = test_50.map(EvaluateFunction.eva)

// Final result 
val a = res_50.reduceByKey(EvaluateFunction.addT)
val a_f = a.map(row => (row._1, row._2._2.toFloat/row._2._1.toFloat))

// 100 return results 
a_f.filter(row => row._2 > 0.2).count
// res117: Long = 32                                                               
a_f.filter(row => row._2 > 0.1).count
// res118: Long = 104     

// 200 return results
scala> a_f.filter(row => row._2 > 0.2).count
// res122: Long = 36                                                               
scala> a_f.filter(row => row._2 > 0.1).count
// res123: Long = 115                  


// helper functions 
object rangeRestFunction {
    def rangeRest(userAttrs: List[Float], num: Int): List[String] = {
        val listBuff = new ListBuffer[(String, Float)]()
        for (tuple <- tupleList) {
            val id: String = tuple._1
            val list = tuple._2
            var total: Float = (list, userAttrs).zipped.map((l, u) => l.toFloat * u).sum
            val t = (id, total)
            listBuff += t
        }
        listBuff.toList.sortWith(_._2>_._2).take(num).map(row => row._1)
    }
}

object EvaluateFunction {
  def eva(row : (String, String)) : (String, (Int, Int)) = {
    var res = 0
    for (t <- recMap) {
      if (t._1 == row._1 && t._2.contains(row._2)) {
        res = 1
      }
    }
    (row._1, (1, res))
  };
  def addT(t1: (Int, Int), t2: (Int, Int)) : (Int, Int) = {
    (t1._1 + t2._1, t1._2 + t2._2)
  };
}

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
