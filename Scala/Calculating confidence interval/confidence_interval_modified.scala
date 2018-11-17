import scala.util.Sorting.quickSort
import org.apache.spark.rdd._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column


var data_ext = spark.sql(
"""
SELECT
id,
is_control,
sum(your_value) AS your_value
FROM
test_data
GROUP BY
id,
is_control
"""
)

// For clickout_rev

// Cplitting test / control too two different datasets and selecting only cell_id 2118034735109460
val data_filter_1 = data_ext.filter($"is_control" === 0 && $"your_value" > 0).select("your_value")
val data_filter_2 = data_ext.filter($"is_control" === 1 && $"your_value" > 0).select("your_value")

// Only taking one column for each dataframe
val float_filter_1 = data_filter_1.withColumn("your_value", col("your_value").cast("Float"))
val float_filter_2 = data_filter_2.withColumn("your_value", col("your_value").cast("Float"))


// Converting to RDD
val rowRDD_1 = float_filter_1.rdd
val rowRDD_2 = float_filter_2.rdd

// Mapping dataframe and caching it! For better use when we bootstrap many times 
val data_1 = rowRDD_1.map{case Row(your_value:Float) => (your_value)}.cache()
val data_2 = rowRDD_2.map{case Row(your_value: Float) => (your_value)}.cache()


def getConfIntervalTwoMeans(input1: org.apache.spark.rdd.RDD[Float],
                        input2: org.apache.spark.rdd.RDD[Float],
                        N: Int 
                        )                
                        
    : Array[Double] = {
// Hardcoding the intervals     
var arr = Array(0.005, 0.025,0.05, 0.1, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 0.95, 0.975, 0.995)


// Simulate average of differences
val hist = Array.fill(N){0.0}
for (i <- 0 to N-1) {
    val mean1 = input1.sample(withReplacement = true, fraction = 1.0).mean
    val mean2 = input2.sample(withReplacement = true, fraction = 1.0).mean
    hist(i) = mean2 - mean1
}

// Sort the averages and calculate quantiles
quickSort(hist)



// As there are 17 values in the arr defined we loop through them 
val quar = Array.fill(17){0.0}
for (i <- 0 to arr.size-1) {
    quar(i) = hist((N * arr(i) ).toInt)
}

return quar
}

// Collecting the results in a Array[Double]
// in this example we bootstrap over 1000 samples 
val (quar) = getConfIntervalTwoMeans(data_1, data_2, 1000)


// Sginificant yes / no
if (quar(1) > 0) {
println("We failed to reject H0. It seems like H0 is correct.")
} else {
println("We rejected H0")
}  