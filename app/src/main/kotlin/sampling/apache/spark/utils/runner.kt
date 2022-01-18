package sampling.apache.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession

fun sparkSessionRunner(runner: (SparkSession)->Any, appName: String, buildConfig: Map<String, String>): Any{
    val spark = SparkSession
        .builder()
        .master(buildConfig.getOrDefault("master", "local"))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .appName(appName)
        .orCreate  // getOrCreate()

    return runner(spark)
}

fun sparkContextRunner(runner: (JavaSparkContext)->Any, appName: String): Any {
    val conf = SparkConf()
        .setAppName(appName)
        .setMaster("local")
    val sc = JavaSparkContext(conf)
    return runner(sc)
}