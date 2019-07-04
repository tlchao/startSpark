import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object MaxPrice {
  def main(args: Array[String]){
    //测试1
    /*val conf = new SparkConf().setMaster("local").setAppName("Max Price")
    val sc = new SparkContext(conf)

    sc.textFile(args(0))
      .map(_.split(","))
      .map(rec => ((rec(0).split("-"))(0).toInt, rec(1).toFloat))
      .reduceByKey((a,b) => Math.max(a,b))
      .saveAsTextFile(args(1))*/
    //基础设置
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc= new SparkContext(conf)

    val input = sc.textFile("/Users/chaotan/JavaUtils/spark-2.4.3-bin-hadoop2.7/README.md")
    //统计文件单词数
    val counts: RDD[(String, Int)] = input.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
    counts.foreach(println)

    //统计文件行数
    /*val counts = input.count()
    println(counts)*/
  }
}
