package com.invincea.spark.hash

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vectors, SparseVector}
import java.io._

object OpenPortApp {
    def main(args: Array[String]) {
      
      if(args.length != 3) {
        println("Usage: OpenPortApp <file> <partitions> <data_sample>")
        System.exit(1)
      } 

      //parse input arguments
      val file : String = args(0)
      val partitions : Int =  args(1).toInt
      val data_sample : Float = args(2).toFloat

      //spark job setup
      val conf = new SparkConf().setAppName("Open Port LSH")
      val sc = new SparkContext(conf)

      //read data file in as a RDD, partition RDD across <partitions> cores  
      val port_set : org.apache.spark.rdd.RDD[(List[Int], Int)] = sc.objectFile(file).repartition(partitions)
      
      //remove any port sets less than size
      val port_set_filtered = port_set.filter(tpl => tpl._1.size >= 2)

      //take a random sample of the data      
      val sample = port_set_filtered.sample(false, data_sample).repartition(partitions)

      //include index with each point sample
      val points = sample.zipWithIndex().map(x => x.swap)

      //convert points to vectors
      val vctr = sample.map(r => (r._1.map(i => (i, 1.0)))).map(a => Vectors.sparse(65535, a).asInstanceOf[SparseVector])

      //run locality sensitive hashing
      val lsh = new  LSH(data = vctr, p = 65537, m = 1000, numRows = 1000, numBands = 25, minClusterSize = 2)
      val model = lsh.run


      println("samples: " + sample.count())
      println("clusters: " + model.clusters.count())
      
      //-- generate a CSV of the results
      //-- cluster original points with scores
      val points_clustered_scores = points.join(model.vector_cluster).map(x => (x._2._2, x._2._1)).groupByKey().join(model.scores)

      //--port list union, cluster size, # ip addresses, 
      val score_coverage_size = points_clustered_scores.map(x => (x._2._2,x._2._1.foldLeft(List())((a1, b1) => a1.union(b1._1.asInstanceOf[List[Nothing]])).distinct.size, x._2._1.size, x._2._1.foldLeft(0)((a,b) => a + b._2))).collect()

      //write out the results
      val writer = new PrintWriter(new File("results.csv" ))
      writer.write("score,port_coverage,cluster_size,num_ips\n")
      score_coverage_size.foreach(x => writer.write(x._1 + "," + x._2 + "," + x._3 + "," + x._4 +"\n"))
      
      writer.flush
      writer.close
      
    }
}
