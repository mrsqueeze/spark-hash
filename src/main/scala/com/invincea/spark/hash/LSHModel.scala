package com.invincea.spark.hash

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._


class LSHModel(p : Int, m : Int, numRows : Int) extends Serializable {
  
  /** generate rows hash functions */
  private val _hashFunctions = ListBuffer[Hasher]()
  for (i <- 0 until numRows)
    _hashFunctions += Hasher.create(p, m)
  final val hashFunctions : List[(Hasher, Int)] = _hashFunctions.toList.zipWithIndex
    
  /** the signature matrix with (hashFunctions.size signatures) */
  var signatureMatrix : RDD[List[Int]] = null  

  /** the "bands" ((hash of List, band#), row#) */
  var bands : RDD[((Int, Int), Iterable[Long])] = null
  
  /** (vector id, cluster id) */
  var vector_cluster : RDD[(Long, Long)] = null
  
  /** (cluster id, vector id) */
  var cluster_vector : RDD[(Long, Long)] = null
  
  /** (cluster id, List(Vector) */
  var clusters : RDD[(Long, Iterable[SparseVector])] = null
 
  /** jaccard cluster scores */
  var scores : RDD[(Long, Double)] = null
  
  /** filter out scores below threshold. this is an optional step.*/
  def filter(score : Double) : LSHModel = {
    
    val scores_filtered = scores.filter(x => x._2 > score)
    val clusters_filtered = scores_filtered.join(clusters).map(x => (x._1, x._2._2))
    val cluster_vector_filtered = scores_filtered.join(cluster_vector).map(x => (x._1, x._2._2))
    scores = scores_filtered
    clusters = clusters_filtered
    cluster_vector = cluster_vector_filtered
    vector_cluster = cluster_vector.map(x => x.swap)
    this
  }
  
  //def compare(SparseVector v) : RDD

  
}