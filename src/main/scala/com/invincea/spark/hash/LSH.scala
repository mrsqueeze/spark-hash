package com.invincea.spark.hash

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._

class LSH(data : RDD[SparseVector], p : Int, m : Int, numRows : Int, numBands : Int, minClusterSize : Int) extends Serializable {
  
  /** run LSH using the constructor parameters */
  def run() : LSHModel = {
    
    //create a new model object
    val model = new LSHModel(p, m, numRows)

    //preserve vector index
    val zdata = data.zipWithIndex().cache()
    
    //compute signatures from matrix
    // - hash each vector <numRows> times
    // - position hashes into bands. we'll later group these signature bins and has them as well
    //this gives us ((vector idx, band#), minhash)
    val signatures = zdata.flatMap(v => model.hashFunctions.flatMap(h => List(((v._2, h._2 % numBands),h._1.minhash(v._1))))).cache()

    //reorganize data for shuffle
    //this gives us ((band#, hash of minhash list), vector id)
    //groupByKey gives us items that hash together in the same band   
    model.bands = signatures.groupByKey().map(x => ((x._1._2, x._2.hashCode), x._1._1)).groupByKey().cache()
    
    //we only want groups of size >= <minClusterSize>
    //(vector id, cluster id)
    model.vector_cluster = model.bands.filter(x => x._2.size >= minClusterSize).map(x => x._2.toList.sorted).distinct().zipWithIndex().map(x => x._1.map(y => (y.asInstanceOf[Long], x._2))).flatMap(x => x.grouped(1)).map(x => x(0)).cache()
    
    //(cluster id, vector id)
    model.cluster_vector = model.vector_cluster.map(x => x.swap).cache()
    
    //(cluster id, List(vector))
    model.clusters = zdata.map(x => x.swap).join(model.vector_cluster).map(x => (x._2._2, x._2._1)).groupByKey().cache()
    
    //compute the jaccard similarity of each cluster
    model.scores = model.clusters.map(row => (row._1, jaccard(row._2.toList))).cache()
    
    model
  }
  
  /** compute a single vector against an existing model */
  def compute(data : SparseVector, model : LSHModel, minScore : Double) : RDD[(Long, Iterable[SparseVector])] = {
     model.clusters.map(x => (x._1, x._2++List(data))).filter(x => jaccard(x._2.toList) >= minScore)
  }
  
  /** compute jaccard between two vectors */
  def jaccard(a : SparseVector, b : SparseVector) : Double = {
    val al = a.indices.toList
    val bl = b.indices.toList
    al.intersect(bl).size / al.union(bl).size.doubleValue
  }
  
  /** compute jaccard similarity over a list of vectors */
  def jaccard(l : List[SparseVector]) : Double = {
    l.foldLeft(l(0).indices.toList)((a1, b1) => a1.intersect(b1.indices.toList.asInstanceOf[List[Nothing]])).size / 
    l.foldLeft(List())((a1, b1) => a1.union(b1.indices.toList.asInstanceOf[List[Nothing]])).distinct.size.doubleValue
  }  
  
}