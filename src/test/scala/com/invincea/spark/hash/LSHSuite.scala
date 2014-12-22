package com.invincea.spark.hash

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{SparseVector, Vectors}

class LSHSuite extends FunSuite with LocalSparkContext {
  test("simple test") {

    
    val data = List(List(21, 25, 80, 110, 143, 443),
                    List(21, 25, 80, 110, 143, 443, 8080),
                    List(80, 2121, 3306, 3389, 8080, 8443),
                    List(13, 17, 21, 23, 80, 137, 443, 3306, 3389))
                    
    val rdd = sc.parallelize(data)
    
    //make sure we have 4
    assert(rdd.count() == 4)
    
    val vctr = rdd.map(r => (r.map(i => (i, 1.0)))).map(a => Vectors.sparse(65535, a).asInstanceOf[SparseVector])
    
    //make sure we still have 4
    assert(vctr.count() == 4)
    
    
    val lsh = new  LSH(data = vctr, p = 65537, m = 1000, numRows = 1000, numBands = 100, minClusterSize = 2)
    val model = lsh.run
    
    //this should return 1 cluster
    assert(model.clusters.count() == 1)
    
    //filter for clusters with jaccard score > 0.85. should still return 1    
    assert(model.filter(0.85).clusters.count() == 1)
    
  }
}