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
  test("repeated items test") {


    // suppose each key represents a movie and the value is a rating
    val ratings = List(List((47,5.0), (33,2.0), (54,1.0), (23,5.0)),
                       List((47,5.0), (33,2.0), (54,1.0), (23,4.0), (92,1.0)),
                       List((54,3.0), (123,5.0), (82,1.0), (536,3.0), (92,3.0), (101,2.0)),
                       List((45,2.0), (46,5.0), (47,4.0), (50,1.0), (54,2.0), (75,5.0), (23,4.0), (82,2.0)))

    val rdd = sc.parallelize(ratings)

    // make sure we have 4
    assert(rdd.count() == 4)

    val vctr = rdd.map(a => Vectors.sparse(1000, a).asInstanceOf[SparseVector])

    val lsh = new LSH(data = vctr, p = 1019, m = 1000, numRows = 1000, numBands = 100, minClusterSize = 2, repeatedItems = true)
    val model = lsh.run

    // this should return 1 cluster
    assert(model.clusters.count() == 1)

    // The cluster similarity should be greater than 0.45 (0.462 exactly)
    // The limit on jaccard similarity using bag semantics is 0.5, so this is actually very high
    assert(model.filter(0.45).clusters.count() == 1)
  }
}