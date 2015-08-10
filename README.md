spark-hash
==============================

Locality sensitive hashing for [Apache Spark](http://spark.apache.org/).
This implementation was largely based on the algorithm described in chapter 3 of [Mining of Massive Datasets](http://mmds.org/) with some modifications for use in spark.

Maven Central Repository
--------------
spark-hash is on maven central and is accessible at:

	<dependency>
       <groupId>com.invincea</groupId>
       <artifactId>spark-hash</artifactId>
       <version>0.1.3</version>
    </dependency>

Building
--------------
spark-hash can be built using maven

	> mvn clean package
	
**Note: to use the scripts included in this release, make sure $SPARK_HOME is set**


Example Data
-----
We executed some nmap probes and now would like to group IP addresses with similar sets of exposed ports. An small example of this dataset has been provided in data/sample.dat for illustration. 

As part of the preprocessing which was performed on nmap xml files, we flattened IP addresses with identical port sets. The flattening did not factor the time when the port was open. This processing resulted in the following dataset which is highlighted with the Spark REPL in local mode:

	bash$ ./shell_local.sh
	scala> val port_set : org.apache.spark.rdd.RDD[(List[Int], Int)] = sc.objectFile("data/sample.dat")
	
	scala> port_set.take(5).foreach(println)
	(List(21, 23, 443, 8000, 8080),1)
	(List(80, 3389, 49152, 49153, 49154, 49155, 49156, 49157),9)
	(List(21, 23, 80, 2000, 3000),13)
	(List(1723),1)
	(List(3389),1)

Each row in the RDD is a Tuple2 containing the open ports alone with the number of distinct IP addresses that had those ports. In the above example, 13 IP addresses had ports 21, 23, 80, 2000, 3000 opened.

Count the RDD for the total dataset size:
	
	scala> port_set.count()
	res1: Long = 261

Count the total number of IP addresses that contributed to this dataset:

	scala> port_set.map(x => x._2).reduce(_ + _)
	res2: Int = 2273

Show the top five port sets sorted by IP count:

	scala> port_set.sortBy(_._2, false).take(5).foreach(println)
	(List(21, 23, 80),496)
	(List(22, 53, 80),289)
	(List(80, 443),271)
	(List(80),228)
	(List(22, 53, 80, 443),186)

Filter the dataset to drop port sets smaller than 2. Show the top results:

	scala> val port_set_filtered = port_set.filter(tpl => tpl._1.size >= 2)
	scala> port_set_filtered.sortBy(_._2, false).take(5).foreach(println)
	(List(21, 23, 80),496)
	(List(22, 53, 80),289)
	(List(80, 443),271)
	(List(22, 53, 80, 443),186)
	(List(21, 22, 23, 80, 2000),73)

Show the top three port sets by set size:

	scala> port_set.sortBy(_._1.size, false).take(3).foreach(println)
	(List(13, 21, 37, 80, 113, 443, 1025, 1026, 1027, 2121, 3000, 5000, 5101, 5631),1)
	(List(21, 80, 443, 1723, 3306, 5800, 5900, 8081, 49152, 49153, 49154, 49155),1)
	(List(21, 22, 26, 80, 110, 143, 443, 465, 587, 993, 995, 3306),3)


Implementation Details
----

Implementation of LSH follows the rough steps

1. minhash each vector some number of times. The number of times to hash is an input parameter. The hashing function is defined in com.invincea.spark.hash.Hasher. Essentially each element of the input vector is hashed and the minimum hash value for the vector is returned. Minhashing produces a set of signatures for each vector. 
2. Chop up each vector's minhash signatures into bands where each band contains an equal number of signatures. Bands with a greater number of signatures will produce clusters with *greater* similarity. A greater number of bands will increase the probabilty that similar vector signatures  hash to the same value.
3. Order each of the vector bands such that for each band the vector's data for that band are grouped together. 
4. Hash each band and group similar values. These similar values for a given band that hash to the same value are added to the result set.
5. Optionally filter results. An example operation would be to filter out  singleton sets.

#### Example

Input data

1. [21, 25, 80, 110, 143, 443]
2. [21, 25, 80, 110, 143, 443, 8080]
3. [80, 2121, 3306, 3389, 8080, 8443]
4. [13, 17, 21, 23, 80, 137, 443, 3306, 3389]

Let's hash each vector 1000 times. To do this we'll need to create 1000 hash functions and minhash each vector 1000 times.

1. 1000 minhash signatures
2. 1000 minhash signatures
3. 1000 minhash signatures
4. 1000 minhash signatures

Now we want to chop up the signatures into 100 bands where each band will have 10 elements.

1. band 1 (10 elements), band 2 (10 elements), ... , band 100 (10 elements)
2. band 1 (10 elements), band 2 (10 elements), ... , band 100 (10 elements)
3. band 1 (10 elements), band 2 (10 elements), ... , band 100 (10 elements)
4. band 1 (10 elements), band 2 (10 elements), ... , band 100 (10 elements)

For each of the 4 sets of bands, group all of band 1, band 2, .... band 4

	band 1: vector 1 (10 elements), ... , vector 4 (10 elements) 

	band 2: vector 1 (10 elements), ... , vector 4 (10 elements) 
	
	band 100: vector 1 (10 elements), ... , vector 4 (10 elements) 

For each band, hash each of 10 element signatures.

	band 1: 10, 10, 3, 4

	band 2: 6, 5, 2, 4
	
	band 100: 1, 2, 3, 8

Group identical values within each band. This correspond to the similar clusters. In the above example, only two vectors (1 and 2) are deemed similar.

	scala> model.clusters.foreach(println)
	(0,CompactBuffer((65535,[21,25,80,110,143,443],[1.0,1.0,1.0,1.0,1.0,1.0]), (65535,[21,25,80,110,143,443,8080],[1.0,1.0,1.0,1.0,1.0,1.0,1.0])))


Usage
-----

The previously described data can easily be converted into Spark's SparseVector class with the open port corresponding to an index in the vector. With that in mind, there are a few domain specific tunables that will need to be set prior to running LSH

- *p* - a prime number > the largest vector index. In the case of open ports, this number is set to 65537.

- *m* - the number of "bins" to hash data into. Smaller numbers increase collisions. We use 1000.

- *numRows* - the total number of times to minhash a vector. *numRows* separate hash functions are generated. Larger numbers produce more samples and increase the likelihood similar vectors will hash together. 

- *numBands* - how many times to chop *numRows*. Each band will have *numRows*/*numBand* hash signatures. The larger number of elements the higher confidence in vector similarity. 

- *minClusterSize* - a post processing filter function that excludes clusters below a threshold.

There are two ways to execute LSH. The first being a driver class that is submitted to a spark cluster (can be a single machine running in local mode). The second is using spark's REPL. The later is useful for parameter tuning. 


### Driver Class

Executing a driver in local mode. This executes com.invincea.sparki.hash.OpenPortDriver and saves the resulting cluster output to the file results.csv. In this case 50% of the data is sampled and spread over 8 partitions. For normal use the driver will need to be modified to handle data load, requisite transforms, and parameter tuning. Also, for some datasets it may not be practical to save all results to a local driver.

	bash$ ./run_local.sh
	Usage: OpenPortApp <file> <partitions> <data_sample>
	
	bash$ ./run_local.sh data/sample.dat 8 0.5
	
### Spark REPL

	bash$ ./shell_local.sh
	spark>
	import com.invincea.spark.hash.{LSH, LSHModel}
	import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector}
	import org.apache.spark.mllib.linalg.{Matrix, Matrices}


	val port_set : org.apache.spark.rdd.RDD[(List[Int], Int)] = sc.objectFile("data/sample.dat")
	port_set.repartition(8)
	val port_set_filtered = port_set.filter(tpl => tpl._1.size > 3)
		
	val vctr = port_set_filtered.map(r => (r._1.map(i => (i, 1.0)))).map(a => Vectors.sparse(65535, a).asInstanceOf[SparseVector])


	val lsh = new  LSH(data = vctr, p = 65537, m = 1000, numRows = 1000, numBands = 25, minClusterSize = 2)
	val model = lsh.run
	
	spark> model.clusters.count()
	res3: Long = 80648
	
### Finding similar sets for a new point

	val np = List(21, 23, 80, 2000, 8443)
	val nv = Vectors.sparse(65535, np.map(x => (x, 1.0))).asInstanceOf[SparseVector]
	
	//use jaccard score of 0.50 across the entire cluster. This may be a bit harsh for large tests.
	scala> val sim = lsh.compute(nv, model, 0.50)
	
	scala> sim.count()
	res6: Long = 5
	
	scala> sim.collect().foreach(println)
	(9,List((65535,[21,22,23,80,2000,3389,8000],[1.0,1.0,1.0,1.0,1.0,1.0,1.0]), (65535,[21,22,23,80,2000,3389],[1.0,1.0,1.0,1.0,1.0,1.0]), (65535,[21,23,80,2000,8443],[1.0,1.0,1.0,1.0,1.0])))
	(4,List((65535,[21,23,80,81,2000],[1.0,1.0,1.0,1.0,1.0]), (65535,[21,23,80,2000,8081],[1.0,1.0,1.0,1.0,1.0]), (65535,[21,23,80,2000,8443],[1.0,1.0,1.0,1.0,1.0])))
	(5,List((65535,[21,22,23,80,1723,2000,8000],[1.0,1.0,1.0,1.0,1.0,1.0,1.0]), (65535,[21,22,23,80,1723,2000],[1.0,1.0,1.0,1.0,1.0,1.0]), (65535,[21,23,80,2000,8443],[1.0,1.0,1.0,1.0,1.0])))
	(6,List((65535,[21,22,23,53,80,2000,8000],[1.0,1.0,1.0,1.0,1.0,1.0,1.0]), (65535,[21,22,23,53,80,2000],[1.0,1.0,1.0,1.0,1.0,1.0]), (65535,[21,23,80,2000,8443],[1.0,1.0,1.0,1.0,1.0])))
	(7,List((65535,[21,22,23,80,554,2000],[1.0,1.0,1.0,1.0,1.0,1.0]), (65535,[21,22,23,80,554,2000,8000],[1.0,1.0,1.0,1.0,1.0,1.0,1.0]), (65535,[21,23,80,2000,8443],[1.0,1.0,1.0,1.0,1.0])))

### Spark REPL (cluster mode)

	bash$ ./shell_cluster.sh
	import org.apache.spark.SparkContext
	import org.apache.spark.SparkContext._
	import org.apache.spark.SparkConf
	import org.apache.spark.mllib.linalg.{Vectors, SparseVector}
	import java.io._
	import com.invincea.spark.hash.{LSH, LSHModel}

	val port_set : org.apache.spark.rdd.RDD[(List[Int], Int)] = sc.objectFile("sample.dat").repartition(320)
	val port_set_filtered = port_set.filter(tpl => tpl._1.size >= 3)
	val points = port_set.zipWithIndex().map(x => x.swap)
	val vctr = port_set.map(r => (r._1.map(i => (i, 1.0)))).map(a => Vectors.sparse(65535, a).asInstanceOf[SparseVector])
	
	val lsh = new LSH(data = vctr, p = 65537, m = 1000, numRows = 2000, numBands = 100, minClusterSize = 2)
    val model = lsh.run
    
### Finding the largest cluster

	//by count
	model.clusters.sortBy(_._2.size, false).map(x => (x._1, x._2.size)).take(5).foreach(println)
	
	//retrieve underlying data
	model.clusters.sortBy(_._2.size, false).take(5).foreach(println)
	
	
### Tuning Results
As described in the MMDS book, LSH can be tuned by adjusting the number of rows and bands such that:

	threshold = Math.pow(1/bands),(1/(rows/bands))
	
Naturally, the number of rows, bands, and the resulting size of the band (rows/bands) dictates the quality of results yielded by LSH. Higher thresholds produces clusters with higher similarity. Lower thresholds typically produce more clusters but sacrifices similarity. 

Regardless of parameters, it may be good to independently verify each cluster. One such verification method is to calculate the jaccard similarity of the cluster (it is a set of sets). Implementation of jaccard similarity is provided in com.invincea.spark.hash.LSH.jaccard.

 
	
