Distributed LSH (DLSH)
=======================
Locality-Sensitive Hashing (LSH) is one of the most important algorithms for solving the c-Approximate Nearest Neighbor (c-ANN) problem in high-dimensional space. With the rapid rise in data, however, the single node LSH algorithm can hardly handle massive data. Thus it is reasonable to use distributed computing framework to tackle c-ANN problem with massive data. One of the most commonly used frameworks is MapReduce. Consequently, we adopt MapReduce in our experiment on distributed LSH algorithm.


Naive DLSH
--------------
The typical approach to use MapRduce to solve c-ANN problem is cutting the hash tables of LSH into several blocks so that each block of hash tables can be processed by one computing node. Since the LSH algorithm that solves c-ANN problem has to enlarge the search radius when it can not find enough nearest neighbors within the current radius, it must acquire the number of nearest neighbors found so far from all hash tables to judge whether it should continue searching by expanding the radius.

Unfortunately, computing nodes of MapReduce are relatively independent when running, which means that it is difficult for LSH to get the number of nearest neighbors from all blocks of hash tables that are processed by different nodes. The current related work of distributed LSH avoids this paradox by building hash tables for just one radius, which is not capable of solving c-ANN problem.

Therefore, we implement a distributed LSH algorithm that solves c-ANN problem, named Naive Distributed LSH (NDLSH). NDLSH solves the paradox through a simple but brute way, i.e., submitting new MapReduce jobs every time it needs to expand the search radius. However, this means the huge data set must be read into memory every time new jobs are executed, which is very time-consuming and somewhat unacceptable.


MapReduce LSH
--------------
Furthermore, we present a more optimal algorithm, called MapReduce LSH (MRLSH). The idea of MRLSH is partitioning the original data set into several sub data sets then building hash tables for each sub data set separately. During the search phase, MRLSH does parallel searching in these sub data sets then collects the search results.

Since each sub data set is mutually independent, there is no need to acquire the search states of other sub data sets when enlarging search radius. In other words, the search phase of MRLSH can be completed in a MapReduce job. This bypasses the problem that NDLSH encounters and improves the search response time significantly.


Dependencies
--------------
* Hadoop 1.0.4
* Maven
