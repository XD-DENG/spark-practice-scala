# Spark Practice (RDDs in Scala)

This repo is a ***Scala*** version "mirror" of my another Github repo [Spark-practice](https://github.com/XD-DENG/Spark-practice) (which focused on PySpark). I will use Spark-Scala to look into a downloading log file in .CSV format.


- [1. Sample Data](#1-sample-data)
- [2. How to Use Spark (Scala) Interactively](#2-how-to-use-spark-scala-interactively)
  - [Start Spark](#start-spark)
  - [Load Data](#load-data)
  - [Show the Head](#show-the-head-first-n-rows)
  - [Transformation (map & flatMap)](#transformation-map--flatmap)
  - [Reduce and Counting](#reduce-and-counting)
  - [Sorting](#sorting)
  - [Filter](#filter)
  - [Collect Result ('Export' into Scala)](#collect-result-export-into-scala)
  - [Set Operation](#set-operation)
  - [Join](#join)
  - [Persisting (Caching)](#persisting-caching)
- [References](#references)
- [License](#license)



## 1. Sample Data
The sample data we use here is from http://cran-logs.rstudio.com/. It is the full downloads log of R packages from Rstudio's CRAN mirror on December 12 2015 (you can get the data in the `sample_data` folder of this repository). 

![\[pic link\]](https://github.com/XD-DENG/Spark-practice/blob/master/sample_data/data_screenshot.png?raw=true)

We will try to use Spark to do some simple analytics on this data.




## 2. How to Use Spark (Scala) Interactively

### Start Spark

We can directly call `spark-shell` to start Spark

```bash
$  ./bin/spark-shell
```

After Spark is started, a default SparkContext will be created (usually named as "sc").

### Load Data

The most common method used to load data is `textFile`. This method takes an URI for the file (local file or other URI like hdfs://), and will read the data in as a collections of lines. 

```scala
scala> val raw_content = sc.textFile("2015-12-12.csv")
raw_content: org.apache.spark.rdd.RDD[String] = 2015-12-12.csv MapPartitionsRDD[1] at textFile at <console>:24

scala> println(raw_content.getClass())
class org.apache.spark.rdd.MapPartitionsRDD

scala> println(raw_content.count())
421970
```

You may want to take note of that all of Spark’s file-based input methods, including `textFile`, support running on directories, compressed files, and wildcards as well [1]. For example, you can use textFile("/my/directory"), textFile("/my/directory/*.txt"), and textFile("/my/directory/*.gz").

This feature also makes things much simpler when we have multiple text data files to load. By giving the directory under where these files are ("/my/directory"), we can load many data files with only one line. Additionally, we can also specify the file types we would like to load using wildcard, like with `textFile("/my/directory/*.txt")`, we will only load those files with `.txt` file type in the directory we specified.


### Show the Head (First `n` rows)
We can use `take` method to return first `n` rows.

```scala
scala> raw_content.take(5).foreach(println)
"date","time","size","r_version","r_arch","r_os","package","version","country","ip_id"
"2015-12-12","13:42:10",257886,"3.2.2","i386","mingw32","HistData","0.7-6","CZ",1
"2015-12-12","13:24:37",1236751,"3.2.2","x86_64","mingw32","RJSONIO","1.3-0","DE",2
"2015-12-12","13:42:35",2077876,"3.2.2","i386","mingw32","UsingR","2.0-5","CZ",1
"2015-12-12","13:42:01",266724,"3.2.2","i386","mingw32","gridExtra","2.0.0","CZ",1
```
We can also take samples randomly with `takeSample` method. With `takeSample` method, we can give three arguments and need to give at least two of them. They are "if replacement", "number of samples", and "seed" (optional).

```scala
scala> raw_content.takeSample(true, 5, seed=100).foreach(println)
"2015-12-12","16:27:29",516053,"3.2.3","x86_64","linux-gnu","agop","0.1-4","CN",41
"2015-12-12","08:32:56",79946,"3.2.1","x86_64","mingw32","markdown","0.7.7","HK",9859
"2015-12-12","03:40:47",1487505,"3.2.3","x86_64","mingw32","shiny","0.12.2","US",429
"2015-12-12","15:43:59",153893,"3.2.2","x86_64","mingw32","gridBase","0.4-7","US",9130
"2015-12-12","15:43:39",30625,"3.1.2","x86_64","mingw32","DBI","0.3.1","US",9104
```
If we specified the last argument, i.e. seed, then we can reproduce the samples exactly.


### Transformation (map & flatMap)

We may note that each row of the data is a character string, and it would be more convenient to have an array instead. So we use `map` to transform them and use `take` method to get the first three rows to check how the resutls look like.

```scala
scala> var content = raw_content.map(x => x.split(','))

scala> content.take(2)
res27: Array[Array[String]] = Array(Array("date", "time", "size", "r_version", "r_arch", "r_os", "package", "version", "country", "ip_id"), Array("2015-12-12", "13:42:10", 257886, "3.2.2", "i386", "mingw32", "HistData", "0.7-6", "CZ", 1))

```

I would say `map(function)` method is one of the most basic and important methods in Spark. It returns a new distributed dataset formed by passing each element of the source through a function specified by user [1]. 

There are several ways to define the functions for `map`. Normally, we can use anonymous function to do this, just like what I did above. This is suitable for simple functions (one line statement). For more complicated process, we can also define a separate function and invoke it within `map` method. 

We have an example here: you may have noted the double quotation marks in the imported data above, and I want to remove all of them in each element of our data

```scala
// remove the double quotation marks in the imported data
scala> def clean(x: Array[String]): Array[String] = {x.map(_.filter(_ != '"'))}
test: (x: Array[String])Array[String]

scala> content = content.map(clean)
content: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[26] at map at <console>:30

scala> content.take(2)
res34: Array[Array[String]] = Array(Array(date, time, size, r_version, r_arch, r_os, package, version, country, ip_id), Array(2015-12-12, 13:42:10, 257886, 3.2.2, i386, mingw32, HistData, 0.7-6, CZ, 1))

```

We can also use multiple `map` operators in a single statement. For example, `raw_content.map(_.split(',')).map(clean)` will return the same results.

The same function-defining approach is also applicable to `filter` method which will be introduced later.

You may have noted that there is another method named `flatMap`. Then what's the difference between `map` and `flatMap`? We can look into a simple example firstly.

```scala
scala> val text=Array("a b c", "d e", "f g h")
text: Array[String] = Array(a b c, d e, f g h)

scala> sc.parallelize(text).map(_.split(' ')).collect()
res38: Array[Array[String]] = Array(Array(a, b, c), Array(d, e), Array(f, g, h))

scala> sc.parallelize(text).flatMap(_.split(" ")).collect()
res39: Array[String] = Array(a, b, c, d, e, f, g, h)

```

To put it simple (maybe not precise), we can say that `map` will return a **sequence** of the same length as the original data. In this sequence each element is a **sub-sequence** corresponding to one element in original data. `flatMap` will return a sequence whose length equals to the sum of the lengths of all sub-sequance returned by `map`.       



### Reduce and Counting

Here I would like to know how many downloading records each package has. For example, for R package "Rcpp", I want to know how many rows belong to it.

```scala

scala> val package_count = content.map(x => (x(6), 1)).reduceByKey((a,b) => a+b)

scala> println(package_count.getClass)
class org.apache.spark.rdd.ShuffledRDD

scala> println(package_count.count)
8660

scala> package_count.take(5).foreach(println)
(HarmonicRegression,15)
(csound,3)
(classifly,29)
(ROI,32)
(ftsspec,11)
```

To achive the same purpose, we can also use `countByKey` method. The result returned by it is in hashmap (like dictionary) structure.

```scala
scala> val package_count_2 = content.map(x => (x(6), 1)).countByKey()

scala> println(package_count_2.getClass)
class scala.collection.immutable.HashMap$HashTrieMap

scala> println(package_count_2("Rcpp"))
4783

scala> println(package_count_2("ggplot2"))
3913
```

Please note that `countByKey` method ONLY works on RDDs of type (K, V), returning a hashmap of (K, int) pairs with the COUNT of each key [1]. **The value of `V` will NOT affect results from `countByKey`!** Just like the example below.

```scala
scala> val package_count_2 = content.map(x => (x(6), 1)).countByKey()
scala> println(package_count_2("ggplot2"))
3913

scala> val package_count_2 = content.map(x => (x(6), 10)).countByKey()
scala> println(package_count_2("ggplot2"))
3913

scala> val package_count_2 = content.map(x => (x(6), "test")).countByKey()
scala> println(package_count_2("ggplot2"))
3913

```



### Sorting

After counting using `reduce` method, I may want to know the rankings of these packages based on how many downloads they have. Then we need to use `sortByKey` method. Please note: 

* The `Key` here refers to the first element of each tuple.
* The argument of `sortByKey` (`true` or `false`) will determine if we're sorting descently (`false`) or ascently (`true`).

**The usage of `._1` or `._2` below is for tuples.**

```scala
scala> package_count.map(x => (x._2, x._1)).sortByKey(true).take(5).foreach(println)
(1,graphite)
(1,RAFM)
(1,waldwolf)
(1,JMLSD)
(1,cxxPack)

scala> package_count.map(x => (x._2, x._1)).sortByKey(false).take(5).foreach(println)
(4783,Rcpp)
(3913,ggplot2)
(3748,stringi)
(3449,stringr)
(3436,plyr)
```

Other than sorting by key (normally it's the first element in each observation), we can also specify by which element to sort using method `sortBy`, 

```scala
scala> package_count.sortBy(_._2).take(5).foreach(println)
(graphite,1)
(RAFM,1)
(waldwolf,1)
(JMLSD,1)
(cxxPack,1)

scala> package_count.sortBy(_._2, false).take(5).foreach(println)
(Rcpp,4783)
(ggplot2,3913)
(stringi,3748)
(stringr,3449)
(plyr,3436)

scala> package_count.sortBy(x => x._2, false).take(5).foreach(println)
(Rcpp,4783)
(ggplot2,3913)
(stringi,3748)
(stringr,3449)
(plyr,3436)
```


### Filter
We can consider `filter` as the `SELECT * from TABLE WHERE ???` statement in SQL. It can help return a new dataset formed by selecting those elements of the source on which the function specified by user returns true.

For example, I would want to obtain these downloading records of R package "Rtts" from China (CN), then the condition is "package == 'Rtts' AND country = 'CN'".

```scala
scala> println(content.filter(x => (x(6) == "Rtts") & (x(8) == "CN")).count())
1

scala> content.filter(x => (x(6) == "Rtts") & (x(8) == "CN")).take(1)(0).foreach(println)
2015-12-12
20:15:24
23820
3.2.2
x86_64
mingw32
Rtts
0.3.3
CN
41
```

### Collect Result ('Export' into Scala)

Almost all the operations listed above were done as RDD (Resilient Distributed Datasets). We can say that they were implemented 'within' Spark. And we may want to transfer some dataset into Scala itself.

`take` method we used above can help us fulfill this purpose partially. But we also have `collect` method to do this, and the difference between `collect` and `take` is that the former will return all the elements in the dataset by default and the later one will return the first `n` rows (`n` is specified by user). Meanwhile, we also need to be careful when we use `collect`, since you may run out of your memory on Master node. In some references, it's suggested to NEVER use `collect()` in production.

```scala
scala> val temp = content.filter(x => (x(6) == "Rtts") & (x(8) == "US")).collect()

scala> println(temp.getClass)
class [[Ljava.lang.String;

scala> temp(0).foreach(println)
2015-12-12
04:52:36
23820
3.2.3
i386
mingw32
Rtts
0.3.3
US
1652
```

### Set Operation

Like the set operators in vanilla SQL, we can do set operations in Spark. Here we would introduce `union`, `intersection`, and `distinct`. We can make intuitive interpretations as below.

- *union of A and B*: return elements of A AND elements of B.
- *intersection of A and B*: return these elements existing in both A and B.
- *distinct of A*: return the distinct values in A. That is, if element `a` appears more than once, it will only appear once in the result returned.

```scala
scala> println(raw_content.count())
421970

// one set's union with itself equals to its "double"
scala> println(raw_content.union(raw_content).count())
843940

// one set's intersection with itself equals to its disctinct value set
scala> println(raw_content.intersection(raw_content).count())
421553

scala> println(raw_content.distinct.count())
421553

```

**We may need to note that if each line of our data is an array instead of a string, `intersection` and `distinct` methods can't work properly**. This is why I used `raw_content` instead of `content` here as example.



### Join

Once again, I have found the data process methods in Spark is quite similar to that in vanilla SQL, like I can use `join` method in Spark, which is a great news! **Outer joins** are also supported through `leftOuterJoin`, `rightOuterJoin`, and `fullOuterJoin` [1]. Additionally, `cartesian` is available as well (**please note [Spark SQL](https://spark.apache.org/sql/) is available for similar purpose and would be preferred & recommended**).

When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key[1].

```scala
// generate a new RDD in which the 'country' variable is KEY
scala> val content_modified=content.map(x => (x(8), 1))

// prepare a mapping table of the abbreviates of four countries and their full names.
scala> val local_mapping=Array(("DE", "Germany"), ("US", "United States"), ("CN", "China"), ("IN","India"))
local_mapping: Array[(String, String)] = Array((DE,Germany), (US,United States), (CN,China), (IN,India))

scala> val mapping = sc.parallelize(local_mapping)


// join
scala> content_modified.join(mapping).takeSample(false, 5, seed=1).foreach(println)
(US,(1,United States))
(CN,(1,China))
(US,(1,United States))
(US,(1,United States))
(IN,(1,India))


// left outer join. 
// In the mapping table, we only gave the mappings of four countries, so we found some 'None' values in the returned result below
scala> content_modified.leftOuterJoin(mapping).takeSample(false, 5, seed=1).foreach(println)
(IN,(1,Some(India)))
(TN,(1,None))
(US,(1,Some(United States)))
(DE,(1,Some(Germany)))
(GB,(1,None))
```


### Persisting (Caching)

Some RDDs may be repeatedly accessed, like the RDD *content* in the example above. In such scenario, we may want to pull such RDDs into cluster-wide in-memory cache so that the computing relating to them will not be repeatedly invoked, so that resource and time can be saved. This is called "caching" in Spark, and can be done using `RDD.cache()` or `RDD.persist()` method. 

Spark automatically monitors cache usage on each node and drops out old data partitions in a least-recently-used (LRU) fashion. Of course we can also manually remove an RDD instead of waiting for it to fall out of the cache, using the `RDD.unpersist()` method.

We can use `.getStorageLevel` to check the persistence status (like whether a RDD is already cached or not).

```scala
scala> println(content.getStorageLevel)
StorageLevel(1 replicas)

scala> content.persist()

scala> println(content.getStorageLevel)
StorageLevel(memory, deserialized, 1 replicas)

scala> content.unpersist()

scala> println(content.getStorageLevel)
StorageLevel(1 replicas)

```

Please note caching may make little or even no difference when the data is small or computation is simple. In some case, persisting your RDDs may even make your application slower if the functions that computed your datasets are too simple. So do choose proper storage level when you persist your RDDs [5].




## References
[1] Spark Programming Guide, http://spark.apache.org/docs/latest/programming-guide.html

[2] Submitting Applications, http://spark.apache.org/docs/latest/submitting-applications.html

[3] Spark Examples, http://spark.apache.org/examples.html

[4] Spark Configuration, http://spark.apache.org/docs/latest/configuration.html

[5] Which Storage Level to Choose?, https://spark.apache.org/docs/latest/rdd-programming-guide.html#which-storage-level-to-choose


## License
This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License - [CC BY-NC-SA 4.0](http://creativecommons.org/licenses/by-nc-sa/4.0/legalcode)
