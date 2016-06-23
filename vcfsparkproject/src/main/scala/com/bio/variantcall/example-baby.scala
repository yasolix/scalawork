
// https://www.supergloo.com/fieldnotes/what-is-apache-spark-deconstructing-the-building-blocks-part-1/

val babyNames = sc.textFile("./data/baby_names.csv")
babyNames.count
babyNames.first()

val rows = babyNames.map(line => line.split(","))
rows.map(row => row(2)).distinct.count

val davidRows = rows.filter(row => row(1).contains("DAVID"))
davidRows.count
davidRows.filter(row => row(4).toInt > 10).count()
davidRows.filter(row => row(4).toInt > 10).map( r => r(2) ).distinct.count



val names = rows.map(name => (name(1),1))
names.reduceByKey((a,b) => a + b).sortBy(_._2).foreach ( println _) 


 val filteredRows = babyNames.filter(line => !line.contains("Count")).map(line => line.split(","))
 filteredRows.map ( n => (n(1), n(4).toInt)).reduceByKey((a,b) => a + b).sortBy(_._2).foreach (println _)
 filteredRows.map ( n => (n(1), n(4).toInt)).reduceByKey((a,b) => a + b).sortBy(_._2).collect.foreach (println _)



// spark tutorial

//map 
val rows = babyNames.map(line => line.split(","))

sc.parallelize(List(1,2,3)).map(x=>List(x,x,x)).collect
//flatMap
sc.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x)).collect

//filter
val filteredRows = babyNames.filter(line => !line.contains("Count"))

//mapPartitions
val parallel = sc.parallelize(1 to 9, 3)
parallel.mapPartitions( x => List(x.next).iterator).collect
// compare to the same, but with default parallelize
val parallel = sc.parallelize(1 to 9)
parallel.mapPartitions( x => List(x.next).iterator).collect


//mapPartitionsWithIndex
val parallel = sc.parallelize(1 to 9)
parallel.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", "+x).iterator).collect
// res17: Array[String] = Array(0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 7, 9)
parallel.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", "+x).iterator).collect
// res17: Array[String] = Array(0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 7, 9)
