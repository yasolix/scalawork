
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

