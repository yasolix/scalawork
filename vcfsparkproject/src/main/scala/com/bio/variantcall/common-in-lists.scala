
// http://stackoverflow.com/questions/33926472/scala-to-find-common-values-between-two-lists

val str = "a,b,c,d,e\n" +
  "f,g,h,i,j\n" +
  "b,g,k,l,m\n" +
  "g,h,o,p,q"

val rows = str.split("\n")
val splittedRows = rows.map(_.split(","))

val stringsInSecondColumn = splittedRows.map(_.apply(1)).toSet    

val result = splittedRows.filter { row =>
  stringsInSecondColumn.contains(row.apply(0))
}
result.foreach(x => println(x.mkString(",")))