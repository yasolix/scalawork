val singles = Array("this", "is")

val sentences = Array("this Date", 
                      "is there something", 
                      "where are something", 
                      "this is a string")

val rdd = sc.parallelize(sentences) // create RDD

val keys = singles.toSet            // words required as keys.

val result = rdd.flatMap{ sen => 
                    val words = sen.split(" ").toSet; 
                    val common = keys & words;       // intersect
                    common.map(x => (x, sen))        // map as key -> sen
                }
                .groupByKey.mapValues(_.toArray)     // group values for a key
                .collect                             // get rdd contents as array
