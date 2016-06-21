import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName("spark-scratch").setMaster("local")
val sc = new SparkContext(conf)

val jabberwocky = """
Twas brillig, and the slithy toves
      Did gyre and gimble in the wabe:
All mimsy were the borogoves,
      And the mome raths outgrabe.

“Beware the Jabberwock, my son!
      The jaws that bite, the claws that catch!
Beware the Jubjub bird, and shun
      The frumious Bandersnatch!”

He took his vorpal sword in hand;
      Long time the manxome foe he sought—
So rested he by the Tumtum tree
      And stood awhile in thought.

And, as in uffish thought he stood,
      The Jabberwock, with eyes of flame,
Came whiffling through the tulgey wood,
      And burbled as it came!

One, two! One, two! And through and through
      The vorpal blade went snicker-snack!
He left it dead, and with its head
      He went galumphing back.

“And hast thou slain the Jabberwock?
      Come to my arms, my beamish boy!
O frabjous day! Callooh! Callay!”
      He chortled in his joy.

’Twas brillig, and the slithy toves
      Did gyre and gimble in the wabe:
All mimsy were the borogoves,
      And the mome raths outgrabe
"""
val words = "the and in all were"

val posts = sc.parallelize(jabberwocky.split('\n')
                                      .filter(_.nonEmpty)
                                      .zipWithIndex
                                      .map (_.swap))

val wordList = sc.parallelize(words.split(' ')).map(x => (x.toLowerCase(), x))


val postsPairs = posts.flatMap
    { case (i, s) => s.split("\\W+").map(w=> (w.toLowerCase(), (i, s))) }

     val withExcluded = postsPairs.join(wordList).map(_._2._1)

     val res = posts.subtract(withExcluded)

  // (19,      He went galumphing back.)
  // (22,O frabjous day! Callooh! Callay!”)
  // (21,      Come to my arms, my beamish boy!)

  
