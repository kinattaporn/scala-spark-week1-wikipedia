package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking extends WikipediaRankingInterface {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
  val langsTest = List("the", "of", "categories")

  val conf: SparkConf = new SparkConf().setAppName("week1-wikipedia").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.parallelize`, `WikipediaData.lines` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.lines.map(x => WikipediaData.parse(x))).cache()
  val wikiRdd10: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.lines.take(10).map(x => WikipediaData.parse(x))).cache()

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.aggregate(0)(
      (acc, article) => {
        if (article.mentionsLanguage(lang)) {
          println(s"acc = ${acc + 1}, $article")
          acc + 1
        }
        else {
          println(s"acc = $acc, $article")
          acc
        }
      }
      , (acc1, acc2) => {
        println(s"acc1 = $acc1, acc2 = $acc2")
        acc1 + acc2
      }
    )
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs
      .map(lang => (lang, occurrencesOfLang(lang, rdd)))  // List((the,4), (of,5), (categories,2))
      .sortBy(-_._2)                                      // List((of,5), (the,4), (categories,2))
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    rdd
      .map(article => (langs.filter(lang => article.mentionsLanguage(lang)), article))
        //    (List(the, of),WikipediaArticle(...))
        //    (List(),WikipediaArticle(...))
        //    (List(),WikipediaArticle(...))
        //    (List(the, of, categories),WikipediaArticle(...))
        //    ...
      .map(x => x._1.map(eachLang => (eachLang, x._2)))
        //    List((the,WikipediaArticle(...)), (of,WikipediaArticle(...)))
        //    List()
        //    List()
        //    List((the,WikipediaArticle(...)), (of,WikipediaArticle(...)), (categories,WikipediaArticle(...)))
        //    ...
      .flatMap(x => x)
        //    (the,WikipediaArticle(...))
        //    (of,WikipediaArticle(...))
        //    (the,WikipediaArticle(...))
        //    (of,WikipediaArticle(...))
        //    (categories,WikipediaArticle(...))
        //    ...
      .groupByKey()
        //    (of,CompactBuffer(WikipediaArticle(...), WikipediaArticle(...), WikipediaArticle(...), WikipediaArticle(...), WikipediaArticle(...)))
        //    (categories,CompactBuffer(WikipediaArticle(...), WikipediaArticle(...)))
        //    (the,CompactBuffer(WikipediaArticle(...), WikipediaArticle(...), WikipediaArticle(...), WikipediaArticle(...)))
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index
      .map(x => (x._1, x._2.size))
      .sortBy(-_._2)
        //    (of,5)
        //    (the,4)
        //    (categories,2)
      .collect().toList
        //    List((of,5), (the,4), (categories,2))
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd
      .map(article => (langs.filter(lang => article.mentionsLanguage(lang)), article))
        //    (List(the, of),WikipediaArticle(...))
        //    (List(),WikipediaArticle(...))
        //    (List(),WikipediaArticle(...))
        //    (List(the, of, categories),WikipediaArticle(...))
        //    ...
      .map(x => x._1.map(eachLang => (eachLang, x._2)))
        //    List((the,WikipediaArticle(...)), (of,WikipediaArticle(...)))
        //    List()
        //    List()
        //    List((the,WikipediaArticle(...)), (of,WikipediaArticle(...)), (categories,WikipediaArticle(...)))
        //    ...
      .flatMap(x => x)
        //    (the,WikipediaArticle(...))
        //    (of,WikipediaArticle(...))
        //    (the,WikipediaArticle(...))
        //    (of,WikipediaArticle(...))
        //    (categories,WikipediaArticle(...))
        //    ...
      .map(x => (x._1, 1))
        //    (the,1)
        //    (of,1)
        //    (the,1)
        //    (of,1)
        //    (categories,1)
        //    ...
      .reduceByKey(_+_)
        //    (of,5)
        //    (categories,2)
        //    (the,4)
      .sortBy(-_._2)
        //    (of,5)
        //    (the,4)
        //    (categories,2)
      .collect().toList
        //    List((of,5), (the,4), (categories,2))
  }

  def main(args: Array[String]): Unit = {
    println("------------------------------------ wikiList")
    val wikiList = WikipediaData.lines.take(2).map(x => WikipediaData.parse(x))
    println(wikiList.getClass.getName)
    println(wikiList)
    println("------------------------------------ wikiRdd10")
    wikiRdd10.foreach(println)
    println("------------------------------------ occurrencesOfLang")
//    occurrencesOfLang("the", wikiRdd10) // 4
//    occurrencesOfLang("of", wikiRdd10) // 5
//    occurrencesOfLang("categories", wikiRdd10) // 2
    println("------------------------------------ rankLangs")
//    println(rankLangs(langsTest, wikiRdd10))
    println("------------------------------------ makeIndex")
//    makeIndex(langsTest, wikiRdd10).foreach(println)
    println("------------------------------------ rankLangsUsingIndex")
//    println(rankLangsUsingIndex(makeIndex(langsTest, wikiRdd10)))
    println("------------------------------------ rankLangsReduceByKey")
//    println(rankLangsReduceByKey(langsTest, wikiRdd10))
//    println("------------------------------------ timed")
    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)

    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
