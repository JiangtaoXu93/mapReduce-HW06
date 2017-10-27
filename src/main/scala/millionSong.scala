import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

object millionSong {

  def main(args: Array[String]): Unit = {
    def TRACK_ID = 0
    def ARTIST_ID = 16
    def ALBUM = 22
    def DURATION = 5
    def LOUDNESS = 6
    def SONG_HOTNESS = 25
    def ARTIST_HOT = 20
    def TEMPO = 7
    def FAMILIARITY = 19
    def KEY = 8
    def KEY_CONFIDENCE = 9
    def SONG_TITLE = 24
    def ARTIST_ID_IN_TERM = 0
    def ARTIST_TERM = 1

    val conf = new SparkConf().setMaster("local").setAppName("Million Music")
    val sc = new SparkContext(conf)

    val input = sc.textFile("input/song_info.csv")
    val songInfo = input.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(line => line.split(";")).persist()

    val genreInput = sc.textFile("input/artist_terms.csv")
    val termInfo = genreInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(line => line.split(";")).persist()

    val artistHotnessTuple = songInfo.filter(line => !line(ARTIST_ID).isEmpty && Try(line(ARTIST_HOT).toFloat).isSuccess).map(line => (line(ARTIST_ID),line(ARTIST_HOT).toFloat)).distinct().persist()

    getDistinctSongs(TRACK_ID _, sc, songInfo)
    getDistinctArtists(ARTIST_ID _, sc, songInfo)
    getDistinctAlbum(ARTIST_ID _, ALBUM _, sc, songInfo)
    getTop5LoudestSongs(TRACK_ID _, sc, songInfo, LOUDNESS)
    getTop5LongestSongs(TRACK_ID _, sc, songInfo, DURATION)
    getTop5FastestSongs(TRACK_ID _, sc, songInfo, TEMPO)
    getTop5FamiliarArtist(ARTIST_ID _, sc, songInfo, FAMILIARITY)
    getTop5HottestSongs(TRACK_ID _, sc, songInfo, SONG_HOTNESS)
    getTop5HottestArtist(ARTIST_ID _, sc, artistHotnessTuple)
    getTop5PopularKeys(KEY _, sc, songInfo, KEY_CONFIDENCE)
    getTop5ProlificArtist(TRACK_ID _, ARTIST_ID _, sc, songInfo)
    getTop5HottestGenre(ARTIST_ID_IN_TERM _, ARTIST_TERM _, sc, termInfo, artistHotnessTuple)
    getCommonWords(sc, songInfo, SONG_TITLE)

  }

  private def getDistinctSongs(TRACK_ID: () => Int, sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val numberOfSongs = songInfo.filter(line => !line(TRACK_ID()).isEmpty).map(line => line(TRACK_ID())).distinct().count()
    val distinctSongOutput = sc.parallelize(List(numberOfSongs))
    distinctSongOutput.saveAsTextFile("output/distinctSongs")
  }

  private def getDistinctArtists(ARTIST_ID: () => Int, sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val numberOfArtists = songInfo.filter(line => !line(ARTIST_ID()).isEmpty).map(line => line(ARTIST_ID())).distinct().count()
    val distinctArtistOutput = sc.parallelize(List(numberOfArtists))
    distinctArtistOutput.saveAsTextFile("output/distinctArtist")
  }

  private def getDistinctAlbum(ARTIST_ID: () => Int, ALBUM: () => Int, sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val artistAlbumTuple = songInfo.filter(line => !line(ARTIST_ID()).isEmpty && !line(ALBUM()).isEmpty).map(line => (line(ARTIST_ID()), line(ALBUM()))).distinct()
    val numberOfAlbum = artistAlbumTuple.countByKey().foldLeft(0l)(_ + _._2)
    val distinctAlbumOutput = sc.parallelize(List(numberOfAlbum))
    distinctAlbumOutput.saveAsTextFile("output/distinctAlbum")
    //    System.out.println("Number of distinct albums are: "+ numberOfAlbum)
  }

  private def getTop5LoudestSongs(TRACK_ID: () => Int, sc: SparkContext, songInfo: RDD[Array[String]], LOUDNESS:Int) = {
    val songLoudnessTuple = songInfo.filter(line => !line(TRACK_ID()).isEmpty && Try(line(LOUDNESS).toFloat).isSuccess).map(line => (line(TRACK_ID()), line(LOUDNESS).toFloat)).distinct()
    val top5LoudestSongs = songLoudnessTuple.sortBy(_._2,false).take(5)
    val loudestSongOutput = sc.parallelize(top5LoudestSongs)
    loudestSongOutput.saveAsTextFile("output/top5LoudestSongs")
  }

  private def getTop5LongestSongs(TRACK_ID: () => Int, sc: SparkContext, songInfo: RDD[Array[String]], DURATION:Int) = {
    val longSongTuple = songInfo.filter(line => !line(TRACK_ID()).isEmpty && Try(line(DURATION).toFloat).isSuccess).map(line => (line(TRACK_ID()), line(DURATION).toFloat)).distinct()
    val top5LongestSongs = longSongTuple.sortBy(_._2, false).take(5)
    val longestSongOutput = sc.parallelize(top5LongestSongs)
    longestSongOutput.saveAsTextFile("output/top5LongestSongs")
  }

  private def getTop5FastestSongs(TRACK_ID: () => Int, sc: SparkContext, songInfo: RDD[Array[String]], TEMPO:Int) = {
    val fastSongTuple = songInfo.filter(line => !line(TRACK_ID()).isEmpty && Try(line(TEMPO).toFloat).isSuccess).map(line => (line(TRACK_ID()), line(TEMPO).toFloat)).distinct()
    val top5FastestSongs = fastSongTuple.sortBy(_._2, false).take(5)
    val fastestSongOutput = sc.parallelize(top5FastestSongs)
    fastestSongOutput.saveAsTextFile("output/top5FastestSongs")
  }

  private def getTop5FamiliarArtist(ARTIST_ID: () => Int, sc: SparkContext, songInfo: RDD[Array[String]], FAMILIARITY:Int) = {
    val artistFamiliarityTuple = songInfo.filter(line => !line(ARTIST_ID()).isEmpty && Try(line(FAMILIARITY).toFloat).isSuccess).map(line => (line(ARTIST_ID()), line(FAMILIARITY).toFloat)).distinct()
    val top5FamiliarArtist = artistFamiliarityTuple.sortBy(_._2, false).take(5)
    val familiaritySongOutput = sc.parallelize(top5FamiliarArtist)
    familiaritySongOutput.saveAsTextFile("output/top5FamiliaritySongs")
  }

  private def getTop5PopularKeys(KEY: () => Int, sc: SparkContext, songInfo: RDD[Array[String]], KEY_CONFIDENCE:Int) = {
    val keyCountTuple = songInfo.filter(line => !line(KEY()).isEmpty && Try(line(KEY_CONFIDENCE).toFloat).isSuccess && line(KEY_CONFIDENCE).toFloat > 0.7).map(line => (line(KEY()), 1))
    val top5PopularKey = keyCountTuple.countByKey().toSeq.sortWith(_._2 > _._2).take(5)
    val popularSongOutput = sc.parallelize(top5PopularKey)
    popularSongOutput.saveAsTextFile("output/top5PopularSongs")
  }

  private def getTop5ProlificArtist(TRACK_ID: () => Int, ARTIST_ID: () => Int, sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val artistSongTuple = songInfo.filter(line => !line(ARTIST_ID()).isEmpty && !line(TRACK_ID()).isEmpty).map(line => (line(ARTIST_ID()), line(TRACK_ID()))).distinct()
    val top5ProlificArtists = artistSongTuple.countByKey().toSeq.sortWith(_._2 > _._2).take(5)
    val prolificArtistOutput = sc.parallelize(top5ProlificArtists)
    prolificArtistOutput.saveAsTextFile("output/top5ProlificArtist")
  }

  private def getTop5HottestArtist(ARTIST_ID: () => Int, sc: SparkContext, artistHotnessTuple: RDD[(String, Float)]) = {
    val top5HottestArtist = artistHotnessTuple.sortBy(_._2,false).take(5)
    val hottestArtistOutput = sc.parallelize(top5HottestArtist)
    hottestArtistOutput.saveAsTextFile("output/top5HottestArtist")
  }

  private def getTop5HottestSongs(TRACK_ID: () => Int, sc: SparkContext, songInfo: RDD[Array[String]], SONG_HOTNESS:Int) = {
    val songHotnessTuple = songInfo.filter(line => !line(TRACK_ID()).isEmpty && Try(line(SONG_HOTNESS).toFloat).isSuccess).map(line => (line(TRACK_ID()), line(SONG_HOTNESS).toFloat)).distinct()
    val top5HottestSongs = songHotnessTuple.sortBy(_._2, false).take(5)
    val hottestSongOutput = sc.parallelize(top5HottestSongs)
    hottestSongOutput.saveAsTextFile("output/top5HottestSongs")
  }

  private def getTop5HottestGenre(ARTIST_ID_IN_TERM: () => Int, ARTIST_TERM: () => Int, sc: SparkContext, termInfo: RDD[Array[String]], artistHotnessTuple: RDD[(String, Float)]) = {
    val artistGenreTuple = termInfo.filter(line => !line(ARTIST_ID_IN_TERM()).isEmpty && !line(ARTIST_TERM()).isEmpty)
      .map(line => (line(ARTIST_ID_IN_TERM()), line(ARTIST_TERM())))
    val termHotnessInfo = artistGenreTuple.join(artistHotnessTuple).map { case (artist, (term, hot)) => (term, hot) }
    val hottestGenre = termHotnessInfo.combineByKey((v) => (v, 1), (acc: (Float, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map { case (key, value) => (key, value._1 / value._2.toFloat) }
    val top5HottestGenre = hottestGenre.sortBy(_._2, false).take(5)
    val hottestGenreOutput = sc.parallelize(top5HottestGenre)
    hottestGenreOutput.saveAsTextFile("output/top5HottestGenre")
  }

  private def getCommonWords(sc: SparkContext, songInfo: RDD[Array[String]], SONG_TITLE:Int) = {
    val ignored = Set(
      "THAT", "WITH", "THE", "AND", "TO", "OF",
      "A", "IT", "SHE", "HE", "YOU", "IN", "I",
      "HER", "AT", "AS", "ON", "THIS", "FOR",
      "BUT", "NOT", "OR")
    val words = songInfo.flatMap(line => line(SONG_TITLE).toUpperCase().split(" ")).filter {!_.isEmpty}
    val wordsCount = words.filter {!ignored.contains(_)}.map(w => (w, 1))
    val top5Words = wordsCount.countByKey().toSeq.sortWith(_._2 > _._2).take(5)
    val commonWordsOutput = sc.parallelize(top5Words)
    commonWordsOutput.saveAsTextFile("output/top5CommonWords")
  }

}
