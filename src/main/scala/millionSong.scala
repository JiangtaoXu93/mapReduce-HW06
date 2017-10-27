import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

/**
  * @author Ankita,Jiangtao
  */

object millionSong {
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

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Million Music")
    val sc = new SparkContext(conf)

    val input = sc.textFile("input/song_info.csv")
    val songInfo = input.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(line => line.split(";")).persist()

    val genreInput = sc.textFile("input/artist_terms.csv")
    val termInfo = genreInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(line => line.split(",")).persist()

    val artistHotnessTuple = songInfo.filter(line => !line(ARTIST_ID).isEmpty && Try(line(ARTIST_HOT).toFloat).isSuccess).map(line => (line(ARTIST_ID),line(ARTIST_HOT).toFloat)).distinct().persist()

    getDistinctSongs(sc, songInfo)
    getDistinctArtists(sc, songInfo)
    getDistinctAlbum(sc, songInfo)
    getTop5LoudestSongs(sc, songInfo)
    getTop5LongestSongs(sc, songInfo)
    getTop5FastestSongs(sc, songInfo)
    getTop5FamiliarArtist(sc, songInfo)
    getTop5HottestSongs(sc, songInfo)
    getTop5HottestArtist(sc, artistHotnessTuple)
    getTop5PopularKeys(sc, songInfo)
    getTop5ProlificArtist(sc, songInfo)
    getTop5HottestGenre(sc, termInfo, artistHotnessTuple)
    getCommonWords(sc, songInfo)

  }

  /**
    * The getDistinctSongs function gets the number of distinct track-id's in the dataset.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getDistinctSongs(sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val numberOfSongs = songInfo.filter(line => !line(TRACK_ID).isEmpty).map(line => line(TRACK_ID)).distinct().count()
    val distinctSongOutput = sc.parallelize(List(numberOfSongs))
    distinctSongOutput.saveAsTextFile("output/distinctSongs")
  }

  /**
    * The getDistinctArtists function gets the number of distinct artist-id's in the dataset.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getDistinctArtists(sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val numberOfArtists = songInfo.filter(line => !line(ARTIST_ID).isEmpty).map(line => line(ARTIST_ID)).distinct().count()
    val distinctArtistOutput = sc.parallelize(List(numberOfArtists))
    distinctArtistOutput.saveAsTextFile("output/distinctArtist")
  }

  /**
    * The getDistinctAlbum function gets the number of distinct albums in the dataset.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getDistinctAlbum( sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val artistAlbumTuple = songInfo.filter(line => !line(ARTIST_ID).isEmpty && !line(ALBUM).isEmpty).map(line => (line(ARTIST_ID), line(ALBUM))).distinct()
    val numberOfAlbum = artistAlbumTuple.countByKey().foldLeft(0l)(_ + _._2)
    val distinctAlbumOutput = sc.parallelize(List(numberOfAlbum))
    distinctAlbumOutput.saveAsTextFile("output/distinctAlbum")
  }

  /**
    * The getTop5LoudestSongs function gets the top 5 loudest songs from the dataset by sorting (TrackId,Loudness) on loudness
    * and picks the top 5 from the list.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getTop5LoudestSongs(sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val songLoudnessTuple = songInfo.filter(line => !line(TRACK_ID).isEmpty && Try(line(LOUDNESS).toFloat).isSuccess).map(line => (line(TRACK_ID), line(LOUDNESS).toFloat)).distinct()
    val top5LoudestSongs = songLoudnessTuple.sortBy(_._2,false).take(5)
    val loudestSongOutput = sc.parallelize(top5LoudestSongs)
    loudestSongOutput.saveAsTextFile("output/top5LoudestSongs")
  }

  /**
    * The getTop5LongestSongs function gets the top 5 longest songs from the dataset by sorting (TrackId,Duration) on duration
    * and picks the top 5 from the list.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getTop5LongestSongs(sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val longSongTuple = songInfo.filter(line => !line(TRACK_ID).isEmpty && Try(line(DURATION).toFloat).isSuccess).map(line => (line(TRACK_ID), line(DURATION).toFloat)).distinct()
    val top5LongestSongs = longSongTuple.sortBy(_._2, false).take(5)
    val longestSongOutput = sc.parallelize(top5LongestSongs)
    longestSongOutput.saveAsTextFile("output/top5LongestSongs")
  }

  /**
    * The getTop5FastestSongs function gets the top 5 fastest songs from the dataset by sorting (TrackId,Tempo) on tempo
    * and picks the top 5 from the list.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getTop5FastestSongs( sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val fastSongTuple = songInfo.filter(line => !line(TRACK_ID).isEmpty && Try(line(TEMPO).toFloat).isSuccess).map(line => (line(TRACK_ID), line(TEMPO).toFloat)).distinct()
    val top5FastestSongs = fastSongTuple.sortBy(_._2, false).take(5)
    val fastestSongOutput = sc.parallelize(top5FastestSongs)
    fastestSongOutput.saveAsTextFile("output/top5FastestSongs")
  }

  /**
    * The getTop5FamiliarArtist function gets the top 5 familiar songs from the dataset by sorting (ArtistId,Familiarity) on Familiarity
    * and picks the top 5 from the list.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getTop5FamiliarArtist(sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val artistFamiliarityTuple = songInfo.filter(line => !line(ARTIST_ID).isEmpty && Try(line(FAMILIARITY).toFloat).isSuccess).map(line => (line(ARTIST_ID), line(FAMILIARITY).toFloat)).distinct()
    val top5FamiliarArtist = artistFamiliarityTuple.sortBy(_._2, false).take(5)
    val familiaritySongOutput = sc.parallelize(top5FamiliarArtist)
    familiaritySongOutput.saveAsTextFile("output/top5FamiliarArtist")
  }

  /**
    * The getTop5PopularKeys function gets the top 5 popular keys from the dataset by doing counByKey on Keys and sort on Key_Confidence
    * and picks the top 5 from the list.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getTop5PopularKeys(sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val keyCountTuple = songInfo.filter(line => !line(KEY).isEmpty && Try(line(KEY_CONFIDENCE).toFloat).isSuccess && line(KEY_CONFIDENCE).toFloat > 0.7).map(line => (line(KEY), 1))
    val top5PopularKey = keyCountTuple.countByKey().toSeq.sortWith(_._2 > _._2).take(5)
    val popularSongOutput = sc.parallelize(top5PopularKey)
    popularSongOutput.saveAsTextFile("output/top5PopularKeys")
  }

  /**
    * The getTop5ProlificArtist function gets the top 5 prolific artists from the data set by doing countByKey on ArtistId and sort on TrackId's
    * to get the most number of tracks for each artist and thus pick the top 5 from the list.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getTop5ProlificArtist( sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val artistSongTuple = songInfo.filter(line => !line(ARTIST_ID).isEmpty && !line(TRACK_ID).isEmpty).map(line => (line(ARTIST_ID), line(TRACK_ID))).distinct()
    val top5ProlificArtists = artistSongTuple.countByKey().toSeq.sortWith(_._2 > _._2).take(5)
    val prolificArtistOutput = sc.parallelize(top5ProlificArtists)
    prolificArtistOutput.saveAsTextFile("output/top5ProlificArtist")
  }

  /**
    * The getTop5HottestArtist function gets the top 5 hottest artists from the dataset by doing sort on artist_hotness
    * and picks the top 5 from the list.
    * @param sc         The SparkContext object for this job.
    * @param artistHotnessTuple   The artistHotnessTuple RDD having the artistId and artist_hotness
    */
  private def getTop5HottestArtist(sc: SparkContext, artistHotnessTuple: RDD[(String, Float)]) = {
    val top5HottestArtist = artistHotnessTuple.sortBy(_._2,false).take(5)
    val hottestArtistOutput = sc.parallelize(top5HottestArtist)
    hottestArtistOutput.saveAsTextFile("output/top5HottestArtist")
  }

  /**
    * The getTop5HottestSongs function gets the top 5 hottest songs from the dataset by grouping on
    * (TrackId,SongHotness) and doing sort on songs_hotness thus picks the top 5 from the list.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getTop5HottestSongs(sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val songHotnessTuple = songInfo.filter(line => !line(TRACK_ID).isEmpty && Try(line(SONG_HOTNESS).toFloat).isSuccess).map(line => (line(TRACK_ID), line(SONG_HOTNESS).toFloat)).distinct()
    val top5HottestSongs = songHotnessTuple.sortBy(_._2, false).take(5)
    val hottestSongOutput = sc.parallelize(top5HottestSongs)
    hottestSongOutput.saveAsTextFile("output/top5HottestSongs")
  }

  /**
    * The getTop5HottestGenre function gets the top 5 hottest genre from the dataset by joining
    * (ArtistId, ArtistTerm) and (ArtistId,ArtistHotness) and does a combineByKey to get the mean artist_hotness
    * and sorts on the mean to pick the top 5 genre.
    * @param sc         The SparkContext object for this job.
    * @param termInfo   The artist_terms RDD without the header row and each line split by ;
    * @param artistHotnessTuple   The artistHotnessTuple RDD having the artistId and artist_hotness
    */
  private def getTop5HottestGenre(sc: SparkContext, termInfo: RDD[Array[String]], artistHotnessTuple: RDD[(String, Float)]) = {
    val artistGenreTuple = termInfo.filter(line => !line(ARTIST_ID_IN_TERM).isEmpty && !line(ARTIST_TERM).isEmpty)
      .map(line => (line(ARTIST_ID_IN_TERM), line(ARTIST_TERM)))
    val termHotnessInfo = artistGenreTuple.join(artistHotnessTuple).map { case (artist, (term, hot)) => (term, hot) }
    val hottestGenre = termHotnessInfo.combineByKey((v) => (v, 1), (acc: (Float, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map { case (key, value) => (key, value._1 / value._2.toFloat) }
    val top5HottestGenre = hottestGenre.sortBy(_._2, false).take(5)
    val hottestGenreOutput = sc.parallelize(top5HottestGenre)
    hottestGenreOutput.saveAsTextFile("output/top5HottestGenre")
  }

  /**
    * The getCommonWords function gets the top 5 common words from the dataset by counting the frquency of each
    * word in Song_Tile column and thus picking the top 5 from the list.
    * @param sc         The SparkContext object for this job.
    * @param songInfo   The songInfo RDD without the header row and each line split by ;
    */
  private def getCommonWords(sc: SparkContext, songInfo: RDD[Array[String]]) = {
    val ignored = Set(
      "THAT", "WITH", "THE", "AND", "TO", "OF",
      "A", "IT", "SHE", "HE", "YOU", "IN", "I",
      "HER", "AT", "AS", "ON", "THIS", "FOR",
      "BUT", "NOT", "OR")
    val words = songInfo.flatMap(line => line(SONG_TITLE).filter(c => c.isLetter || c.isWhitespace).toUpperCase().split(" ")).filter {!_.isEmpty}
    val wordsCount = words.filter {!ignored.contains(_)}.map(w => (w, 1))
    val top5Words = wordsCount.countByKey().toSeq.sortWith(_._2 > _._2).take(5)
    val commonWordsOutput = sc.parallelize(top5Words)
    commonWordsOutput.saveAsTextFile("output/top5CommonWords")
  }


}
