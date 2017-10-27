import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try
object main {
    def main(args: Array[String]): Unit = {
    def TRACK_ID = 0
    def ARTIST_ID = 16
    def ALBUM = 22
    def DURATION = 5
    def LOUDNESS = 6
    def SONG_HOT = 25
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
    val input = sc.textFile("MillionSongSubset/song_info.csv")
    val songInfo = input.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(line => line.split(";")).persist()

    val numberOfSongs = songInfo.filter(line => !line(TRACK_ID).isEmpty).map(line => line(TRACK_ID)).distinct().count()
    System.out.println("Number of distinct songs are: "+ numberOfSongs)

    val numberOfArtists = songInfo.filter(line => !line(ARTIST_ID).isEmpty).map(line => line(ARTIST_ID)).distinct().count()
    System.out.println("Number of distinct artists are: "+ numberOfArtists)

    val artistAlbumTuple = songInfo.filter(line => !line(ARTIST_ID).isEmpty && !line(ALBUM).isEmpty).map(line => (line(ARTIST_ID),line(ALBUM))).distinct()
    val numberOfAlbum = artistAlbumTuple.countByKey().foldLeft(0l)(_+_._2)
    System.out.println("Number of distinct albums are: "+ numberOfAlbum)

    val songLoudnessTuple = songInfo.filter(line => !line(TRACK_ID).isEmpty && Try(line(LOUDNESS).toFloat).isSuccess).map(line => (line(TRACK_ID),line(LOUDNESS).toFloat)).distinct()
    val top5LoudestSongs = songLoudnessTuple.sortBy(_._2).take(5)
    System.out.println("Top 5 Loudest songs are: "+ top5LoudestSongs.toList)

    val longSongTuple = songInfo.filter(line => !line(TRACK_ID).isEmpty && Try(line(DURATION).toFloat).isSuccess).map(line => (line(TRACK_ID),line(DURATION).toFloat)).distinct()
    val top5LongestSongs = longSongTuple.sortBy(_._2,false).take(5)
    System.out.println("Top 5 Longest songs are: "+ top5LongestSongs.toList)

    val fastSongTuple = songInfo.filter(line => !line(TRACK_ID).isEmpty && Try(line(TEMPO).toFloat).isSuccess).map(line => (line(TRACK_ID),line(TEMPO).toFloat)).distinct()
    val top5FastestSongs = fastSongTuple.sortBy(_._2,false).take(5)
    System.out.println("Top 5 Fastest songs are: "+ top5FastestSongs.toList)

    val artistFamiliarityTuple = songInfo.filter(line => !line(TRACK_ID).isEmpty && Try(line(FAMILIARITY).toFloat).isSuccess).map(line => (line(TRACK_ID),line(FAMILIARITY).toFloat)).distinct()
    val top5FamiliarArtist = artistFamiliarityTuple.sortBy(_._2,false).take(5)
    System.out.println("Top 5 most familiar artists are: "+ top5FamiliarArtist.toList)

    val songHotnessTuple = songInfo.filter(line => !line(TRACK_ID).isEmpty && Try(line(SONG_HOT).toFloat).isSuccess).map(line => (line(TRACK_ID),line(SONG_HOT).toFloat)).distinct()
    val top5HottestSongs = songHotnessTuple.sortBy(_._2,false).take(5)
    System.out.println("Top 5 most Hottest songs are: "+ top5HottestSongs.toList)

    val artistHotnessTuple = songInfo.filter(line => !line(ARTIST_ID).isEmpty && Try(line(ARTIST_HOT).toFloat).isSuccess).map(line => (line(ARTIST_ID),line(ARTIST_HOT).toFloat)).distinct().persist()
    val top5HottestArtist = artistHotnessTuple.sortBy(_._2,false).take(5)
    System.out.println("Top 5 most Hottest artists are: "+ top5HottestArtist.toList)

    val keyCountTuple = songInfo.filter(line => !line(KEY).isEmpty && Try(line(KEY_CONFIDENCE).toFloat).isSuccess && line(KEY_CONFIDENCE).toFloat > 0.7).map(line => (line(KEY), 1))
    val top5PopularKey = keyCountTuple.countByKey().toSeq.sortWith(_._2 > _._2).take(5)
    System.out.println("Top 5 most popular keys are: "+ top5PopularKey.toList)

    val artistSongTuple = songInfo.filter(line => !line(ARTIST_ID).isEmpty && !line(TRACK_ID).isEmpty).map(line => (line(ARTIST_ID), line(TRACK_ID))).distinct()
    val top5ProlificArtists = artistSongTuple.countByKey().toSeq.sortWith(_._2 > _._2).take(5)
    System.out.println("Top 5 most prolific artists are: "+ top5ProlificArtists.toList)

    val genreInput = sc.textFile("MillionSongSubset/artist_terms.csv")
    val termInfo = genreInput.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(line => line.split(",")).persist()
    val artistGenreTuple = termInfo.filter(line => !line(ARTIST_ID_IN_TERM).isEmpty&& !line(ARTIST_TERM).isEmpty)
      .map(line => (line(ARTIST_ID_IN_TERM),line(ARTIST_TERM)))
    val termHotnessInfo = artistGenreTuple.join(artistHotnessTuple).map{case (artist,(term, hot)) => (term,hot)}
    val hottestGenre =  termHotnessInfo.combineByKey((v) => (v, 1),(acc: (Float, Int), v) => (acc._1 + v, acc._2 + 1),(acc1:(Float, Int), acc2:(Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map{ case (key, value) => (key, value._1 / value._2.toFloat)}
    val top5HottestGenre = hottestGenre.sortBy(_._2,false).take(5)
    System.out.println("Top 5 most hottes genres are: "+ top5HottestGenre.toList)


    val ignored = Set(
      "THAT", "WITH", "THE", "AND", "TO", "OF",
      "A", "IT", "SHE", "HE", "YOU", "IN", "I",
      "HER", "AT", "AS", "ON", "THIS", "FOR",
      "BUT", "NOT", "OR")
    val words = songInfo.flatMap(line => line(SONG_TITLE).toUpperCase().split(" ")).filter { !_.isEmpty }
    val wordsCount = words.filter{ !ignored.contains(_) }.map(w => (w,1))
    val top5Words = wordsCount.countByKey().toSeq.sortWith(_._2 > _._2).take(5)
    System.out.println("Top 5 most common words in titles are: "+ top5Words.toList)


  }
}