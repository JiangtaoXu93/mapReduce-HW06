import org.apache.spark.{SparkConf, SparkContext}

object main {
  def main(args: Array[String]): Unit ={
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Million Music")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val inputSongsInfo = sc.textFile("input/song_info.csv")
    val header = inputSongsInfo.first()
    val songInfoData = inputSongsInfo.filter(row => row != header)

    val numberOfSongs = songInfoData.map(line => line.split(";")(23)).filter { !_.isEmpty }.distinct().count
    System.out.println("Number of distinct songs are: "+ numberOfSongs)

    val numberOfArtists = songInfoData.map(line => line.split(";")(16)).filter { !_.isEmpty }.distinct().count()
    System.out.println("Number of distinct artists are: "+ numberOfArtists)

    val songLoudnessTuple = songInfoData.map(line => ((line.split(";")(23)),(line.split(";")(6).toFloat)))
    val top5LoudestSongs = songLoudnessTuple.sortBy(_._2).take(5)
    System.out.println("Top 5 Loudest songs are: "+ top5LoudestSongs.toList)

    val longSongTuple = songInfoData.map(line => ((line.split(";")(23)),(line.split(";")(5).toFloat)))
    val top5LongestSongs = longSongTuple.sortBy(_._2,false).take(5)
    System.out.println("Top 5 Longest songs are: "+ top5LongestSongs.toList)

    val fastSongTuple = songInfoData.map(line => ((line.split(";")(23)),(line.split(";")(7).toFloat)))
    val top5FastestSongs = fastSongTuple.sortBy(_._2,false).take(5)
    System.out.println("Top 5 Fastest songs are: "+ top5FastestSongs.toList)

    val artistFamiliarityTuple = songInfoData.map(line => ((line.split(";")(16)),(line.split(";")(19).toFloat)))
    val top5FamiliarArtist = artistFamiliarityTuple.sortBy(_._2,false).take(5)
    System.out.println("Top 5 most familiar artists are: "+ top5FamiliarArtist.toList)

    val songHottnessTuple = songInfoData.map(line => ((line.split(";")(23)),(line.split(";")(25).toFloat)))
    val top5HottestSongs = songHottnessTuple.sortBy(_._2,false).take(5)
    System.out.println("Top 5 most Hottest songs are: "+ top5HottestSongs.toList)

    val artistHottnessTuple = songInfoData.map(line => ((line.split(";")(16)),(line.split(";")(20).toFloat)))
    val top5HottestArtist = artistHottnessTuple.sortBy(_._2,false).take(5)
    System.out.println("Top 5 most Hottest artists are: "+ top5HottestArtist.toList)




  }
}