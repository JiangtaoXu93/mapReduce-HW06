name := "A6-Ankita-Jiangtao"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++={
  val sparkVer = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer
  )
}
        