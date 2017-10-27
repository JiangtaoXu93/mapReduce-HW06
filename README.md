# a6-ankita-jiangtao

http://janvitek.org/pdpmr/f17/task-a6-high-fidelity.html

## Running Instructions

### Local

#### Prepare

1) Make sure your enviroment have installed scala, scala should have the same version as Spark build-in scala.
2) Change `HADOOP_HOME`, `SPARK_HOME`,`SCALA_HOME`, `HADOOP_VERSION` to be same as your environment.
3) Put your test data into `input` directory.
4) If you are planning to run big-corpus song dataset, use `line => line.split(";")` in 
millionSong.scala: line 34
```
    val termInfo = genreInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(line => line.split(",")).persist()

```
Otherwise, use `line => line.split(",")` for the small-corpus song dataset

#### Run

- Goto `<project-root>`
- Run `make`