# Flink

This directory contains a docker-compose.yml file containing Flink.
Flink can be downloaded from : [https://www.apache.org/dyn/closer.lua/flink/flink-1.9.1/flink-1.9.1-bin-scala_2.11.tgz](flink.apache.org)
Version build with scala 2.12 does not support Flink REPL.

You can also run this command line to have a Flink REPL within a docker container :

```
$> docker run -it --rm  -v "$PWD":/usr/src/app -w /usr/src/app flink:1.9.1-scala_2.11 start-scala-shell.sh local
```

# Try it

## Word count

### Using the DataSet API :

```
val data = benv.readTextFile("/usr/src/app/data/lorem-ipsum.txt")

data
  .filter(_.nonEmpty)
  .flatMap(_.split(" "))
  .map((_, 1))
  .groupBy(0)
  .sum(1)
  .print

```

### Using the DataStream API :

```
val data = senv.readTextFile("/usr/src/app/data/lorem-ipsum.txt")

data
  .filter(_.nonEmpty)
  .flatMap(_.split(" "))
  .map((_, 1))
  .keyBy(0)
  .sum(1)
  .print

senv.execute("Wordcount using DataStream API")

```

### Using SQL :

```
import org.apache.flink.table.functions.TableFunction // Needed to create our UDF

val source = senv.readTextFile("/usr/src/app/data/lorem-ipsum.txt")

class $SplitUdtf extends TableFunction[String] {
    def eval(s: String): Unit = {
      s.toLowerCase.split("\\W+").foreach(collect)
    }
  }

stenv.registerFunction("SplitUdtf", new $SplitUdtf)
stenv.registerDataStream("Words", source);

val result = stenv.sqlQuery("""SELECT T.word, COUNT(1)
    FROM Words, LATERAL TABLE(SplitUdtf(f0)) AS T(word)
    GROUP BY T.word""")

result.toRetractStream[(String, Long)].print

stenv.execute("Wordcount using SQL")
```
