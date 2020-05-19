package org.myorg.quickstart


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import java.util.Properties
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer




object StreamingJob {
  //The Condition for anomaly detection
  var condition= 0.1
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //StreamExecutionEnvironment.setStreamTimeCharacteristic()
    val p = new Properties()
    p.setProperty("bootstrap.servers", "localhost:9092")
    p.setProperty("group.id", "test")

    //Reading from Kafka source

    val Impression = env.addSource(new FlinkKafkaConsumer[String]("displays", new SimpleStringSchema(), p))
    val click = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), p))
        //.print()



    // Convert function
    def idExtraction(l: Array[String]): (String, Long, String) = {
      val ip = l(3).split(":")(1)
      val uid = l(1).split(":")(1)
      val timestamp = l(2).split(":")(1).toLong * 1000
      return (uid, timestamp, ip)
    }

    // Find Users who use the same ip.
    val ipClick = click.map(_.split(","))
      .map(x => (idExtraction(x), 1.0)).assignAscendingTimestamps {
      case ((_, timestamp, _), _) => timestamp
    }
      .keyBy { elem =>
        elem match {
          case ((_, _, ip), _) => ip
        }
      }
      .timeWindow(Time.seconds(10))
      .sum(1)


    val ipImpression = Impression.map(_.split(","))
      .map(x => (idExtraction(x), 1.0)).assignAscendingTimestamps {
      case ((_, timestamp, _), _) => timestamp
    }
      .keyBy { elem =>
        elem match {
          case ((_, _, ip), _) => ip
        }
      }
      .timeWindow(Time.seconds(10))
      .sum(1)

    //Anomaly Extraction
    var JoinDataStreams = ipImpression.join(ipClick).where(x => x._1._3).equalTo(x => x._1._3)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply((a, b) => (b._1._3, b._2 / a._2))
      .filter(x => x._2 > condition)
      .print()



    //Execution
    env.execute("Flink Kafka")
  }
}
