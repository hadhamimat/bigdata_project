package spark;

/**
 * Created by Hadhami on 10/03/2016.
 */

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class SparkConsumer {

    static public String brokerList = "ambari:6667,slave1:6667";

    public static void main(String[] args){
        //create spark config
        SparkConf sparkConf = new SparkConf().setAppName("kafkaStreaming");
        sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaStreaming");
        //crete spark context for each batch
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //create spark streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(1));

        //read data from kafka topic to direct stream ---->  RDD group
        HashSet<String> topicsSet = new HashSet<String>();
        topicsSet.add("topic_edf");
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",brokerList);
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,StringDecoder.class, kafkaParams, topicsSet);

        //print result
        System.out.println("xxxxxxxxxx RESULT xxxxxxx");
        kafkaStream.print();
        System.out.println("xxxxxxxxxx RESULT xxxxxxx");


        jssc.start();
        jssc.awaitTermination();

    }
}

