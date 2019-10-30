package com.him.sample.kafka.streams;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Properties;

public class StreamsFilterTweets {

    public static void main(String[] args) {
        //create properties
        Properties properties= getProperties();
        //  create a topology
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic=streamsBuilder.stream("twitter_topic");

        //filter
        Predicate<String,String> predicate = (k,jsonValue)->extractIdFromTweets(jsonValue)>10000;
        KStream<String,String> filteredStream=inputTopic.filter(predicate);

        //outpot topic
        filteredStream.to("imp_topics");
        // start our streams application
        KafkaStreams streams=new KafkaStreams(streamsBuilder.build(),properties);
        streams.start();
    }

    private static Properties getProperties() {
        Properties properties= new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-stream");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());
        return properties;
    }

    private static Integer extractIdFromTweets(String tweetJson){
        JsonParser parser=new JsonParser();
        try {
            return parser.parse(tweetJson).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
        }catch(NullPointerException e){
            return 0;
        }
    }

}
