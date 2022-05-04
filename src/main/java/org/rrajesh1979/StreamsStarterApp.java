package org.rrajesh1979;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class StreamsStarterApp {
    public static void main(String[] args) {
        log.info("StreamsStarterApp.main()");

        //Kafka Streams configuration
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/2");

        //1 - Create a Stream from Kafka
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        //2 - Transform the Stream - map the words to lower case
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))    //3 - Transform the Stream - flatMap the words to produce a stream of words //split the words \W+
                .selectKey((key, word) -> word)                                 //4 - Transform the Stream - select key to apply a key (discard the old key)
                .groupByKey()                                                     //5 - Transform the Stream - group by key
                .count(Materialized.as("Counts"));                                 //6 - Transform the Stream - count the words

        //7 - Transform the Stream - print the results, write to a topic
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        //8 - Create a Kafka Streams instance and start it
        try (KafkaStreams streams = new KafkaStreams(builder.build(), config)) {
            //9 - Start the Kafka Streams instance
            streams.start();
            log.info("StreamsStarterApp.main() - Kafka Streams started");

            log.info("Streams topology is {}", streams.toString());

            //10 - Shutdown hook to correctly close the Kafka Streams instance
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

            //11 - Wait for the application to be shut down
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
