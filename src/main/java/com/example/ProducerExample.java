package com.example;

import kafka.producer.ProducerConfig;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.util.Properties;

/**
 * Created by pchanumolu on 3/31/15.
 */

public class ProducerExample {

    public static void main(String[] args) {

        final String TOPIC = args[0];

        // define properties for how the Producer finds the cluster, serializes the messages and
        // if appropriate directs the message to a specific Partition
        Properties props = new Properties();

        //The first property, “metadata.broker.list” defines where the Producer
        // can find a one or more Brokers to determine the Leader for each topic
        // No need to worry about figuring out which Broker is the leader for the topic (and partition),
        // the Producer knows how to connect to the Broker and ask for the meta data then connect to the correct Broker
        props.put("metadata.broker.list", "localhost:9092,broker:9092");

        // The second property “serializer.class” defines what Serializer to use when preparing the message for
        // transmission to the Broker. In our example we use a simple String encoder provided as part of Kafka.
        // Note that the encoder must accept the same type as defined in the KeyedMessage object in the next step.
        // It is possible to change the Serializer for the Key (see below) of the message by defining
        // "key.serializer.class" appropriately. By default it is set to the same value as "serializer.class".
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        // The third property  "partitioner.class" defines what class to use to determine which Partition in the
        // Topic the message is to be sent to. This is optional, but for any non-trivial implementation you are going
        // to want to implement a partitioning scheme.
        props.put("partitioner.class", "example.producer.SimplePartitioner");

        // The last property "request.required.acks" tells Kafka that you want your Producer to require an
        // acknowledgement from the Broker that the message was received. Without this setting the Producer will
        // 'fire and forget' possibly leading to data loss
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        // define the Producer object itself
        // Producer is a Java Generic and you need to tell it the type of two parameters.
        // The first is the type of the Partition key, the second the type of the message.
        // In this example they are both Strings, which also matches to what we defined in the Properties above
        Producer<String,String> producer = new Producer<String, String>(config);


        // construct the message
        String ip = "10.10.100.188";
        // TOPIC is the Topic to write to. Here we are passing the IP as the partition key.
        // Note that if you do not include a key, even if you've defined a partitioner class,
        // Kafka will assign the message to a random partition.
        KeyedMessage<String,String> msg = new KeyedMessage<String, String>(TOPIC,ip,"Sample Message");

        // send the message
        producer.send(msg);

        // close the producer
        producer.close();

    }
}

