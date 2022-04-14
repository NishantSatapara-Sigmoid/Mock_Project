import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.StringDeserializer;
import Config.KafkaConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.util.HashMap;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Kafka_Consumer {

    private final Properties properties;
    private final String topic;
    private final KafkaConsumer <String ,String> Consumer;

    public Kafka_Consumer(String groupId, String topic)
    {   this.topic=topic;
        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaConfig.Server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        Consumer= new KafkaConsumer<>(properties);
        Consumer.subscribe(Arrays.asList(topic));
    }

    private MongoCollection<Document> sink()
    {
        MongoClientURI uri = new MongoClientURI(
                "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false");
        MongoClient mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase("TwitterDB");
        MongoCollection<Document> collection = database.getCollection("Tweets");
        System.out.println("Collection myCollection selected successfully");
        return collection;
    }
    void run()
    {  Logger logger= LoggerFactory.getLogger(Kafka_Consumer.class.getName());

       MongoCollection<Document> collection= sink();
        while (true) {
            ConsumerRecords<String, String> records = Consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                Document doc = Document.parse(record.value());
                collection.insertOne(doc);
            }
            try {
                Consumer.commitSync();
            } catch (CommitFailedException e) {
               logger.info("commit failed", e);
            }
        }
    }
}
