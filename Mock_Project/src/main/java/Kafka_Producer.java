import Config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;


public class Kafka_Producer {
    private final String topic;
    private final Properties properties;
    private final KafkaProducer<String,String> producer;

    public Kafka_Producer(String topic)
    {   this.topic=topic;
        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.Server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public void send(String mes)
    {
        ProducerRecord<String,String> record = new ProducerRecord<>(topic, mes);
        producer.send(record);
        producer.close();
    }
}
