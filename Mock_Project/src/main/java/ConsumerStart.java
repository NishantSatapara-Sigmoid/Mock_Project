import java.io.IOException;

public class ConsumerStart{
    public static void main(String[] args) throws IOException{
        String topic = "Twitter";
        String Group_Id="My-Application";
        Kafka_Consumer Consumer = new Kafka_Consumer(Group_Id,topic);
        Consumer.run();
    }
}

