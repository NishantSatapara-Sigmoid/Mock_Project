import Config.TwitterConfig;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Twitter_API {

    private final String topic;
    private final List<String> keywords;
    private final Logger logger = LoggerFactory.getLogger(Twitter_API.class.getName());

    public Twitter_API(String topic,List<String> keywords)
    {   this.topic = topic;
        this.keywords=keywords;
    }
    public void run()
    {
        logger.info("Start");
        LinkedBlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = createTwitterClient(msgQueue);
        client.connect();
         int f=1;
        while (!client.isDone()&&f==1) {
            String msg = null;
            try {
                msg = msgQueue.poll(5,TimeUnit.SECONDS);
            } catch (InterruptedException e){
                //e.printStackTrace();
                client.stop();
            }

            if(msg != null){
               Kafka_Producer producerDemo = new Kafka_Producer(topic);
               //ProducerRecord<String,String> record = new ProducerRecord<>(topic, msg);
               producerDemo.send(msg);
            }
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue)
    {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(keywords);
        Authentication hosebirdAuth = new OAuth1(TwitterConfig.CONSUMER_KEYS,TwitterConfig.CONSUMER_SECRETS,
                TwitterConfig.SECRET, TwitterConfig.TOKEN);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();
    }
}
