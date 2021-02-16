package com.orenn.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.orenn.kafka.utils.JSONFileReader;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private final JSONObject secretsMap = new JSONFileReader("/home/oren/projects/KafkaTwitter/.twitter_secrets.json").getJsonObject();

    private final String consumerKey = (String) secretsMap.get("consumerKey");
    private final String consumerSecret = (String) secretsMap.get("consumerSecret");
    private final String token = (String) secretsMap.get("token");
    private final String secret = (String) secretsMap.get("secret");
    private final List<String> terms = Lists.newArrayList("kafka");

    public TwitterProducer() {

    }

    public static void main(String[] args) {

        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.run();

    }

    public void run() {

        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        // create twitter client
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        // create kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application");
            logger.info("shutting down twitter client");
            twitterClient.stop();
            logger.info("closing kafka producer");
            kafkaProducer.close();
            logger.info("done");
        }));

        // loop to send tweets to kafka
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }

            if (msg != null) {
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Error occurred: ", e);
                        }
                    }
                });
            }
        }

        logger.info("End of Application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        endpoint.trackTerms(terms);

        Authentication oAuth1 = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Twitter-Client-01")
                .hosts(hosts)
                .authentication(oAuth1)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client twitterClient = builder.build();

        return twitterClient;

    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // properties for safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // basically this property is
                                                                                  // sufficient but for clarity you
                                                                                  // can/should explicitly set also
                                                                                  // below properties
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // properties for high throughput producer (trading off CPU usage (for compression) and latency for improved throughput)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // compression
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // how many ms producer should wait for sending
                                                                       // messages. outcome is batching messages together
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // max batch size in KB. 32KB here

        // create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        return kafkaProducer;
    }

}
