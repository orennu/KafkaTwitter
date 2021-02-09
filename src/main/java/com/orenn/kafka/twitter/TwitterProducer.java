package com.orenn.kafka.twitter;

import com.google.common.collect.Lists;
import com.orenn.kafka.utils.JSONFileReader;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private final JSONObject secretsMap = new JSONFileReader().getJsonObject();

    private final String consumerKey = (String) secretsMap.get("consumerKey");
    private final String consumerSecret = (String) secretsMap.get("consumerSecret");
    private final String token = (String) secretsMap.get("token");
    private final String secret = (String) secretsMap.get("secret");

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
            }
        }

        logger.info("End of Application");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("kafka");
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
}
