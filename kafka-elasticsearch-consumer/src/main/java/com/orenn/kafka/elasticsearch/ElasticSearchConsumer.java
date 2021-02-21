package com.orenn.kafka.elasticsearch;

import com.google.gson.JsonParser;
import com.orenn.kafka.utils.JSONFileReader;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ElasticSearchConsumer {

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    private final JSONObject secretsMap = new JSONFileReader("/home/oren/projects/KafkaTwitter/.elasticsearch_secrets.json").getJsonObject();

    private final String hostname = (String) secretsMap.get("hostname");
    private final String username = (String) secretsMap.get("username");
    private final String password = (String) secretsMap.get("password");

    public ElasticSearchConsumer() {

    }

    public RestHighLevelClient createElasticSearchClient() {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient elasticSearchClient = new RestHighLevelClient(restClientBuilder);

        return elasticSearchClient;
    }

    public KafkaConsumer<String, String> createKafkaConsumer(String topic) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "elasticsearch-app-group-1";

        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        List<String> topicList = new ArrayList<>();
        topicList.add(topic);

        // create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(topicList);

        return kafkaConsumer;
    }

    public static void main(String[] args) throws IOException {

        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer();
        RestHighLevelClient elasticSearchClient = elasticSearchConsumer.createElasticSearchClient();

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = elasticSearchConsumer.createKafkaConsumer("twitter_tweets");

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            Integer recordCount = records.count();
            logger.info(String.format("Received %s records", recordCount));

            BulkRequest bulkRequest = new BulkRequest(); // create BulkRequest for batching messages

            for (ConsumerRecord<String, String> record : records) {
                try {
                    // insert data into elasticsearch
                    // strategies for creating unique ID
                    // 1. Kafka generic ID
                    // String id = String.format("%s_%s_%s", record.topic(), record.partition(), record.offset());
                    // 2. Twitter specific ID --> going with this approach for this project
                    String id = extractIdFromTweet(record.value());

                    IndexRequest indexRequest = new IndexRequest("twitter");
                    indexRequest.id(id);
                    indexRequest.source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest); // add record to bulk request
                } catch (NullPointerException err) {
                    logger.warn(String.format("skipping bad data: %s", record.value()));
                }
            }

            if (recordCount > 0) {
                BulkResponse bulkResponse = elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT); // sending batch of messages in bulk

                logger.info("Committing offsets...");
                kafkaConsumer.commitAsync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // close client gracefully
        //elasticSearchClient.close();

    }

    private static String extractIdFromTweet(String tweetJson) {

        return JsonParser.parseString(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
}
