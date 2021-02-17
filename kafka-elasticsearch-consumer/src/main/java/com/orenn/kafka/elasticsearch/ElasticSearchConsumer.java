package com.orenn.kafka.elasticsearch;

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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
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
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

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

            for (ConsumerRecord<String, String> record : records) {
                // insert data into elasticsearch
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(record.value(), XContentType.JSON);
                IndexResponse indexResponse = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info(id);
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
}
