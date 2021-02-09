package com.orenn.kafka.utils;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileNotFoundException;
import java.io.FileReader;

public class JSONFileReader {

    private JSONObject jsonObject;

    public JSONFileReader() {

        JSONParser parser = new JSONParser();

        try {
            Object fileObject = parser.parse(new FileReader("/home/oren/projects/KafkaTwitter/.twitter_secrets.json"));
            this.jsonObject = (JSONObject) fileObject;
        } catch (Exception err) {
            err.printStackTrace();
        }
    }

    public JSONObject getJsonObject() {
        return jsonObject;
    }

//    public static void main(String[] args) {
//
//        JSONParser parser = new JSONParser();
//
//        try {
//            Object fileObject = parser.parse(new FileReader("/home/oren/projects/KafkaTwitter/.twitter_secrets.json"));
//            JSONObject jsonObject = (JSONObject) fileObject;
//
//            String consumerKey = (String) jsonObject.get("consumerKey");
//            String consumerSecret = (String) jsonObject.get("consumerSecret");
//            String token = (String) jsonObject.get("token");
//            String secret = (String) jsonObject.get("secret");
//
//            System.out.println(consumerKey);
//            System.out.println(consumerSecret);
//            System.out.println(token);
//            System.out.println(secret);
//
//        } catch (Exception err) {
//            err.printStackTrace();
//        }
//    }
}
