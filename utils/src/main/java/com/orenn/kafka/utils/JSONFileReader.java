package com.orenn.kafka.utils;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;

public class JSONFileReader {

    private JSONObject jsonObject;

    public JSONFileReader(String filePath) {

        JSONParser parser = new JSONParser();

        try {
            Object fileObject = parser.parse(new FileReader(filePath));
            this.jsonObject = (JSONObject) fileObject;
        } catch (Exception err) {
            err.printStackTrace();
        }
    }

    public JSONObject getJsonObject() {
        return jsonObject;
    }

}
