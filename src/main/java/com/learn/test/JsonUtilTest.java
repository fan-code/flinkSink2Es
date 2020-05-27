package com.learn.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class JsonUtilTest {
    public static void main(String[] args) {
        String s= "{\"contextDto\":{\"userLogout\":{\"failedCount\":0,\"eventTimeList\":[]},\"userKick\":{\"failedCount\":0,\"eventTimeList\":[]},\"resetRoom\":{\"failedCount\":0,\"eventTimeList\":[]},\"finshRoom\":{\"finshType\":0}},\"metaDataDoc\":{\"lessonUid\":\"721277727c2741a08e9bfae5d92f8154\",\"role\":\"student\",\"appId\":\"10583\"},\"baseDto\":{\"courseDuration\":0,\"lessonStartTime\":0,\"lessonEndTime\":0,\"lessonStartDate\":\"1970/01/01 08:00:00\",\"lessonEndDate\":\"1970/01/01 08:00:00\"},\"id\":\"student:721277727c2741a08e9bfae5d92f8154:ea04466279804dfaae18f5dc55abbb8e\",\"eventTime\":\"2020/01/15 11:33:03\",\"esConfig\":{\"index\":\"zjf-index-2020-05-30\",\"type\":\"doc\",\"id\":\"student:721277727c2741a08e9bfae5d92f8154:ea04466279804dfaae18f5dc55abbb8e\"}}";
        JSONObject object = JSON.parseObject(s);
        Object o = object.getJSONObject("esConfig").get("id");


        System.out.println(o);

    }
}
