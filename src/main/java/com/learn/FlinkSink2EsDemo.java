package com.learn;

import com.alibaba.fastjson.JSON;
import com.learn.utils.ESSinkUtil;
import com.learn.utils.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;


import java.util.List;
import static com.learn.constant.PropertiesConstants.*;

@Slf4j
public class FlinkSink2EsDemo {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\bdproject\\flink-learning\\flink-learning-connectors\\flink-learning-connectors-es\\flink-learning-connectors-es7\\src\\main\\resources\\data.txt";
        DataStreamSource<String> data = env.readTextFile(path);
//        data.print();

        List<HttpHost> esAddresses = ESSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);

        ESSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data, new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(Requests.indexRequest()
                        .index("zjf_2020-05-26")
                        .type("ooo")
                        .source(JSON.parseObject(element)));
            }
        }, parameterTool);

        log.info("-----esAddresses = {}, parameterTool = {}, ", esAddresses, parameterTool);

        env.execute("demo");
    }
}
