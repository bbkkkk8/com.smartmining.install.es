package com.smartmining.install.es.tools;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class TransUtils {

    //    private static final Logger logger = LoggerFactory.getLogger(TransUtils.class);
    private static final String SCROLL_ALIVE_TIME = "5m";

    public static void main(String[] args) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        ((ch.qos.logback.classic.Logger) logger).setLevel(Level.INFO);
        try {
            JestClient jestClient = null;
            int maxsize = 1;
            String tenantOneCode = "T1000";
            String indexName = "x_restree_release";
            String typeName = "def_release";
            String elasticIps = "http://192.168.0.22:9200";
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig.Builder(elasticIps).connTimeout(60000).readTimeout(60000).multiThreaded(true).build());
            jestClient = factory.getObject();


            JestClient jestClient1 = null;
            String tenantOneCode1 = "T1000";
            String indexName1 = "x_restree";
            String typeName1 = "def";
            String elasticIps1 = "http://192.168.0.91:29200";
            JestClientFactory factory1 = new JestClientFactory();
            factory1.setHttpClientConfig(new HttpClientConfig.Builder(elasticIps1).connTimeout(60000).readTimeout(60000).multiThreaded(true).build());
            jestClient1 = factory1.getObject();


            int pageStart = 0;
            int resultSize = 0;
            while (true) {
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.from(pageStart).size(maxsize);
                searchSourceBuilder.query(QueryBuilders.termQuery("c_tenant_one_code", tenantOneCode));
                Search search = new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(indexName)
                        .addType(typeName)
                        .build();
                JestResult jr = jestClient.execute(search);
                List<String> results = jr.getSourceAsStringList();

                if (results == null || results.size() < 0) {
//                    logger.warn("没有查询到数据");
                    System.out.println("没查到数据");
                    break;
                }
                resultSize = results.size();
                Bulk.Builder bulk = new Bulk.Builder().defaultIndex(indexName1).defaultType(typeName1);
                Index index;
                BulkResult br;
                JSONObject temp;
                for (int i = 0; i < resultSize; i++) {
                    temp = JSONObject.parseObject(String.valueOf(JSON.parseObject(results.get(i))).replace("T1000", "T1001"));
                    temp.put("c_tenantid","750ca335bbe74634853b7fd90c4ba8b7");
                    temp.put("c_tenant_one_id","750ca335bbe74634853b7fd90c4ba8b7");
                    temp.put("c_ptenantid","root");
//                    "c_tenantid": "root",
//                            "c_tenant_one_id": "root",
//                            "c_systype": 0,
//                            "c_id": "FIELD.e7b9fe466b3d4e1caf5f4d1c2b3bffab.test.la_plan.bid_date",
//                            "c_createtime": 1573899886946,
//                            "c_ptenantid": "proot",
                    logger.info(temp.toJSONString());
                    index = new Index.Builder(temp).id(temp.getString("c_id")).build();
                    bulk.addAction(index);
                }
                br = jestClient1.execute(bulk.build());
                logger.info(br.isSucceeded() + "");
                pageStart = pageStart + resultSize;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
