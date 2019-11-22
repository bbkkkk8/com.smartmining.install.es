package com.smartmining.install.es.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.gson.JsonObject;
import com.smartmining.install.es.service.IDataTransService;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.util.ResourceUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Service
public class DataTransService implements IDataTransService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private Environment env;
    private final int maxsize = 2;

    public void exportIndex(String tenantOneCode) {
        JestClient jestClient = null;
        try {
//            String indexName = env.getProperty("es.index.name");
//            String typeName = env.getProperty("es.index.type");
//            String elasticIps = env.getProperty("es.address");
            String indexName = "x_restree_dev";
            String typeName = "def_dev";
            String elasticIps = "http://192.168.0.23:9200";
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig.Builder(elasticIps).connTimeout(60000).readTimeout(60000).multiThreaded(true).build());
            jestClient = factory.getObject();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.from(0).size(maxsize);
            searchSourceBuilder.query(QueryBuilders.termQuery("c_tenant_one_code", tenantOneCode));
            Search search = new Search.Builder(searchSourceBuilder.toString())
                    .addIndex(indexName)
                    .addType(typeName)
                    .build();
            JestResult jr = jestClient.execute(search);
            List<String> results = jr.getSourceAsStringList();
            if (results.size() < 0) {
                logger.warn("没有查询到数据");
                return;
            }
            JSONArray dataArr = new JSONArray();
            for (int i = 0; i < results.size(); i++) {
                dataArr.add(JSON.parseObject(results.get(i)));
            }
            byte[] bytes = JSON.toJSONBytes(dataArr);
            HttpServletResponse response = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getResponse();
            try {
                String fileName = getToDayDateStr() + "_log.txt";
                response.addHeader("Content-Disposition", "attachment;filename=" + fileName);
                response.addHeader("filename", fileName);
                OutputStream toClient = new BufferedOutputStream(response.getOutputStream());
//            response.setContentType("application/octetc-download");
                response.setContentType("*/*");
                toClient.write(bytes);
                toClient.flush();
                toClient.close();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("文件下载异常！", e);

            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (jestClient != null) {
                try {
                    jestClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void importIndex(String tenantOneCode) {

    }

    private String getToDayDateStr() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date());
    }

    public static void main(String[] args) {
        new DataTransService().exportIndex("T1001");
    }
}
