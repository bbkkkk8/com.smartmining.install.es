package com.smartmining.install.es.ctl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class DemoCtl {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private Environment env;
    private final int maxsize = 2;


    @RequestMapping("/exportz")
    public String importz(@RequestParam String code, HttpServletResponse response) {
        JestClient jestClient = null;
        try {
            String indexName = env.getProperty("es.index.name");
            String typeName = env.getProperty("es.index.type");
            String elasticIps = env.getProperty("es.address");
//            String indexName = "x_restree_dev";
//            String typeName = "def_dev";
//            String elasticIps = "http://192.168.0.23:9200";
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig.Builder(elasticIps).connTimeout(60000).readTimeout(60000).multiThreaded(true).build());
            jestClient = factory.getObject();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.from(0).size(maxsize);
            searchSourceBuilder.query(QueryBuilders.termQuery("c_tenant_one_code", code));
            Search search = new Search.Builder(searchSourceBuilder.toString())
                    .addIndex(indexName)
                    .addType(typeName)
                    .build();
            JestResult jr = jestClient.execute(search);
            List<String> results = jr.getSourceAsStringList();
            if (results.size() < 0) {
                logger.warn("没有查询到数据");
                return "没有查询到数据";
            }
            JSONArray dataArr = new JSONArray();
            for (int i = 0; i < results.size(); i++) {
                dataArr.add(JSON.parseObject(results.get(i)));
            }
            byte[] bytes = JSON.toJSONBytes(dataArr);
            try {
                String fileName =code+"_"+getToDayDateStr() + ".json";
                response.addHeader("Content-Disposition", "attachment;filename=" + fileName);
                response.addHeader("filename", fileName);
                OutputStream toClient = new BufferedOutputStream(response.getOutputStream());
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
        return "success";
    }

    /**
     * 如果上传的地方upload.html中的名称如果和这里的参数MultipartFile名称一致的话就不用加@RequestParm注解，
     * 上传的名称为：filename,而这里接受的却为multipartFile，所以为了可以接受到就将名字注解一下，或者改为一致也行。
     * 至于RequestMapping中的参数要和上传时的action参数一致，这样上传的时候才能访问到本方法。
     *
     * @param multipartFile springMvc封装好的一个文件对象，其中可以包括：图片，音频，视频，文本....
     * @return
     * @throws Exception
     */
    @RequestMapping("/importz")
    public String upload(@RequestParam("filename") MultipartFile multipartFile, @RequestParam String code) throws Exception {
        System.out.println("文件名：" + multipartFile.getOriginalFilename());
        //保存文件
//        multipartFile.transferTo(new File("e:/"+multipartFile.getOriginalFilename()));

        String indexName = env.getProperty("es.index.name");
        String typeName = env.getProperty("es.index.type");
        String elasticIps = env.getProperty("es.address");
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(elasticIps).connTimeout(60000).readTimeout(60000).multiThreaded(true).build());
        JestClient jestClient = factory.getObject();
        List<JSONObject> resData = null;
        // JSON 文件 解析成 List<XOpsAudit>对象
        try {
            String jsonStr = JSON.toJSONString(JSON.parse(multipartFile.getBytes()));
            resData = JSON.parseObject(jsonStr, new TypeReference<List<JSONObject>>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("文件解析异常！", e);

        }
        if (resData == null || resData.size() < 1) {
            logger.error("无对应数据！");
            return "无对应数据！";
        }

        // id设置为null，直接新增
        resData.stream().forEach(e -> {
            e.put("c_tenant_one_code", code);
//            e.put("c_id",UUID.randomUUID().toString().replace("-",""));//测试用
            logger.debug(e.toJSONString());
        });

        Bulk.Builder bulk = new Bulk.Builder().defaultIndex(indexName).defaultType(typeName);
        Index index;
        BulkResult br;
        int cnt = 0;

        for (JSONObject obj : resData) {
            cnt++;
            index = new Index.Builder(obj).id(obj.getString("c_id")).build();
            bulk.addAction(index);
            if (cnt % 500 == 0) {
                br = jestClient.execute(bulk.build());
                logger.debug(br.isSucceeded() + "");
            }
        }
        br = jestClient.execute(bulk.build());
        logger.debug(br.isSucceeded() + "");
        jestClient.close();
        return "success,total=" + resData.size();
    }

    private String getToDayDateStr() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date());
    }

}