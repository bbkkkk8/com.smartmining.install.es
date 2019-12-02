package com.smartmining.install.es.ctl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
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


//    @RequestMapping("/exportz")
//    public String exportz(@RequestParam String code, HttpServletResponse response) {
//        JestClient jestClient = null;
//        try {
//            String indexName = env.getProperty("es.index.name");
//            String typeName = env.getProperty("es.index.type");
//            String elasticIps = env.getProperty("es.address");
//            int maxsize = Integer.parseInt(env.getProperty("es.maxrow"));
////            String indexName = "x_restree_dev";
////            String typeName = "def_dev";
////            String elasticIps = "http://192.168.0.23:9200";
//            JestClientFactory factory = new JestClientFactory();
//            factory.setHttpClientConfig(new HttpClientConfig.Builder(elasticIps).connTimeout(60000).readTimeout(60000).multiThreaded(true).build());
//            jestClient = factory.getObject();
//            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            searchSourceBuilder.from(0).size(maxsize);
//            searchSourceBuilder.query(QueryBuilders.termQuery("c_tenant_one_code", code));
//            Search search = new Search.Builder(searchSourceBuilder.toString())
//                    .addIndex(indexName)
//                    .addType(typeName)
//                    .build();
//            JestResult jr = jestClient.execute(search);
//            List<String> results = jr.getSourceAsStringList();
//            if (results.size() < 0) {
//                logger.warn("没有查询到数据");
//                return "没有查询到数据";
//            }
//            JSONArray dataArr = new JSONArray();
//            for (int i = 0; i < results.size(); i++) {
//                dataArr.add(JSON.parseObject(results.get(i)));
//            }
//            byte[] bytes = JSON.toJSONBytes(dataArr);
//            try {
//                String fileName = code + "_" + getToDayDateStr() + ".json";
//                response.addHeader("Content-Disposition", "attachment;filename=" + fileName);
//                response.addHeader("filename", fileName);
//                OutputStream toClient = new BufferedOutputStream(response.getOutputStream());
//                response.setContentType("*/*");
//                toClient.write(bytes);
//                toClient.flush();
//                toClient.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//                logger.error("文件下载异常！", e);
//
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (jestClient != null) {
//                try {
//                    jestClient.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        return "success";
//    }

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

        String envName = env.getProperty("es.index.env");
        String elasticIps = env.getProperty("es.address");
        String treeIndexName = "x_restree" + envName;
        String bloodIndexName = "x_blood" + envName;
        String typeName = "def" + envName;

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(elasticIps).connTimeout(60000).readTimeout(60000).multiThreaded(true).build());
        JestClient jestClient = factory.getObject();
        JSONObject importData;
        // JSON 文件 解析成 List<XOpsAudit>对象
        try {
            String jsonStr = JSON.toJSONString(JSON.parse(multipartFile.getBytes()));
            importData = JSON.parseObject(jsonStr, new TypeReference<JSONObject>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("文件解析异常！", e);

            return "文件解析异常";
        }
        List<JSONObject> treeData = importData.getJSONArray("treedata").toJavaList(JSONObject.class);
        List<JSONObject> bloodData = importData.getJSONArray("blooddata").toJavaList(JSONObject.class);
        if (treeData == null || treeData.size() < 1) {
            logger.error("无对应数据！");
            return "无对应数据！";
        }
        JSONObject one=treeData.get(0);
        String oldcode=one.getString("c_tenant_one_code");
        // 写入前数据转换为对应租户数据
        treeData.stream().forEach(e -> {
            e.put("c_tenant_one_code", code);
            e.put("c_id", e.getString("c_id").replace(oldcode, code));
            e.put("c_pid", e.getString("c_pid").replace(oldcode, code));
//            e.put("c_id",UUID.randomUUID().toString().replace("-",""));//测试用
            logger.info("写入资源{}",e.toJSONString());
        });

        Bulk.Builder bulk = new Bulk.Builder().defaultIndex(treeIndexName).defaultType(typeName);
        Index index;
        BulkResult br;
        int cnt = 0;

        for (JSONObject obj : treeData) {
            cnt++;
            logger.info("=======================已经导入{}条记录",cnt);
            index = new Index.Builder(obj).id(obj.getString("c_id")).build();
            bulk.addAction(index);
            batchDeleteBlood(obj.getString("c_id"), bloodIndexName, typeName, jestClient);
            if (cnt % 500 == 0) {
                br = jestClient.execute(bulk.build());
                logger.info(br.isSucceeded() + "");
            }
        }

        br = jestClient.execute(bulk.build());
        logger.info(br.isSucceeded() + "");


        batchInsertBlood(bloodData,bloodIndexName,typeName,oldcode,code,jestClient);

        jestClient.close();
        return "success,total=" + treeData.size();
    }

    private void batchDeleteBlood(String id, String bloodIndexName, String typeName, JestClient jestClient) throws IOException {
        Bulk.Builder bulk = new Bulk.Builder().defaultIndex(bloodIndexName).defaultType(typeName);
        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        searchSourceBuilder2.from(0).size(10000);
        searchSourceBuilder2.query(QueryBuilders.termQuery("c_id", id));
        Search search2 = new Search.Builder(searchSourceBuilder2.toString())
                .addIndex(bloodIndexName)
                .addType(typeName)
                .build();
        List<SearchResult.Hit<JSONObject, Void>> searchResults =
                jestClient.execute(search2)
                        .getHits(JSONObject.class);
        searchResults.forEach(hit -> {
            logger.info(String.format("Document %s has score %s", hit.id, hit.score));
            Delete dr = new Delete.Builder(hit.id).build();
            bulk.addAction(dr);
            logger.info("删除血统id={},内容={}",hit.id,hit.source.toJSONString());
        });
        BulkResult   br = jestClient.execute(bulk.build());
        logger.info(br.isSucceeded() + "");


//        List<String> results2 = jr2.getSourceAsStringList();
//        if (results2.size() < 1) {
//            logger.warn("没有查询到数据");
//            return;
//        }
//        for (int i = 0; i < results2.size(); i++) {
//            dr= new Delete.Builder(results2.get(i).).build();
//            bulk.addAction(dr);
//        }

    }

    private void batchInsertBlood(List<JSONObject> bloodData, String bloodIndexName, String typeName,String oldcode,String code, JestClient jestClient) throws IOException {
        Bulk.Builder bulk = new Bulk.Builder().defaultIndex(bloodIndexName).defaultType(typeName);
        Index index;
        BulkResult br;
        int cnt = 0;
        for (JSONObject obj : bloodData) {
            cnt++;
            if(obj.getString("c_bloodid")==null){//有些垃圾数据
                continue;
            }
            obj.put("c_tenant_one_code",code);
            obj.put("c_id",obj.getString("c_id").replace(oldcode,code));
            obj.put("c_bloodid",obj.getString("c_bloodid").replace(oldcode,code));
            index = new Index.Builder(obj).build();
            logger.info("写入血统{}",obj.toJSONString());
            bulk.addAction(index);
            if (cnt % 500 == 0) {
                br = jestClient.execute(bulk.build());
                logger.info(br.isSucceeded() + "");
            }
        }
        br = jestClient.execute(bulk.build());
        logger.info(br.isSucceeded() + "");
    }

    private String getToDayDateStr() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date());
    }

    @RequestMapping("/exportsub")
    public String exportsub(@RequestParam String id, HttpServletResponse response) {
        JestClient jestClient = null;
        JSONArray treeArr = new JSONArray();
        try {
            String envName = env.getProperty("es.index.env");
            String elasticIps = env.getProperty("es.address");
            String treeIndexName = "x_restree" + envName;
            String bloodIndexName = "x_blood" + envName;
            String typeName = "def" + envName;
//            int maxsize = Integer.parseInt(env.getProperty("es.maxrow"));
            int maxsize = 10000;

//            String indexName = "x_restree_dev";
//            String typeName = "def_dev";
//            String elasticIps = "http://192.168.0.23:9200";
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig.Builder(elasticIps).connTimeout(60000).readTimeout(60000).multiThreaded(true).build());
            jestClient = factory.getObject();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.from(0).size(maxsize);
            searchSourceBuilder.query(QueryBuilders.termQuery("c_id", id));
            Search search = new Search.Builder(searchSourceBuilder.toString())
                    .addIndex(treeIndexName)
                    .addType(typeName)
                    .build();
            JestResult jr = jestClient.execute(search);
            List<String> results = jr.getSourceAsStringList();
            if (results==null||results.size() < 1) {
                logger.warn("没有查询到数据");
                return "没有查询到数据";
            }


            JSONObject jsonObject = JSON.parseObject(results.get(0));
            treeArr.add(jsonObject);

            JSONArray bloodArr = new JSONArray();

            SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
            searchSourceBuilder2.from(0).size(maxsize);
            searchSourceBuilder2.query(QueryBuilders.termQuery("c_id", id));
            Search search2 = new Search.Builder(searchSourceBuilder2.toString())
                    .addIndex(bloodIndexName)
                    .addType(typeName)
                    .build();
            JestResult jr2 = jestClient.execute(search2);
            List<String> results2 = jr2.getSourceAsStringList();
            if (results2==null||results2.size() < 1) {
                logger.warn("没有查询到血统数据");
            }else {
                for (int i = 0; i < results2.size(); i++) {
                    bloodArr.add(JSON.parseObject(results2.get(i)));
                }
            }

            recursionByPid(jsonObject.getString("c_id"), treeArr, bloodArr, jestClient, treeIndexName, bloodIndexName, typeName, maxsize);
            JSONObject jsonResult = new JSONObject();
            jsonResult.put("treedata", treeArr);
            jsonResult.put("blooddata", bloodArr);
            byte[] bytes = JSON.toJSONBytes(jsonResult);
            try {
                String fileName = id + "_" + getToDayDateStr() + ".json";
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
                return "文件下载异常"+e.getMessage();
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
        return "success,total=" + treeArr.size();
    }


    private void recursionByPid(String pid, JSONArray dataArr, JSONArray bloodArr, JestClient jestClient, String treeIndexName, String bloodIndexName, String typeName, int maxsize) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0).size(maxsize);
        searchSourceBuilder.query(QueryBuilders.termQuery("c_pid", pid));
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(treeIndexName)
                .addType(typeName)
                .build();
        JestResult jr = jestClient.execute(search);
        List<String> results = jr.getSourceAsStringList();
        if (results==null||results.size() < 0) {
            logger.warn("没有查询到数据");
            return;
        } else {
            JSONObject jsonObject;
            SearchSourceBuilder searchSourceBuilder2;
            Search search2;
            JestResult jr2;
            List<String> results2;
            for (int i = 0; i < results.size(); i++) {
                jsonObject = JSON.parseObject(results.get(i));
                dataArr.add(jsonObject);

                searchSourceBuilder2 = new SearchSourceBuilder();
                searchSourceBuilder2.from(0).size(maxsize);
                searchSourceBuilder2.query(QueryBuilders.termQuery("c_id", jsonObject.getString("c_id")));
                search2 = new Search.Builder(searchSourceBuilder2.toString())
                        .addIndex(bloodIndexName)
                        .addType(typeName)
                        .build();
                jr2 = jestClient.execute(search2);
                results2 = jr2.getSourceAsStringList();
                if (results2.size() < 1) {
                    logger.warn("没有查询到血统数据");
                }else {
                    for (int j = 0; j < results2.size(); j++) {
                        bloodArr.add(JSON.parseObject(results2.get(j)));
                    }
                }
                recursionByPid(jsonObject.getString("c_id"), dataArr, bloodArr, jestClient, treeIndexName, bloodIndexName, typeName, maxsize);
            }
        }
    }
}