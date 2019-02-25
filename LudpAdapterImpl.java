package com.maplecloudy.uds.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONPath;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.maplecloudy.uds.api.bean.ESRestClientFactoryBean;
import com.maplecloudy.uds.api.utils.ElasticsearchQueryUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Service
public class LudpAdapterImpl  {
  private static final Logger logger = LoggerFactory
      .getLogger(LudpAdapterImpl.class);

  public static final String indexName = "ludp_report";
  public static final String indexType = "uds";

  private List<String> needSpecialHandlerKey= Lists.newArrayList();
  //  private List<String> needSpecialHandlerKey= Lists.newArrayList();
  private boolean jia = true;

  private String parseLewindow(String data){
    return null;
  }





  @Autowired
  private ESRestClientFactoryBean restClient;

  @Autowired
  private RestTemplate restTemplate;

  public static Map<String,String> interfaceMappings = loadInterfaceMappings();

  private String buildToEsData(String queryData) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("data", queryData);
    return jsonObject.toString();
  }

  public void pushToEs() throws Exception{
    List<String> lines =FileUtils.readLines(new File("d:/ludp"),"utf-8");
    for (String line : lines) {

      try{
        String[] datas=line.split("!!!!");
        String sourceKey = datas[0];
        String key = sourceKey.replaceAll("\\s+", "&&&");
        String resData="";
        if (needMergeKeys.contains(sourceKey)&&jia) {
          String oldValue="";
          if(jia) {
            oldValue = ElasticsearchQueryUtil
                .queryById(restClient, indexName, indexType, key);

          }
          String newData = datas[1];
          if(StringUtils.isBlank(newData)){
            System.out.println("空数据");
            continue;
          }
          if(newData.contains("\"msg\":\"error\"")){
            System.out.println("错误数据");
            continue;
          }
          if(jia) {
            resData = mergeOldData(oldValue, newData, "", 2);
          }
        }else {
          resData = datas[1];
        }

        String saveToEsData = buildToEsData(resData);
        System.out.println(saveToEsData);

        ElasticsearchQueryUtil
            .postOneData(restClient, indexName, indexType, key, saveToEsData);
      } catch (Exception e) {
        e.printStackTrace();
      }

    }
  }

  private static List<String> needMergeKeys = Lists
      .newArrayList("getCompanionEvent");



  public static String mergeOldData(String oldData, String newData,
      String needMergeDate, int mergeDataLenth) {
    Map<String,TreeMap<String,List<String>>> oldDataMap = new HashMap<>();
    //parse oldData
    String source_oldDatas =JSONPath
        .read(oldData, "$._source.data").toString();
    Map<String,JSONArray> oldDatas = (Map<String,JSONArray>) JSONPath
        .read(source_oldDatas, "$.data");
    for (Map.Entry<String,JSONArray> entry : oldDatas.entrySet()) {
      String key = entry.getKey();
      JSONArray inDatas = entry.getValue();
      if (inDatas == null || inDatas.isEmpty()) {
        continue;
      }
      if (!oldDataMap.containsKey(key)) {
        oldDataMap.put(key, new TreeMap<>());
      }
      JSONArray keys = (JSONArray) inDatas.get(0);
      TreeMap tmpMap = oldDataMap.get(key);
      for (int j = 0; j < keys.size(); j++) {
        for (int i = 1; i < inDatas.size(); i++) {
          JSONArray tmpJsonArray = (JSONArray) inDatas.get(i);
          if (!tmpMap.containsKey(keys.get(j).toString())) {
            tmpMap.put(keys.get(j).toString(), new ArrayList<String>());
          }
          List<String> tmpDataList = (List<String>) tmpMap
              .get(keys.get(j).toString());
          try {
            tmpDataList.add(tmpJsonArray.get(j).toString());
          }catch (Exception e){
            System.out.println(key);
          }
        }
      }
    }
    //parse olddata end
    //paser newData
    Map<String,JSONArray> newDatas = (Map<String,JSONArray>) JSONPath
        .read(newData, "$.data");
    for (Map.Entry<String,JSONArray> entry : newDatas.entrySet()) {
      String key = entry.getKey();
      JSONArray value = entry.getValue();
      //      if (!value.contains(needMergeDate)) {
      //        log.warn("key:" + key + ",date:" + needMergeDate + "没有数据");
      //        continue;
      //      }
      //      int startIndex = value.indexOf(needMergeDate);

      int startIndex = 0;
      needMergeDate = value.get(0).toString();

      if (!oldDataMap.containsKey(key)) {
        oldDataMap.put(key, new TreeMap<>());
      }

      if (oldDataMap.get(key).containsKey(needMergeDate)) {
        continue;
      }
      oldDataMap.get(key).put(needMergeDate, new ArrayList<String>());
      List<String> tmpDataList = oldDataMap.get(key).get(needMergeDate);
      for (int i = 1; i <= mergeDataLenth; i++) {
        tmpDataList.add(value.get(i + startIndex).toString());
      }
    }
    System.out.println(oldDataMap);
    com.alibaba.fastjson.JSONObject retJsonObject = JSON
        .parseObject("{\"code\":0,\"msg\":\"success\"}");
    Map<String,List<List<String>>> dataMap = new TreeMap<>();
    for (Map.Entry<String,TreeMap<String,List<String>>> entry : oldDataMap
        .entrySet()) {
      String key = entry.getKey();
      TreeMap<String,List<String>> value = entry.getValue();
      if (!dataMap.containsKey(key)) {
        dataMap.put(key, new ArrayList<>());
      }
      List<List<String>> inDatas = dataMap.get(key);
      for (Map.Entry<String,List<String>> inEntry : value.entrySet()) {
        String inKey = inEntry.getKey();
        List<String> inValue = inEntry.getValue();
        if (inDatas.isEmpty()) {
          for (int i = 0; i < inValue.size() + 1; i++) {
            inDatas.add(new ArrayList<>());
          }
        }
        inDatas.get(0).add(inKey);
        for (int i = 0; i < inValue.size(); i++) {
          try {
            inDatas.get(i + 1).add(inValue.get(i));
          }catch (Exception e){
            System.out.println(key);
          }
        }
      }
    }
    //截取30天
    Map<String,List<List<String>>> newDataMap = new TreeMap<>();
    for (Map.Entry<String,List<List<String>>> entry : dataMap.entrySet()) {
      Iterator<List<String>> iterator = entry.getValue().iterator();
      List<List<String>> newTmpDatas = new ArrayList<>();
      while (iterator.hasNext()) {
        List<String> tmpValue = iterator.next();
        newTmpDatas.add(tmpValue
            .subList(tmpValue.size() - 32 > 0 ? tmpValue.size() - 32 : 0,
                tmpValue.size()));
      }
      newDataMap.put(entry.getKey(), newTmpDatas);
    }
    retJsonObject.put("data", newDataMap);
    //    System.out.println(retJsonObject.toJSONString());
    return retJsonObject.toJSONString();
  }

  public void saveRes() throws IOException {
    Map<String,String> map = LudpAdapterImpl.interfaceMappings;
    Map<String,String> catchErrorMap = new TreeMap<>();
    Iterator<Map.Entry<String,String>> iterator = map.entrySet().iterator();
    Map<String,String> queryDataMap = new HashMap<>();
    List<String> lines = new ArrayList<>();
    while (iterator.hasNext()) {
      Map.Entry<String,String> entry = iterator.next();
      String sourceKey = entry.getKey();
      String key = sourceKey.replaceAll("\\s+", "&&&");
      String url = entry.getValue();
      String value = null;
      String data=null;
      try {
        //                   value = ElasticsearchQueryUtil.queryById(restClient, indexName, indexType, key);
        if (needMergeKeys.contains(sourceKey)) {
          String oldValue="";
          if(!jia) {
            oldValue = ElasticsearchQueryUtil
                .queryById(restClient, indexName, indexType, key);

          }
          String newData = excuteLudpInterface(url);
          if(StringUtils.isBlank(newData)){
            catchErrorMap.put(sourceKey,url);
            continue;
          }
          if(newData.contains("\"msg\":\"error\"")){
            catchErrorMap.put(sourceKey,newData+"-------"+url);
            continue;
          }
          if(!jia) {
            data = mergeOldData(oldValue, newData, "", 2);
          }else {
            data=newData;
          }
        } else {
          data = excuteLudpInterface(url);
          if(StringUtils.isBlank(data)){
            catchErrorMap.put(sourceKey,url);
            continue;
          }
          if(data.contains("\"msg\":\"error\"")){
            catchErrorMap.put(sourceKey,data+"-------"+url);
            continue;
          }
        }
        lines.add(String.format("%s!!!!%s",key,data));
        System.out.println(key+"@@@@@@@@@@"+data);
        queryDataMap.put("key",data);
      } catch (Exception e) {
        e.printStackTrace();
        catchErrorMap.put(sourceKey,url);
      }
    }
    System.out.println("执行错误列表:");
    Map<String,String> lastDayDataEmpty = new TreeMap<>();

    for (Map.Entry<String,String> entry : catchErrorMap.entrySet()) {
      if (entry.getValue().contains("最近一天数据为空")) {
        lastDayDataEmpty.put(entry.getKey(),entry.getValue());
      } else {
        String errorMess = String.format("%s,%s", entry.getKey(), entry.getValue());
        System.out.println(errorMess);
      }
    }
    System.out.println("最后一天空数据列表:");
    for (Map.Entry<String,String> entry : lastDayDataEmpty.entrySet()) {

        String errorMess = String.format("%s,%s", entry.getKey(), entry.getValue());
        System.out.println(errorMess);

    }
    System.out.println("错误列表end!!!");
    FileUtils.writeLines(new File("d:/ludp"),lines);
  }

  private static String getClassResources() {
    String path = (String.valueOf(Thread.currentThread().getContextClassLoader().getResource(""))).replaceAll("file:/", "").replaceAll("%20", " ").trim();
    if (path.indexOf(":") != 1) {
      path = File.separator + path;
    }
    return path;
  }

  private static Map<String,String> loadInterfaceMappings() {
    Map<String,String> resMap = Maps.newHashMap();
    try {
      String classpath = getClassResources();
      File file = new File(classpath + "/ludpInterfaceMappings.csv");
      if (!file.exists()) {
        file = new File(classpath + "/conf/ludpInterfaceMappings.csv");
      }
      List<String> lines =FileUtils.readLines(file, "utf-8");
      for (String line : lines) {
        String[] datas = line.split("\\,");
        if(datas.length==2) {
          resMap.put(datas[0], datas[1]);
        }
      }
    }catch (Exception e){
      logger.error(e.getMessage());
    }
    return resMap;
  }

  public String buildLudpInterface(String frontpara){
    String key = interfaceMappings.get(frontpara);
    return interfaceMappings.get(key);
  }

  public String excuteLudpInterface(String interfaceUrl){
    ResponseEntity<String> resData = null;
    try {
      resData = restTemplate
          .getForEntity(interfaceUrl, String.class);
    }catch (Exception e){
      logger.error("执行接口出错:"+interfaceUrl,e);
      return null;
    }
    return resData.getBody();
  }

}
