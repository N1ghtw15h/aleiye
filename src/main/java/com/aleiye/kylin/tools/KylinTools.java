package com.aleiye.kylin.tools;

import com.aleiye.common.tools.EncodeTools;
import com.aleiye.common.tools.TimeTools;
import com.google.gson.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @PackageName: com.aleiye.kylin.tools
 * @ClassName: KylinTools
 * @Date: 2019/4/28 17:35
 * @Auther: Linxu
 * @Description: kylin工具类
 */
public class KylinTools {
    private static String host = "10.0.1.205";              //主机ip
    private static String port = "7070";                    //端口号
    private static String ADMIN = "ADMIN";                  //用户名
    private static String KYLIN = "KYLIN";                  //密码
    private static String encoding = "QURNSU46S1lMSU4=";   //用户名密码加密信息
    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final String PUT = "PUT";

    /**
     * 使用jdbc连接kylin
     * @param host 主机IP
     * @param port 端口号
     * @param projectName 项目名
     * @param userName 用户名
     * @param password 密码
     * @return
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SQLException
     */
    public Statement ConnectKylin(String host, String port, String projectName, String userName, String password)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        // 加载Kylin的JDBC驱动程序
        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();

        // 配置登录Kylin的用户名和密码
        Properties info= new Properties();
        info.put("user",userName);//认证登录用户名
        info.put("password",password);//认证登录密码

        // 连接Kylin服务
        String connStr="jdbc:kylin://"+host+":"+port+"/"+projectName;
        Connection conn= driver.connect(connStr, info);
//        Connection conn = DriverManager.getConnection(connStr,userName,password);
        return conn.createStatement();
    }

    /**
     * 执行sql查询kylin上的表
     * @param state
     * @param sql
     * @throws SQLException
     */
    public ResultSet QueryKylin(Statement state,String sql) throws SQLException {
        ResultSet rs = state.executeQuery(sql);
//        while (rs.next()){
//            System.out.println(rs.getString(1));
//        }
        return rs;
    }

    /**
     * 使用RESTful API连接kylin
     * @param user 用户名
     * @param passwd 密码
     * @return
     */
    public String login(String user,String passwd){
        String auth = user + ":" + passwd;

        //使用BASE64Encoder方法对“账号名:密码”进行加密
        EncodeTools encodeTools = new EncodeTools();
        String code = encodeTools.getBASE64Encode(auth);

//        System.out.println("用户名密码加密信息:" + code);

        String para = "/user/authentication";
//        byte[] key = (user+":"+passwd).getBytes();
        encoding = code;
        return excute(para,POST,null);
    }

    /**
     * 发送查询请求
     * @param body
     * @return
     */
    public String query(String body){
        String para = "/query";

        return excute(para,POST,body);
    }

    /**
     * Kylin RESTful API
     * @param para
     * @param method
     * @param body
     * @return
     */
    public String excute(String para,String method,String body){
//        System.out.println(encoding);
        StringBuilder out = new StringBuilder();
        String urlStr = "http://" + host + ":" + port + "/kylin/api" + para;
        try {
            URL url = new URL(urlStr);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(method);
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Basic " + encoding);
            connection.setRequestProperty("Content-Type", "application/json");
            if (body != null) {
                byte[] outputInBytes = body.getBytes("UTF-8");
                OutputStream os = connection.getOutputStream();
                os.write(outputInBytes);
                os.close();
            }
            InputStream content = connection.getInputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(content));
            String line;
            while ((line = in.readLine()) != null) {
                out.append(line);
            }
            in.close();
            connection.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out.toString();
    }

    /**
     * 获取cube信息
     * @param cubeName Cube名
     * @return cubeJson
     */
    public String getCubeDesc(String cubeName) {
        KylinTools kylinTools = new KylinTools();

        //获取cube信息
        String cubeDescArray = kylinTools.excute("/cube_desc/"+cubeName, GET, null);
        String cubeDescJson = cubeDescArray.substring(1, cubeDescArray.length()-1);
//        System.out.println(cubeDescJson);

        JsonParser parser = new JsonParser();

        JsonObject cubeDescJsonObject = parser.parse(cubeDescJson).getAsJsonObject();

        String cubeUuid = cubeDescJsonObject.get("uuid").getAsString();
        String lastModified = TimeTools.stampToTime(cubeDescJsonObject.get("last_modified").getAsString());
        String kylinVersion = cubeDescJsonObject.get("version").getAsString();
        String cubeDescription = cubeDescJsonObject.get("description").getAsString();
        String modelName = cubeDescJsonObject.get("model_name").getAsString();

        String cubeJson = "{\"Kylin版本号\":\"" + kylinVersion +
                "\",\"Cube名\":\"" +cubeName +
                "\",\"Cubeuuid\":\"" +cubeUuid +
                "\",\"Cube最后修改时间\":\"" +lastModified +
                "\",\"Cube描述\":\"" +cubeDescription + "}";

        return cubeJson;
    }

    /**
     * 获取Model信息
     * @param modelName Model名
     * @return modelDescJson
     */
    public String getModelDesc(String modelName) {
        KylinTools kylinTools = new KylinTools();

        //获取Model信息
        JsonParser parser =new JsonParser();  //创建json解析器
//        System.out.println("----------Model_Desc----------");
        String modelJsonStr = kylinTools.excute("/model/" + modelName + "?", GET, null);
        System.out.println(modelJsonStr);

        JsonObject modelJson = parser.parse(modelJsonStr).getAsJsonObject();

        String modelDescription = modelJson.get("description").getAsString();   //获取model描述
        String modelUuid = modelJson.get("uuid").getAsString();   //获取model_uuid
        String modelLastModifiedTime = TimeTools.stampToTime(modelJson.get("last_modified").getAsString());   //获取model最后修改时间
        String factTableName = modelJson.get("fact_table").getAsString();    //获取事实表表名
        String kylinVersion = modelJson.get("version").getAsString();   //获取kylin版本号
        String modelOwner = modelJson.get("owner").getAsString();   //获取model拥有者
        String metrics = modelJson.get("metrics").toString();   //获取量度
        String filterCondition = modelJson.get("filter_condition").getAsString();  //获取过滤条件

        JsonArray lookupsJsonArray = modelJson.get("lookups").getAsJsonArray();  //获取维表
        String lookupsJsonStr = "[";
        Set lookupSet = new HashSet();
        for (JsonElement lookup:lookupsJsonArray) {
            lookupSet.add(lookup.getAsJsonObject().get("table").getAsString());
        }
        for (Object lookup : lookupSet) {
            lookupsJsonStr += "\"" + lookup.toString() + "\",";
        }
        lookupsJsonStr = lookupsJsonStr.substring(0,lookupsJsonStr.length()-1) + "]";

        JsonArray dimensionsJsonArray = modelJson.get("dimensions").getAsJsonArray();  //获取维度
        String dimensionsJsonStr = "[";
        for (JsonElement dimension : dimensionsJsonArray) {
            String table = dimension.getAsJsonObject().get("table").getAsString();
            JsonArray columnsJsonArray = dimension.getAsJsonObject().get("columns").getAsJsonArray();
            for (Object columns: columnsJsonArray) {
                dimensionsJsonStr += "\"" + table + "." + columns.toString().substring(1,columns.toString().length()) + ",";
            }
        }
        dimensionsJsonStr = dimensionsJsonStr.substring(0,dimensionsJsonStr.length()-1) + "]";

        String cubeList = getCubeList(modelName);

        String modelDescJson = "{\"Kylin版本号\":\"" + kylinVersion +
                "\",\"Model拥有者\":\"" + modelOwner +
                "\",\"Model名\":\"" + modelName +
                "\",\"Model描述\":\"" + modelDescription +
//                "\",\"Model_uuid\":\"" + modelUuid +
                "\",\"Model最后修改时间\":\"" + modelLastModifiedTime +
                "\",\"事实表表名\":\"" + factTableName +
                "\",\"维表表名\":\"" + lookupsJsonStr +
                "\",\"维度\":\"" + dimensionsJsonStr +
                "\",\"量度\":\"" + metrics +
                "\",\"过滤条件\":\"" + filterCondition +
                "\",\"Cube列表\":" + cubeList + "}";

        return modelDescJson;
    }

    /**
     * 获取某Model的表信息
     * @param modelName Model名
     * @return modelTableInfo
     */
    public String getModelTableInfo(String modelName){
        KylinTools kylinTools = new KylinTools();

        //获取Model信息
        String modelJsonStr = kylinTools.excute("/model/" + modelName + "?", GET, null);
//        System.out.println(modelJsonStr);

        JsonParser parser =new JsonParser();  //创建json解析器
        JsonObject modelJson = parser.parse(modelJsonStr).getAsJsonObject();

        String factTable = modelJson.get("fact_table").getAsString();   //获取事实表表名
        JsonArray lookupsJsonArray = modelJson.getAsJsonArray("lookups");//获取维度表信息
        String lookups = "[";
        for (JsonElement lookupsJsonElement : lookupsJsonArray) {
            JsonObject lookupsJsonObject = lookupsJsonElement.getAsJsonObject();
            lookups = lookups + "{\"TableName\":\"" + lookupsJsonObject.get("table").getAsString() +
                    ",\"TableJoin\":" + lookupsJsonObject.get("join").getAsJsonObject() + "},";
        }
        lookups = ((lookups.length() == 1) ? lookups : lookups.substring(0,lookups.length()-1)) + "]";
//        System.out.println(lookups);
        String modelTableInfo =  "{\"ModelName\":\"" + modelName +
                "\",\"FactTable\":\"" + factTable +
                "\",\"LookupsTable\":" + lookups + "}";

        return modelTableInfo;
    }

    /**
     * 获取所有cube
     * @return
     */
    public String getCubeList(){
        KylinTools kylinTools = new KylinTools();

        String para = "/cubes";
        String cubeList = kylinTools.excute(para, GET, null);
        return cubeList;
    }

    /**
     * 获取某model下的所有cube名，返回一个list记录这些cube名
     * @param modelName Model名
     * @return cubeNameList
     */
    public String getCubeList(String modelName){
        KylinTools kylinTools = new KylinTools();

        String para = "/cubes";
        String cubeList = kylinTools.excute(para, GET, null);
//        System.out.println(cubeList.substring(1, cubeList.length() - 1));

        JsonParser parser =new JsonParser();  //创建json解析器
        JsonArray cubeListJsonArray = parser.parse(cubeList).getAsJsonArray();

        String cubeNameList = "[";

        for (JsonElement cubeListJsonElement : cubeListJsonArray){
            JsonObject cubeListJsonObject = cubeListJsonElement.getAsJsonObject();
            String model = cubeListJsonObject.get("model").getAsString();
            if(model.equals(modelName)) {
//                System.out.println(cubeJsonObject);
                cubeNameList += "\"" + cubeListJsonObject.get("name").getAsString() + "\",";
            }
        }
        cubeNameList = cubeNameList.substring(0, cubeNameList.length() - 1) + "]";
        return cubeNameList;
    }

    /**
     * 获取所有Model名的List
     * @return modelNameList
     */
    public String getModelList(){
        KylinTools kylinTools = new KylinTools();

        String para = "/models";
        String cubeList = kylinTools.excute(para, GET, null);
//        System.out.println(cubeList);

        JsonParser parser = new JsonParser();  //创建json解析器
        JsonArray modelListJsonArray = parser.parse(cubeList).getAsJsonArray();

        String modelNameList = "[";

        for (JsonElement modelListJsonElement : modelListJsonArray){
            JsonObject cubeListJsonObject = modelListJsonElement.getAsJsonObject();
            modelNameList += "\"" + cubeListJsonObject.get("name").getAsString() + "\",";
        }

        modelNameList = "\"modelName\":" + ((modelNameList.length() == 1) ? "]" : modelNameList.substring(0, modelNameList.length() - 1) + "]");
        return modelNameList;
    }

    /**
     * 获取某projectName下的所有Model名的List
     * @param projectName 项目名
     * @return modelNameList
     */
    public String getModelList(String projectName){
        KylinTools kylinTools = new KylinTools();

        String para = "/projects";
        String projectList = kylinTools.excute(para, GET, null);
//        System.out.println(projectList);

        JsonParser parse = new JsonParser();  //创建json解析器
        JsonArray projectListJsonArray = parse.parse(projectList).getAsJsonArray();

        String modelNameList = "{\"ModelsName\":" ;
        for (JsonElement projectJsonElement : projectListJsonArray) {
            JsonObject projectJsonObject = projectJsonElement.getAsJsonObject();
            if(projectJsonObject.get("name").getAsString().equals(projectName)){
                modelNameList += projectJsonObject.get("models").getAsJsonArray();
            }
        }
        modelNameList += "}";
        return modelNameList;
    }

    /**
     * 获取某project下的所有table
     * @param projectName 项目名
     * @return tables
     */
    public String getTableList(String projectName){
        KylinTools kylinTools = new KylinTools();

        String para = "/tables?project=" + projectName;
        String tableList = kylinTools.excute(para, GET, null);

        JsonParser parser = new JsonParser();   //创建json解析器
        JsonArray tableListJsonArray = parser.parse(tableList).getAsJsonArray();
        String tables = "[";
        for (JsonElement tableJsonElement : tableListJsonArray) {
            JsonObject tableJsonObject = tableJsonElement.getAsJsonObject();
            tables = tables + "{\"DatabaseName\":\"" + tableJsonObject.get("database").getAsString() +  //库名
                    "\",\"TableName\":\"" + tableJsonObject.get("name").getAsString() +                 //表名
                    "\",\"ColumnsInfo\":" +tableJsonObject.get("columns").getAsJsonArray() + "},";      //字段信息
        }
        tables = "{\"TablesInfo\":" + tables +
                ((tables.length() == 0) ? null : tables.substring(0,tables.length()-1) + "]") + "}";

        return tables;
    }

    /**
     * 创建Model
     * @param projectName 项目名
     * @param modelName Model名
     * @param modelDescData Model的json串
     * @return
     */
    public String createModel(String projectName,String modelName,String modelDescData) {
        KylinTools kylinTools = new KylinTools();

        String para = "/models";
        modelDescData = modelDescData.replaceAll("\"", "\\\\\"");
        modelDescData = modelDescData.replaceAll("[\r\n]", "");
        modelDescData = modelDescData.trim();
        String body = "{\"project\":\"" + projectName + "\"," +
                "\"modelName\":\"" + modelName + "\"," +
                "\"modelDescData\":\"" + modelDescData + "\"}";
        kylinTools.excute(para, "POST", body);
        return "Start create model \"" + modelName +"\"!";
    }

    /**
     * 创建Cube
     * @param projectName 项目名
     * @param cubeName Cube名
     * @param cubeDescData Cube的json串
     * @return
     */
    public String createCube(String projectName,String cubeName,String cubeDescData) {
        KylinTools kylinTools = new KylinTools();

        String para = "/cubes";
        cubeDescData = cubeDescData.replaceAll("\"", "\\\\\"");
        cubeDescData = cubeDescData.replaceAll("[\r\n]", "");
        cubeDescData = cubeDescData.trim();
        String body = "{\"project\":\"" + projectName + "\"," +
                "\"cubeName\":\"" + cubeName + "\"," +
                "\"cubeDescData\":" + "\"" + cubeDescData + "\"}";
        kylinTools.excute(para, POST, body);
        return "Start create cube \"" + cubeName +"\"!";
    }

    /**
     * Build 配置完成的 Cube
     * @param cubeName Cube名
     * @return
     */
    public String buildCube(String cubeName){
        KylinTools kylinTools = new KylinTools();

        String para = "/cubes/" + cubeName + "/build";
        String body = "{\"buildType\":\"BUILD\"}";
        kylinTools.excute(para,PUT,body);
        return "Start build cube \"" + cubeName +"\"!";
    }

    /**
     * Rebuild 配置完成的 Cube
     * @param cubeName Cube名
     * @return
     */
    public String rebuildCube(String cubeName){
        KylinTools kylinTools = new KylinTools();

        String para = "/cubes/" + cubeName + "/rebuild";
        String body = "{\"buildType\":\"BUILD\"}";
        kylinTools.excute(para,PUT,body);
        return "Start rebuild cube \"" + cubeName +"\"!";
    }

    /**
     * 获取某一个PROJECT的CUBE状态
     * @param projectName 项目名
     * @return
     */
    public String getJobStatus(String projectName){
        KylinTools kylinTools = new KylinTools();

        //status = NEW：0，PENDING：1，RUNNING：2，STOPPED：32，FINISHED：4，ERROR：8, DISCARDED: 16
        //timeFilter = LAST ONE DAY: 0, LAST ONE WEEK: 1, LAST ONE MONTH: 2, LAST ONE YEAR: 3, ALL: 4
        String para="/jobs?projectName=" + projectName + "&timeFilter=4&status=1&status=2&status=4&status=8";
        String jobList =  kylinTools.excute(para, GET, null);

        //创建json解析器
        JsonParser parser = new JsonParser();
        JsonArray jobListJsonArray = parser.parse(jobList).getAsJsonArray();
        String jobStatus = "[";
        for (JsonElement jobListJsonElement : jobListJsonArray){
            JsonObject jobListJsonObject = jobListJsonElement.getAsJsonObject();
            String name = jobListJsonObject.get("name").getAsString();
            jobStatus += "\"" + name.substring(name.indexOf("-") + 2, name.indexOf("-", name.indexOf("-") + 1) - 1)+ "\":";
            jobStatus += "\"" + new java.text.DecimalFormat("#.00").format(jobListJsonObject.get("progress").getAsDouble()) + "%\",";
        }
        jobStatus = "\"jobs_status\":" + ((jobStatus.length() == 1) ? "]" : jobStatus.substring(0, jobStatus.length() - 1) + "]");
        return jobStatus;
    }

    public void kylinLogin(String permission){
        if (permission.equals("admin")) {
            login(ADMIN, KYLIN);
        } else if (permission.equals("model")) {
            login("kylin", "abcdef");
        } else if (permission.equals("query")) {
            login("query","123456");
        }
    }

    public static void main(String[] args) {

    }
}
