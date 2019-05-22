package com.aleiye.kylin;

import com.aleiye.kylin.tools.KylinTools;
import com.google.gson.*;


/**
 * @PackageName: com.aleiye.kylin
 * @ClassName: TestKylin
 * @Date: 2019/4/28 17:55
 * @Auther: Linxu
 * @Description: TODO
 */
public class TestKylin {
    public static void main(String[] args) {
//        testGetKylinInfo();
//        testCreateModelAndCubeAndBuildCube();
        KylinTools kylinTools = new KylinTools();
        kylinTools.kylinLogin("admin");
        String wz_jihua_all = kylinTools.getModelList("wz_jihua_all");
        System.out.println(wz_jihua_all);

        KylinTools kylinTools1 = new KylinTools();
        kylinTools1.kylinLogin("general");
        String wz_jihua_all1 = kylinTools1.getModelList("Test");
        System.out.println(wz_jihua_all1);
    }

    /**
     * 测试获取kylin上的信息，包括：
     * 1.根据ProjectName获取ModelName列表;
     * 2.根据ProjectName获取TableInfo列表(包括DatabaseName,TableName以及ColumnsName);
     * 3.根据ModelName获取TableInfo(包括TableName以及TableJoin).
     */
    public static void testGetKylinInfo(){
        KylinTools kylinTools = new KylinTools();
        kylinTools.login("ADMIN","KYLIN");

        System.out.println("----------ModelsName----------");
        String modelsName = kylinTools.getModelList("XYKC_NEW_DATA");
        System.out.println(modelsName);
        System.out.println();

        System.out.println("----------TablesInfo----------");
        String tablesInfo = kylinTools.getTableList("XYKC_NEW_DATA");
        System.out.println(tablesInfo);
        System.out.println();

        JsonParser parser = new JsonParser();
        JsonObject modelsNameJsonObject = parser.parse(modelsName).getAsJsonObject();
        JsonArray modelsNameJsonArray = modelsNameJsonObject.getAsJsonArray("ModelsName");

        System.out.println("----------ModelTableInfo----------");
        for (JsonElement modelsNameJsonElement: modelsNameJsonArray) {
            System.out.println("----------" + modelsNameJsonElement.getAsString() + "----------");
            String modelTableInfo = kylinTools.getModelTableInfo(modelsNameJsonElement.getAsString());
            System.out.println(modelTableInfo);
        }
    }

    /**
     * 在kylin上创建Model和Cube，并build Cube
     */
    public static void testCreateModelAndCubeAndBuildCube(){
        KylinTools kylinTools = new KylinTools();
        kylinTools.login("ADMIN","KYLIN");

        String modelDescData = "{\"name\":\"model_test\"," +
                "\"description\":\"测试model\"," +
                "\"fact_table\":\"WZ_JIHUA_ALL.SGCC_ECP_ADM_BID\"," +
                "\"lookups\":[" +
                "{\"table\":\"WZ_JIHUA_ALL.LA_PLAN\",\"alias\":\"LA_PLAN\",\"joinTable\":\"SGCC_ECP_ADM_BID\",\"kind\":\"LOOKUP\"," +
                "\"join\":{\"type\":\"inner\",\"primary_key\":[\"LA_PLAN.PLAN_CODE\"],\"foreign_key\":[\"SGCC_ECP_ADM_BID.PLAN_CODE\"]," +
                "\"isCompatible\":[true],\"pk_type\":[\"varchar(256)\"],\"fk_type\":[\"varchar(256)\"]}}]," +
                "\"filter_condition\":\"LA_PLAN.PLAN_YEAR>=2018\"," +
                "\"dimensions\":[" +
                "{\"table\":\"SGCC_ECP_ADM_BID\",\"columns\":[\"ZB_REQUIRE_CODE\",\"ZB_REQUIRE_LINE\"," +
                "\"COMPANY_ID\",\"ORGAN_NAME\",\"PLAN_CODE\",\"OBJECT_NAME\",\"PACKAGE_NAME\"]}," +
                "{\"table\":\"LA_PLAN\",\"columns\":[\"PLAN_YEAR\",\"PLAN_CODE\",\"PLAN_BATCH\",\"PLAN_NAME\"]}]," +
                "\"metrics\":[" +
                "\"SGCC_ECP_ADM_BID.AMOUNT\"," +
                "\"SGCC_ECP_ADM_BID.NET_PRICE\"," +
                "\"SGCC_ECP_ADM_BID.TOTAL_NET_PRICE\"," +
                "\"SGCC_ECP_ADM_BID.PRICE\"," +
                "\"SGCC_ECP_ADM_BID.TOTAL_PRICE\"]," +
                "\"partition_desc\":{\"partition_type\":\"APPEND\",\"partition_date_format\":\"yyyy-MM-dd\"}," +
                "\"last_modified\":0}";

        String cubeDescData = "{" +
                "\"name\":\"cube_test\"," +
                "\"model_name\":\"model_test\"," +
                "\"description\":\"测试cube\"," +
                "\"dimensions\":[" +
                "{\"name\":\"ZB_REQUIRE_CODE\",\"table\":\"SGCC_ECP_ADM_BID\",\"column\":\"ZB_REQUIRE_CODE\"}," +
                "{\"name\":\"ZB_REQUIRE_LINE\",\"table\":\"SGCC_ECP_ADM_BID\",\"column\":\"ZB_REQUIRE_LINE\"}," +
                "{\"name\":\"COMPANY_ID\",\"table\":\"SGCC_ECP_ADM_BID\",\"column\":\"COMPANY_ID\"}," +
                "{\"name\":\"ORGAN_NAME\",\"table\":\"SGCC_ECP_ADM_BID\",\"column\":\"ORGAN_NAME\"}," +
                "{\"name\":\"PLAN_CODE\",\"table\":\"SGCC_ECP_ADM_BID\",\"column\":\"PLAN_CODE\"}," +
                "{\"name\":\"OBJECT_NAME\",\"table\":\"SGCC_ECP_ADM_BID\",\"column\":\"OBJECT_NAME\"}," +
                "{\"name\":\"PACKAGE_NAME\",\"table\":\"SGCC_ECP_ADM_BID\",\"column\":\"PACKAGE_NAME\"}," +
                "{\"name\":\"PLAN_YEAR\",\"table\":\"LA_PLAN\",\"derived\":[\"PLAN_YEAR\"]}," +
                "{\"name\":\"PLAN_CODE\",\"table\":\"LA_PLAN\",\"derived\":[\"PLAN_CODE\"]}," +
                "{\"name\":\"PLAN_BATCH\",\"table\":\"LA_PLAN\",\"derived\":[\"PLAN_BATCH\"]}," +
                "{\"name\":\"PLAN_NAME\",\"table\":\"LA_PLAN\",\"derived\":[\"PLAN_NAME\"]}]," +
                "\"measures\":[" +
                "{\"name\":\"_COUNT_\",\"function\":{\"expression\":\"COUNT\",\"returntype\":\"bigint\"," +
                "\"parameter\":{\"type\":\"constant\",\"value\":\"1\"},\"configuration\":{}}}," +
                "{\"name\":\"AMOUNT\",\"function\":{\"expression\":\"SUM\",\"returntype\":\"double\",\"parameter\":" +
                "{\"type\":\"column\",\"value\":\"SGCC_ECP_ADM_BID.AMOUNT\"},\"configuration\":{}}}," +
                "{\"name\":\"NET_PRICE\",\"function\":{\"expression\":\"SUM\",\"returntype\":\"double\",\"parameter\":" +
                "{\"type\":\"column\",\"value\":\"SGCC_ECP_ADM_BID.NET_PRICE\"},\"configuration\":{}}}," +
                "{\"name\":\"TOTAL_NET_PRICE\",\"function\":{\"expression\":\"SUM\",\"returntype\":\"double\",\"parameter\":" +
                "{\"type\":\"column\",\"value\":\"SGCC_ECP_ADM_BID.TOTAL_NET_PRICE\"},\"configuration\":{}}}," +
                "{\"name\":\"PRICE\",\"function\":{\"expression\":\"SUM\",\"returntype\":\"double\",\"parameter\":" +
                "{\"type\":\"column\",\"value\":\"SGCC_ECP_ADM_BID.PRICE\"},\"configuration\":{}}}," +
                "{\"name\":\"TOTAL_PRICE\",\"function\":{\"expression\":\"SUM\",\"returntype\":\"double\",\"parameter\":" +
                "{\"type\":\"column\",\"value\":\"SGCC_ECP_ADM_BID.TOTAL_PRICE\"},\"configuration\":{}}}]," +
                "\"dictionaries\":[]," +
                "\"rowkey\":{\"rowkey_columns\":[" +
                "{\"column\":\"SGCC_ECP_ADM_BID.ZB_REQUIRE_CODE\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false}," +
                "{\"column\":\"SGCC_ECP_ADM_BID.ZB_REQUIRE_LINE\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false}," +
                "{\"column\":\"SGCC_ECP_ADM_BID.COMPANY_ID\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false}," +
                "{\"column\":\"SGCC_ECP_ADM_BID.ORGAN_NAME\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false}," +
                "{\"column\":\"SGCC_ECP_ADM_BID.PLAN_CODE\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false}," +
                "{\"column\":\"SGCC_ECP_ADM_BID.OBJECT_NAME\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false}," +
                "{\"column\":\"SGCC_ECP_ADM_BID.PACKAGE_NAME\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false}]}," +
                "\"aggregation_groups\":[{\"includes\":[\"SGCC_ECP_ADM_BID.ZB_REQUIRE_CODE\"," +
                "\"SGCC_ECP_ADM_BID.ZB_REQUIRE_LINE\",\"SGCC_ECP_ADM_BID.COMPANY_ID\",\"SGCC_ECP_ADM_BID.ORGAN_NAME\"," +
                "\"SGCC_ECP_ADM_BID.PLAN_CODE\",\"SGCC_ECP_ADM_BID.OBJECT_NAME\",\"SGCC_ECP_ADM_BID.PACKAGE_NAME\"]," +
                "\"select_rule\":{\"hierarchy_dims\":[],\"mandatory_dims\":[],\"joint_dims\":[]}}]," +
                "\"hbase_mapping\":{\"column_family\":[{\"name\":\"F1\",\"columns\":[{\"qualifier\":\"M\"," +
                "\"measure_refs\":[\"_COUNT_\",\"AMOUNT\",\"NET_PRICE\",\"TOTAL_NET_PRICE\",\"PRICE\",\"TOTAL_PRICE\"]}]}]}," +
                "\"partition_date_start\":0," +
                "\"notify_list\":[]," +
                "\"retention_range\":0," +
                "\"status_need_notify\":[\"ERROR\",\"DISCARDED\",\"SUCCEED\"]," +
                "\"auto_merge_time_ranges\":[]," +
                "\"engine_type\":2," +
                "\"storage_type\":2," +
                "\"override_kylin_properties\":{}}}";

        try {
            System.out.println(kylinTools.createModel("Test","model_test",modelDescData));

            Thread.sleep(10000);

            System.out.println(kylinTools.createCube("Test","cube_test",cubeDescData));

            Thread.sleep(10000);

            System.out.println(kylinTools.buildCube("cube_test"));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
