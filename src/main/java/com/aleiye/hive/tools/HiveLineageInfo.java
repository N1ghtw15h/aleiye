package com.aleiye.hive.tools;

import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;

import java.util.*;

/**
 * @PackageName: com.aleiye.hive.tools
 * @ClassName: HiveLineageInfo
 * @Date: 2019/5/13 14:52
 * @Auther: Linxu
 * @Description: HiveSQL解析血缘关系工具类
 */
public class HiveLineageInfo implements NodeProcessor {
    TreeSet<String> inputTableList = new TreeSet();
    TreeSet<String> outputTableList = new TreeSet();
    TreeSet<String> tempTableList = new TreeSet();
    TreeSet<String> joinList = new TreeSet();
    TreeSet<String> whereList = new TreeSet();
    TreeSet<String> outputColumnList = new TreeSet();
    TreeSet<String> groupByColumnList = new TreeSet();
    TreeSet<String> orderByColumnList = new TreeSet();

    HashMap<String, String> tableAndColumnMap = new HashMap<>();
    HashMap<String, String> tableMap = new HashMap<>();

    public HiveLineageInfo() {
    }

    public TreeSet<String> getInputTableList() {
        return this.inputTableList;
    }

    public TreeSet<String> getOutputTableList() {
        return this.outputTableList;
    }

    public TreeSet<String> getTempTableList() {
        return this.tempTableList;
    }

    public TreeSet<String> getJoinList() {
        return this.joinList;
    }

    public TreeSet<String> getWhereList() {
        return this.whereList;
    }

    public TreeSet<String> getOutputColumnList() {
        return this.outputColumnList;
    }

    public TreeSet<String> getGroupByColumnList() {
        return this.groupByColumnList;
    }

    public TreeSet<String> getOrderByColumnList() {
        return this.orderByColumnList;
    }

    public HashMap<String,String> getTableAndColumnMap() {
        return this.tableAndColumnMap;
    }

    public HashMap<String,String> getTableMap() {
        return this.tableMap;
    }

    /**
     * 循环遍历tok_select,取outputColumnList
     * @param pt
     */
    public void traversalTokSelect(ASTNode pt){
        for(int i = 0;i < pt.getChildCount(); i++){
            if (pt.getChild(i).getChild(0).getText().toLowerCase().equals("tok_table_or_col")) {  //字段名
                outputColumnList.add(pt.getChild(i).getChild(0).getChild(0).getText().toLowerCase());
            } else if (pt.getChild(i).getChild(0).getText().equals(".")) {  //别名.字段名
                outputColumnList.add((pt.getChild(i).getChildCount()>1) ? pt.getChild(i).getChild(1).getText()
                        .toLowerCase() : pt.getChild(i).getChild(0).getChild(1).getText().toLowerCase());
            } else if (pt.getChild(i).getChild(0).getText().toLowerCase().equals("tok_function")) {  //函数(字段)
                outputColumnList.add(pt.getChild(i).getChild(1).getText().toLowerCase());
            } else if (pt.getChild(i).getChild(0).getText().toLowerCase().equals("tok_allcolref")) { //*
                break;
            } else {  //其他（包括但不限于 加减乘除 以及赋值字段）
                outputColumnList.add(pt.getChild(i).getChild(1).getText().toLowerCase());
            }
        }
    }

    /**
     * 循环遍历，取GroupByColumnList或OrderByColumnList
     * @param pt
     */
    public void traversalGroupOrOrder(ASTNode pt){
        TreeSet<String> set = new TreeSet<>();
        for (int i = 0; i < pt.getChildCount(); i++) {
//            System.out.println(pt.getChild(i).toStringTree());
            if (pt.getChild(i).getText().equals(".")) {
                set.add(("-" + pt.getChild(i).getChild(0).getChild(0).getText() + "." +
                        pt.getChild(i).getChild(1).getText() + "-").toLowerCase());
            } else if (pt.getChild(i).getText().toLowerCase().equals("tok_table_or_col")) {
                set.add(pt.getChild(i).getChild(0).getText());
            }
        }
        if (pt.getText().toLowerCase().equals("tok_groupby")) {
            groupByColumnList.addAll(set);
        } else if (pt.getText().toLowerCase().equals("tok_tabsortcolnameasc")) {
            orderByColumnList.addAll(set);
        }
    }

    /**
     * 循环遍历TOK_INSERT
     * @param pt
     */
    public void traversalTokInsert(ASTNode pt) {
        for(int i = 0; i < pt.getChildCount(); i++) {
            if (pt.getChild(i).getText().toLowerCase().equals("tok_select")){
                //循环遍历tok_select
                traversalTokSelect((ASTNode)pt.getChild(i));
            } else if (pt.getChild(i).getText().toLowerCase().equals("tok_groupby")) {
                //循环遍历tok_groupby
                traversalGroupOrOrder((ASTNode)pt.getChild(i));
            } else if (pt.getChild(i).getText().toLowerCase().equals("tok_orderby")) {
                //循环遍历tok_orderby的子树tok_tabsortcolnameasc
                traversalGroupOrOrder((ASTNode)pt.getChild(i).getChild(0));
            }
        }
    }

    /**
     * 使用map的key循环匹配set，将匹配到的替换成map的value
     * @param set
     * @param map
     * @return
     */
    public TreeSet<String> replaceTableName(TreeSet<String> set,HashMap<String, String> map) {
        TreeSet<String> returnSet = new TreeSet<>();
        for (String s : set) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (s.indexOf(entry.getKey()) != -1){
                    s = s.replaceAll(entry.getKey(), entry.getValue());
                }
            }
            returnSet.add(s);
        }
        return returnSet;
    }

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        ASTNode pt = (ASTNode)nd;
//        System.out.println(pt.toStringTree());
        switch(pt.getToken().getType()) {
            case HiveParser.TOK_INSERT:
//                System.out.println(pt.toStringTree());
                if (pt.getChild(0).getChild(0).getText().toLowerCase().equals("tok_tab")) {
                    traversalTokInsert(pt);
                }
                break;

            case HiveParser.TOK_CREATETABLE:    //651
                //当前节点为tok_createtable时，create table操作，添加表名到outputTableList
                this.outputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode)pt.getChild(0)).toLowerCase());
                //将tok_insert节点赋值给pt
                pt = (ASTNode)pt.getChild(2).getChild(1);
                traversalTokInsert(pt);
                break;

            case HiveParser.TOK_TAB:            //847
                //当前节点为tok_tab时，insert操作，添加表名到outputTableList
                this.outputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode)pt.getChild(0)).toLowerCase());
                break;

            case HiveParser.TOK_ALTERTABLE:     //599
                if (pt.getChild(1).getText() == "TOK_ALTERTABLE_RENAME"){
                    this.outputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode)pt.getChild(1).getChild(0)).toLowerCase());
                    break;
                }
                this.outputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode)pt.getChild(0)));

            //FROM和JOIN语句的表名
            case HiveParser.TOK_TABREF:         //876
//                System.out.println(pt.toStringTree());
                if (pt.getChildCount() > 1) {
                    tableMap.put(pt.getChild(1).getText().toLowerCase(),
                            pt.getChild(0).getChild(pt.getChild(0).getChildCount()-1).getText().toLowerCase());
                }
            //TRUNCATE 清空表
            case HiveParser.TOK_TRUNCATETABLE:  //888
            //DROP 删除表
            case HiveParser.TOK_DROPTABLE:      //680
                ASTNode tabTree = (ASTNode)pt.getChild(0);
                String table_name = tabTree.getChildCount() == 1 ?
                        BaseSemanticAnalyzer.getUnescapedName((ASTNode)tabTree.getChild(0)) :
                        BaseSemanticAnalyzer.getUnescapedName((ASTNode)tabTree.getChild(0)) + "." + tabTree.getChild(1);
                this.inputTableList.add(table_name.toLowerCase());
                break;

            //WITH ... AS语句
            case HiveParser.TOK_CTE:
                for (int i = 0 ;i < pt.getChildCount();i ++){
                    this.tempTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode)pt.getChild(i).getChild(1)).toLowerCase());

                }
                break;

            //JOIN语句
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_LEFTSEMIJOIN:
            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_FULLOUTERJOIN:
            case HiveParser.TOK_JOIN:
                //子树小于三，表示没有关联条件的JOIN，产生笛卡尔积
                if (pt.getChildCount() < 3){
                    break;
                } else {
                    //将pt的最后一个子树(关联条件)赋值给pt
                    pt = (ASTNode) pt.getChild(pt.getChildCount() - 1);
                    //遍历多级的AND语句
                    while(pt.getText().toLowerCase().equals("and")){
                        if(pt.getChild(1).getChild(0).getText().equals(".") && pt.getChild(1).getChild(1).getText().equals(".")){
                            joinList.add(("-" + pt.getChild(1).getChild(0).getChild(0).getChild(0).getText() + "." +
                                    pt.getChild(1).getChild(0).getChild(1).getText() + "- = " +
                                    "-" + pt.getChild(1).getChild(1).getChild(0).getChild(0).getText() + "." +
                                    pt.getChild(1).getChild(1).getChild(1).getText() + "-").toLowerCase());
                        } else if (pt.getChild(1).getChild(0).getText().equals(".")) {
                            whereList.add(("-" + pt.getChild(1).getChild(0).getChild(0).getChild(0).getText() + "." +
                                    pt.getChild(1).getChild(0).getChild(1).getText() + "- " +
                                    pt.getChild(1).getText()).toLowerCase() + " " +
                                    pt.getChild(1).getChild(1).getText());
                        } else if (pt.getChild(1).getChild(0).getText().toLowerCase().equals("tok_table_or_col")) {
                            whereList.add((pt.getChild(0).getChild(0).getText() + " " +
                                    pt.getText()).toLowerCase() + " " + pt.getChild(1));
                        }
                        pt = (ASTNode)pt.getChild(0);
                    }
                    //AND遍历到最里层，是两个子树，where循环里只将第二个子树的条件解析出来添加进List，此处解析最里层的第一个子树并添加到List里
                    if(pt.getChild(0).getText().equals(".") && pt.getChild(1).getText().equals(".")){
                        joinList.add(("-" + pt.getChild(0).getChild(0).getChild(0).getText() + "." +
                                pt.getChild(0).getChild(1).getText() + "- = " +
                                "-" + pt.getChild(1).getChild(0).getChild(0).getText() + "." +
                                pt.getChild(1).getChild(1).getText() + "-").toLowerCase());
                    } else if (pt.getChild(0).getText().equals(".")){
                        whereList.add(("-" + pt.getChild(0).getChild(0).getChild(0).getText() + "." +
                                pt.getChild(0).getChild(1).getText() + "- " +
                                pt.getText()).toLowerCase() + " " + pt.getChild(1).getText());
                    } else if (pt.getChild(0).getText().toLowerCase().equals("tok_table_or_col")) {
                        whereList.add((pt.getChild(0).getChild(0).getText() + " " +
                                pt.getText()).toLowerCase() + " " + pt.getChild(1));
                    }
                    break;
                }

            //WHERE语句
            case HiveParser.TOK_WHERE:
                pt = (ASTNode) pt.getChild(0);
                //遍历AND及去最里层的逻辑类似于JOIN的逻辑
                while(pt.getText().toLowerCase().equals("and")){
                    if (pt.getChild(1).getChild(0).getText().equals(".")) {
                        whereList.add(("-" + pt.getChild(1).getChild(0).getChild(0).getChild(0).getText() + "." +
                                pt.getChild(1).getChild(0).getChild(1).getText() + "- " +
                                pt.getChild(1).getText()).toLowerCase() + " " +
                                pt.getChild(1).getChild(1).getText());
                    } else if (pt.getChild(1).getChild(0).getText().toLowerCase().equals("tok_table_or_col")) {
                        whereList.add(pt.getChild(0).getChild(0).getText().toLowerCase() + " " +
                                pt.getText() + " " + pt.getChild(1));
                    }
                    pt = (ASTNode)pt.getChild(0);
                }
                if (pt.getChild(0).getText().equals(".")){
                    whereList.add(("-" + pt.getChild(0).getChild(0).getChild(0).getText() + "." +
                            pt.getChild(0).getChild(1).getText() + "- " +
                            pt.getText()).toLowerCase() + " " + pt.getChild(1).getText());
                } else if (pt.getChild(0).getText().toLowerCase().equals("tok_table_or_col")) {
                    whereList.add((pt.getChild(0).getChild(0).getText() + " " +
                            pt.getText()).toLowerCase() + " " + pt.getChild(1));
                }
                break;
            case HiveParser.TOK_QUERY:
                System.out.println(pt.toStringTree());
                ASTNode tempPt = pt;
                ASTNode tokSelectPt = pt;
                if (pt.getChildCount() > 1) {
                    if (pt.getChild(1).getChildCount() > 1){
                        tokSelectPt = (ASTNode) pt.getChild(1).getChild(1);
                    }
                }
                while (!pt.isNil()) {
//                    System.out.println(pt.toStringTree());
                    if (pt.getText().toLowerCase().equals("tok_tabref")) {
                        if (pt.getChildCount() > 1) {
                            tableMap.put(pt.getChild(1).getText().toLowerCase(),
                                    pt.getChild(0).getChild(pt.getChild(0).getChildCount()-1).getText().toLowerCase());

                            if (pt.getParent().isNil()) {
                                break;
                            } else if (pt.getParent().getChild(1).getChildCount() > 1 &&
                                    pt.getParent().getChild(1).getText().toLowerCase().equals("tok_tabref")) {
                                tableMap.put(pt.getParent().getChild(1).getChild(1).getText().toLowerCase(),
                                        pt.getParent().getChild(1).getChild(0).getChild(pt.getParent().getChild(1).getChild(0).getChildCount()-1).getText().toLowerCase());
                            }
//                            System.out.println(tableMap);
                        }
                        break;
                    } else if (pt.getChildCount() > 0) {
                        pt = (ASTNode) pt.getChild(0);
                    } else {
                        break;
                    }
                }
//                System.out.println(tokSelectPt.toStringTree());
                if (tokSelectPt.getText().toLowerCase().equals("tok_select")) {
                    for (int i = 0; i < tokSelectPt.getChildCount(); i++) {
                        ASTNode tepPt = (ASTNode)tokSelectPt.getChild(i);
                        while(!tepPt.getText().equals(".")){
                            if (tepPt.getText().toLowerCase().equals("tok_function")) {
                                tepPt = (ASTNode)tepPt.getChild(1);
                            } else if(tepPt.getChildCount() > 0){
                                tepPt = (ASTNode)tepPt.getChild(0);
                            } else {
                                break;
                            }
                        }
                        if (tepPt.getText().equals(".")) {
                            String temp = "";
                            for (Map.Entry<String, String> entry : tableMap.entrySet()) {
                                if (entry.getKey().equals(tepPt.getChild(0).getChild(0).getText().toLowerCase())) {
                                    temp = entry.getValue();
                                }
                            }
                            if (!temp.equals("")) {
                                tableAndColumnMap.put(("-" + tepPt.getChild(0).getChild(0).getText() +
                                                "." + tepPt.getChild(1).getText() + "-").toLowerCase(),
                                        (temp + "." + tepPt.getChild(1).getText()).toLowerCase());
                                if (tempPt.getParent().isNil()){
                                    break;
                                } else if (tempPt.getParent().getText().toLowerCase().equals("tok_subquery")) {
                                    tableAndColumnMap.put(("-" + tempPt.getParent().getChild(1).getText() + "." +
                                                    tepPt.getChild(1).getText() + "-").toLowerCase(),
                                            (temp + "." + tepPt.getChild(1).getText()).toLowerCase());
                                }
                            }
                        }
                    }
                }
                break;

            case HiveParser.DOT:
                for (Map.Entry<String, String> entry : tableMap.entrySet()) {
                    if (entry.getKey().toLowerCase().equals(pt.getChild(0).getChild(0).getText().toLowerCase())) {
                        tableAndColumnMap.put(("-" + entry.getKey() + "." + pt.getChild(1).getText() + "-").toLowerCase(),
                                (entry.getValue() + "." + pt.getChild(1).getText()).toLowerCase());
                    }
                }
                break;
        }
        return null;

    }

    public void getLineageInfo(String query) throws ParseException, SemanticException {
        ParseDriver pd = new ParseDriver();

        ASTNode tree;
        for(tree = pd.parse(query); tree.getToken() == null && tree.getChildCount() > 0; tree = (ASTNode)tree.getChild(0)) {
            ;
        }

        this.inputTableList.clear();
        this.outputTableList.clear();
        this.tempTableList.clear();
        this.joinList.clear();
        this.whereList.clear();
        this.outputColumnList.clear();
        this.groupByColumnList.clear();
        this.orderByColumnList.clear();
        this.tableAndColumnMap.clear();
        this.tableMap.clear();

        Map<Rule, NodeProcessor> rules = new LinkedHashMap();
        Dispatcher disp = new DefaultRuleDispatcher(this, rules, (NodeProcessorCtx)null);
        GraphWalker ogw = new DefaultGraphWalker(disp);
        ArrayList<Node> topNodes = new ArrayList();
        topNodes.add(tree);
        ogw.startWalking(topNodes, (HashMap)null);
    }

    /**
     * 根据sql语句，获取这条语句的血缘关系
     * @param sql sql语句
     * 支持:create table 表名 as select ... from 源表
     *      insert into(overwrite) table 表名 select ... from 源表
     *      select ... from 源表A join 源表B
     *      drop table 表名
     *      truncate table 表名
     *      alter table 表名 操作(rename,add columns,add partition,change column ...)
     *      with 表名 as (select ... from 源表) select ... from 表名
     *      以上sql语句及其组合
     * 【注】1) 传入的sql的库名、表名、字段名均不能加'``'，且结尾不能加';'，否则会报错。
     *       2) 不能直接 'select *' , HiveParser 无法解析出语句里没有的字段。
     *       3) 也不能 'select table.*' , HiveParser 无法解析出语句里没有的字段。
     *       4) 注意不能使用相同的别名,否则解析可能会有问题。
     *       5) with语句的条件（关联、过滤条件，聚合、排序字段）存在问题
     * @return lineage 血缘关系
     *      {
     *          "InputTables":[...],
     *          "Joins":[...],
     *          "Wheres":[...],
     *          "OutputTables":["..."],
     *          "OutputColumns":[...],
     *          "GroupByColumns":[...],
     *          "OrderByColumns":[...]
     *      }
     * @throws SemanticException
     * @throws ParseException
     */
    public String getLineage(String sql) throws SemanticException, ParseException {
        getLineageInfo(sql);

        TreeSet<String> inputTables = getInputTableList();
        TreeSet<String> tempTables = getTempTableList();
        TreeSet<String> outputTables = getOutputTableList();
        TreeSet<String> outputColumns = getOutputColumnList();

        HashMap<String, String> tableAndColumnMap = getTableAndColumnMap();

        TreeSet<String> joins = replaceTableName(getJoinList(),tableAndColumnMap);
        TreeSet<String> wheres = replaceTableName(getWhereList(),tableAndColumnMap);
        TreeSet<String> groupByColumns = replaceTableName(getGroupByColumnList(),tableAndColumnMap);
        TreeSet<String> orderByColumns = replaceTableName(getOrderByColumnList(),tableAndColumnMap);

        //输入表inputTables排除掉with语句定义的临时表的别名
        for (String s : tempTables) {
            inputTables.remove(s);
        }

        String input = "[";
        for (String s : inputTables) {
            input += "\"" + s + "\",";
        }
        input = (input.length() == 1) ? "[]" : input.substring(0,input.length()-1) + "]";
        input = "\"InputTables\":" + input;

        String join = "[";
        for (String s : joins) {
            join += "\"" + s + "\",";
        }
        join = (join.length() == 1) ? "[]" : join.substring(0,join.length()-1) + "]";
        join = "\"Joins\":" + join;

        String where = "[";
        for (String s : wheres) {
            where += "\"" + s + "\",";
        }

        String output = "[";
        for (String s : outputTables) {
            output += "\"" + s + "\",";
        }
        output = (output.length() == 1) ? "[]" : output.substring(0,output.length()-1) + "]";
        output = "\"OutputTables\":" + output;
        where = (where.length() == 1) ? "[]" : where.substring(0,where.length()-1) + "]";
        where = "\"Wheres\":" + where;

        String outputColumn = "[";
        for (String s : outputColumns) {
            outputColumn += "\"" + s + "\",";
        }
        outputColumn = (outputColumn.length() == 1) ? "[]" : outputColumn.substring(0,outputColumn.length()-1) + "]";
        outputColumn = "\"OutputColumns\":" + outputColumn;

        String groupByColumn = "[";
        for (String s : groupByColumns) {
            groupByColumn += "\"" + s + "\",";
        }
        groupByColumn = (groupByColumn.length() == 1) ? "[]" : groupByColumn.substring(0,groupByColumn.length()-1) + "]";
        groupByColumn = "\"GroupByColumns\":" + groupByColumn;

        String orderByColumn = "[";
        for (String s : orderByColumns) {
            orderByColumn += "\"" + s + "\",";
        }
        orderByColumn = (orderByColumn.length() == 1) ? "[]" : orderByColumn.substring(0,orderByColumn.length()-1) + "]";
        orderByColumn = "\"OrderByColumns\":" + orderByColumn;

        String lineage = "{" + input + "," + join + "," + where + "," + output + "," + outputColumn + "," + groupByColumn + "," + orderByColumn + "}";

        return lineage;
    }

    public static void main(String[] args) throws SemanticException, ParseException {
        String sql =
//                "INSERT INTO TABLE FZJC_XYKC_JH \n" +
                "CREATE TABLE FZJC_XYKC_JH AS \n" +
                "SELECT \n" +
                "      t.ZB_REQUIRE_CODE, -- 总部采购申请号\n" +
                "      t.ZB_REQUIRE_LINE, -- 总部采购申请行项目\n" +
                "      t.OBJECT_NAME, -- 分标信息\n" +
                "      t.PACKAGE_NAME, -- 分包信息\n" +
                "      la.PLAN_CODE, -- 招标计划编号\n" +
                "      cast(ltp.PROJECT_ID as string) as PROJECT_ID, -- 项目主键\n" +
                "      cast(ba.AGENCY_ID as string) as AGENCY_ID, -- 招标代理机构ID\n" +
                "      mdm.SUPPLIER_CODE, -- 供应商编码\n" +
                "      mdmm.MATERIEL_ID, -- 物料编码\n" +
                "      tt.REQCODE, -- 总部采购申请号\n" +
                "      tt.REQITEM, -- 总部采购申请行项目号\n" +
                "      k.DEPT_CODE, -- 单位编码\n" +
                "      k.ORDERCODE, -- 采购凭证号\n" +
                "      k.ORDERITEM, -- 行项目号\n" +
                "      kk.POID, -- 采购订单编号\n" +
                "      kk.POLINEITEMID, -- 采购订单行项目号\n" +
                "      kk.ORGCODE, -- 网省编码\n" +
                "      kkk.CONTRACT_ID, -- 合同标识符\n" +
                "      kkk.PROCURE_ORDER_ID, -- erp框架协议号（采购订单号）\n" +
                "      t.COMPANY_ID, -- 单位编码\n" +
                "      '订单' as SOURCE, -- 数据来源\n" +
                "      '协议库存' as XYKC, -- 采购组织方式\n" +
                "      '物资类' as WLLB, -- 物料类别\n" +
                "      '' as XMDYDJ, -- 项目电压等级\n" +
                "      substr(ttt.PRJCODE,0,1) as XMLXYJ, -- 项目类型一级\n" +
                "      substr(ttt.PRJCODE,0,2) as XMLXEJ, -- 项目类型二级\n" +
                "      cast(substr(ltp.RESULT_NOTICE,0,10) as date) as RESULT_NOTICE, -- 中标公告发布日期\n" +
                "      cast(from_unixtime(unix_timestamp(t.BID_DATE,'yyyymmdd'),'yyyy-mm-dd') as date) as BID_DATE, -- 中标结果流转合同日期\n" +
                "      '总部直接组织实施' as SSFS, -- 实施方式\n" +
                "      tt.REQQUAN, -- 采购申请数量\n" +
                "      tt.REQPRICE, -- 采购申请单价（元）\n" +
                "      tt.REQQUAN * tt.REQPRICE AS TOTALREQPRICE, -- 估算总价（元）\n" +
                "      cast(k.PRICE as double) as PRICE, -- 订单不含税单价（元）\n" +
                "      cast(k.TOTALPRICE as double) as TOTALPRICE, -- 订单不含税总价（元）\n" +
                "      cast(k.TAXPRICE as double) as TAXPRICE, -- 订单含税单价（元）\n" +
                "      cast(k.TAXTOTALPRICE as double) as TAXTOTALPRICE-- 订单含税总价（元）\n" +
                "FROM \n" +
                "(SELECT \n" +
                "      seab.COMPANY_ID,\n" +
                "      seab.ZB_REQUIRE_CODE,\n" +
                "      seab.ZB_REQUIRE_LINE,\n" +
                "      dac.COMPNAME,\n" +
                "      seab.PLAN_CODE,\n" +
                "      seab.PROVIDER_MDM_NO,\n" +
                "      seab.MATERIAL_CODE,\n" +
                "      seab.OBJECT_NAME,\n" +
                "      seab.PACKAGE_NAME,\n" +
                "      seab.BID_DATE \n" +
                "FROM SGCC_ECP_ADM_BID seab \n" +
                "LEFT JOIN DIMENSION_ALL_COMPCODE dac on dac.COMPCODE=seab.COMPANY_ID\n" +
                ")t\n" +
                "JOIN LA_PLAN la on t.PLAN_CODE=la.PLAN_CODE\n" +
                "and la.PLAN_NAME like'%输变电项目%' and la.PLAN_NAME like '%协议库存%' and la.ACTIONPOLICY='1'\n" +
                "LEFT JOIN LB_T_PROJECT ltp on ltp.plan_id=la.PLAN_ID\n" +
                "LEFT JOIN BID_AGENCY ba on ba.AGENCY_CODE=la.AGENT_CODE\n" +
                "LEFT JOIN MDMVENDORINFO mdm on mdm.SUPPLIER_CODE=t.PROVIDER_MDM_NO\n" +
                "LEFT JOIN MDMMATLMST mdmm on mdmm.MATERIEL_ID=t.MATERIAL_CODE\n" +
                "LEFT JOIN \n" +
                "(SELECT \n" +
                "      mat.REQDEPTCODE,\n" +
                "      mat.REQCODE,\n" +
                "      mat.REQITEM,\n" +
                "      dac.COMPNAME,\n" +
                "      mat.REQWSCODE,\n" +
                "      mat.REQWSITEM,\n" +
                "      mat.REQQUAN,\n" +
                "      mat.REQPRICE \n" +
                "FROM MATPURREQ mat\n" +
                "LEFT JOIN DIMENSION_ALL_COMPCODE dac on dac.COMPCODE=mat.REQDEPTCODE\n" +
                ")tt \n" +
                "on t.COMPNAME=tt.COMPNAME and t.ZB_REQUIRE_CODE=tt.REQCODE and t.ZB_REQUIRE_LINE=tt.REQITEM\n" +
                "LEFT JOIN \n" +
                "(\n" +
                "SELECT \n" +
                "      ec.PLAN_CODE,\n" +
                "      ec.CONTRACT_ID,\n" +
                "      ec.PROVINCE_CODE,\n" +
                "      ec.OBJECT_NAME,\n" +
                "      ec.PACKAGE_NAME,\n" +
                "      ec.PROCURE_ORDER_ID,\n" +
                "      eml.PRO_RELINUM,\n" +
                "      eml.PRO_RENUM,\n" +
                "      dac.COMPNAME \n" +
                "FROM ECP_CONTRACT ec \n" +
                "LEFT JOIN ECP_MATERIAL_LINEITEM eml on ec.CONTRACT_ID=eml.CONTRACT_ID\n" +
                "LEFT JOIN DIMENSION_ALL_COMPCODE dac on dac.COMPCODE=ec.PROVINCE_CODE\n" +
                ") KKK\n" +
                "on tt.COMPNAME=KKK.COMPNAME and tt.REQWSCODE=kkk.PRO_RENUM and tt.REQWSITEM=kkk.PRO_RELINUM\n" +
                "and t.PLAN_CODE = KKK.PLAN_CODE and t.OBJECT_NAME = KKK.OBJECT_NAME and t.PACKAGE_NAME = KKK.PACKAGE_NAME \n" +
                "LEFT JOIN \n" +
                "(SELECT \n" +
                "      matw.PRJCODE,\n" +
                "      matw.DEPT_CODE,\n" +
                "      matw.REQCODE,\n" +
                "      matw.REQITEMCODE,\n" +
                "      dac.COMPNAME,\n" +
                "      matw.ADDCOLUMN10,\n" +
                "      matw.ORDERCODE,\n" +
                "      matw.ORDERITEM \n" +
                "FROM MATWZADSDATABARGAIN matw\n" +
                "LEFT JOIN DIMENSION_ALL_COMPCODE dac on dac.COMPCODE=matw.DEPT_CODE\n" +
                "where matw.ADDCOLUMN10 = '4'\n" +
                ")ttt\n" +
                "ON tt.COMPNAME = ttt.COMPNAME AND tt.REQWSCODE = ttt.REQCODE AND tt.REQWSITEM = ttt.REQITEMCODE AND KKK.PROCURE_ORDER_ID = ttt.ordercode\n" +
                "LEFT JOIN \n" +
                "(SELECT \n" +
                "      matwn.DEPT_CODE,\n" +
                "      matwn.ADDCOLUMN5,\n" +
                "      matwn.ADDCOLUMN6,\n" +
                "      dac.COMPNAME,\n" +
                "      matwn.ADDCOLUMN10,\n" +
                "      matwn.DELETESIGN,\n" +
                "      matwn.ORDERCODE,\n" +
                "      matwn.ORDERITEM,\n" +
                "      matwn.PRICE,\n" +
                "      matwn.TOTALPRICE,\n" +
                "      matwn.TAXPRICE,\n" +
                "      matwn.TAXTOTALPRICE \n" +
                "FROM MATWZADSDATABARGAIN matwn\n" +
                "LEFT JOIN DIMENSION_ALL_COMPCODE dac on dac.COMPCODE=matwn.DEPT_CODE\n" +
                "where matwn.ADDCOLUMN10 = '2' AND matwn.DELETESIGN !='L'\n" +
                ")K\n" +
                "ON ttt.COMPNAME = K.COMPNAME AND ttt.ORDERCODE = K.ADDCOLUMN5 AND ttt.ORDERITEM = K.ADDCOLUMN6\n" +
                "LEFT JOIN \n" +
                "(SELECT \n" +
                "      ep.ORGCODE,\n" +
                "      ep.POID,\n" +
                "      ep.POLINEITEMID,\n" +
                "      dac.COMPNAME \n" +
                "FROM ECP_PURCHASEORDER ep\n" +
                "LEFT JOIN DIMENSION_ALL_COMPCODE dac on dac.COMPCODE=ep.ORGCODE\n" +
                ")KK\n" +
                "ON K.COMPNAME = KK.COMPNAME AND K.ORDERCODE = KK.POID AND K.ORDERITEM = KK.POLINEITEMID";
        String sql1 = "WITH data1 AS (SELECT table1.id,table.age FROM table1 join table on table1.id = table.id WHERE table1.id > 0),\n" +
                "data2 AS (SELECT t2.id,t2.name FROM table2 AS t2 WHERE t2.id > 0),\n" +
                "data3 AS (select num from table3)\n" +
                "insert overwrite table db.test1 \n" +
                "SELECT data1.*, data2.* \n" +
                "FROM data1 JOIN data2 on data1.id = data2.id";
        String sql2 = "FROM (  SELECT p.datekey datekey, p.userid userid, c.clienttype  FROM detail.usersequence_client c JOIN fact.orderpayment p ON p.orderid = c.orderid "
                + " JOIN default.user du ON du.userid = p.userid WHERE p.datekey = 20131118 ) base  INSERT OVERWRITE TABLE test.customer_kpi SELECT base.datekey, "
                + "  base.clienttype, count(distinct base.userid) buyer_count GROUP BY base.datekey, base.clienttype";
        String sql3 = "ALTER TABLE db.invites ADD COLUMNS (new_col2 INT COMMENT 'a comment')";
        String sql4 = "CREATE TABLE FZJC_xykc_ZB as\n" +
                "SELECT \n" +
                "      seab.ZB_REQUIRE_CODE, -- 总部采购申请号\n" +
                "      seab.ZB_REQUIRE_LINE, -- 总部采购申请行项目\n" +
                "      seab.OBJECT_NAME, -- 分标信息\n" +
                "      seab.PACKAGE_NAME, -- 分包信息\n" +
                "      seab.PLAN_CODE, -- 招标批次编号\n" +
                "      seab.COMPANY_ID, -- 单位编码\n" +
                "      la.PLAN_CODE as PLAN_CODE_one, -- 招标计划编号\n" +
                "      mat.REQCODE, -- 总部采购申请号\n" +
                "      mat.REQITEM, -- 总部采购申请行项目号\n" +
                "      t.CONTRACT_ID, -- 合同标识符\n" +
                "      mdmm.MATERIEL_ID, -- 物料编码\n" +
                "      mdmm.MID_CLASS_NAME, -- 物资中类\n" +
                "      seab.PLAN_CODE as PLAN_CODE_two, -- 批次编码\n" +
                "      t.MATERIAL_LINEITEM_ID, -- 主键(合同行项目物料信息表)      \n" +
                "      cast(substr(la.CLOSE_BID_DATE,0,10) as date) as CLOSE_BID_DATE, -- 截标时间\n" +
                "      cast(substr(la.PUB_DATE,0,10) as date) as PUB_DATE, -- 发布公告时间\n" +
                "      cast(substr(t.VALIDATE_FROM,0,10) as date) as VALIDATE_FROM, -- 协议库存有效开始日期\n" +
                "      cast(substr(t.VALIDATE_END,0,10) as date) as VALIDATE_END, -- 协议库存有效截止日期\n" +
                "      cast(substr(seab.CONSIGNMENT_DATE,0,10) as date) as CONSIGNMENT_DATE, -- 中标结果交货日期\n" +
                "      t.TAX_TOTAL_PRICE, -- 协议总金额\n" +
                "      CAST(seab.AMOUNT AS INT) AS AMOUNT, -- 中标数量\n" +
                "      seab.NET_PRICE, -- 中标结果不含税单价（元）\n" +
                "      -- seab.AMOUNT*seab.NET_PRICE, -- 中标结果不含税总价（元）\n" +
                "      seab.PRICE, -- 中标结果含税单价（元）\n" +
                "      seab.TOTAL_PRICE -- 中标结果含税总价（元）\n" +
                "FROM \n" +
                "SGCC_ECP_ADM_BID seab \n" +
                "JOIN LA_PLAN la on la.PLAN_CODE=seab.PLAN_CODE\n" +
                "and la.PLAN_NAME like'%输变电项目%' and la.PLAN_NAME like '%协议库存%' and la.ACTIONPOLICY='1'\n" +
                "LEFT JOIN MATPURREQ mat on seab.COMPANY_ID=mat.REQDEPTCODE and seab.ZB_REQUIRE_CODE=mat.REQCODE and seab.ZB_REQUIRE_LINE=mat.REQITEM\n" +
                "LEFT JOIN \n" +
                "(\n" +
                "SELECT \n" +
                "      ec.CONTRACT_ID,\n" +
                "      ec.PROVINCE_CODE,\n" +
                "      eml.PRO_RELINUM,\n" +
                "      eml.PRO_RENUM,\n" +
                "      ec.CONTRACT_STATUS,\n" +
                "      eml.MATERIAL_LINEITEM_ID,\n" +
                "      ec.VALIDATE_FROM,\n" +
                "      ec.VALIDATE_END,\n" +
                "      eml.TAX_TOTAL_PRICE\n" +
                "FROM ECP_CONTRACT ec \n" +
                "LEFT JOIN \n" +
                "ECP_MATERIAL_LINEITEM eml on ec.CONTRACT_ID=eml.CONTRACT_ID AND ec.CONTRACT_STATUS='5')t \n" +
                "on mat.REQDEPTCODE=t.PROVINCE_CODE and mat.REQWSCODE=t.PRO_RENUM and mat.REQWSITEM=t.PRO_RELINUM\n" +
                "LEFT JOIN MDMMATLMST mdmm on mdmm.MATERIEL_ID=seab.MATERIAL_CODE";
        String sql5 = "insert into table test  \n" +
                "  select \n" +
                "    a.id as code,\n" +
                "    name,\n" +
                "    b.age,\n" +
                "    a.num % b.num as num,\n" +
                "    avg(b.age) over(partition by a.class ) as avgage,\n" +
                "    sum(a.aaa) as abc,\n" +
                "    case when b.age > 18 then 'adult' else 'kid' end as type \n" +
                "  from tablea as a join ( \n" +
                "    select \n" +
                "      b1.id,\n" +
                "      b1.age,\n" +
                "      b2.num \n" +
                "    from tableb as b1 \n" +
                "    left join tablec as b2 \n" +
                "    on b1.id = b2.id) as b \n" +
                "  on a.id = b.id\n" +
                "  where name = 'lin'\n" +
                "  and b.num >= 60\n" +
                "  group by a.id,name,b.age,a.num,b.num order by a.id";
        String sql6 = "select * from a";
        HiveLineageInfo hiveLineageInfo = new HiveLineageInfo();
        System.out.println(hiveLineageInfo.getLineage(sql1));
    }
}
