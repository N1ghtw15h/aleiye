package com.aleiye.test;

import com.aleiye.hive.tools.HiveLineageInfo;

import java.util.HashMap;
import java.util.TreeSet;

/**
 * @PackageName: com.aleiye.test
 * @ClassName: testReplace
 * @Date: 2019/5/15 15:28
 * @Auther: Linxu
 * @Description: TODO
 */
public class testReplace {
    public static void main(String[] args) {
//        HashMap<String, String> map = new HashMap<>();
//        map.put("-la.plan_code-","la_plan.plan_code");
//        map.put("-seab.plan_code-","sgcc_ecp_adm_bid.plan_code");
//        map.put("-t.plan_code-","la_plan.plan_code");
//        map.put("-tt.plan_code-","sgcc_ecp_adm_bid.plan_code");
//
//        TreeSet<String> set = new TreeSet<>();
//        set.add("-la.plan_code- = -seab.plan_code-");
//        set.add("-t.plan_code- like 'SG18%'");
//        set.add("-t.plan_code- like '%18%'");
//        set.add("-tt.plan_code- like 'SG%'");

        HashMap<String, String> map = new HashMap<String, String>();
        map.put("-la.plan_code-","la_plan.plan.code");
        map.put("-seab.plan_code-","sgcc_ecp_adm_bid.plan_code");
        map.put("-sea.plan_code-","sgcc_ecp_adm.plan_code");
        TreeSet<String> set = new TreeSet<String>();
        set.add("-la.plan_code-=-seab.plan_code-");
        set.add("-la.plan_code-=-sea.plan_code-");
//        set.add("-la.plan_code-=-se.plan_code-");

        for (String s : set) {
            System.out.println(s);
        }
        System.out.println("----------------------------------");

        HiveLineageInfo info = new HiveLineageInfo();
        set = info.replaceTableName(set,map);

        for (String s : set) {
            System.out.println(s);
        }
    }
}
