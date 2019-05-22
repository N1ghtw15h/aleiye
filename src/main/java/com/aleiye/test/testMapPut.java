package com.aleiye.test;

import java.util.HashMap;

/**
 * @PackageName: com.aleiye.test
 * @ClassName: testMapPut
 * @Date: 2019/5/17 14:50
 * @Auther: Linxu
 * @Description: TODO
 */
public class testMapPut {
    public static void main(String[] args) {
        HashMap<String, String> map = new HashMap<>();
        map.put("a","1");
        System.out.println(map);
        map.put("a","2");
        System.out.println(map);
    }
}
