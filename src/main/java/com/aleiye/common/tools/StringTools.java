package com.aleiye.common.tools;

import java.io.UnsupportedEncodingException;

/**
 * @PackageName: com.aleiye.common.tools
 * @ClassName: StringTools
 * @Date: 2019/4/30 11:08
 * @Auther: Linxu
 * @Description: TODO
 */
public class StringTools {
    public static void main(String[] args) throws UnsupportedEncodingException {
        String s = haxStrToChineseString("\\xe9\\xa3\\x9e");
        System.out.println(s);
    }

    /**
     * 16进制字符串转化成UTF-8汉字
     * @param haxStr
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String haxStrToChineseString(String haxStr) throws UnsupportedEncodingException {
        String[] split = (haxStr.replaceAll("\\\\x", "-")).split("-");
        byte[] bytes = new byte[split.length-1];
        for (int i = 1; i < split.length; i++) {
            int i1 = Integer.parseInt(split[i], 16);
            byte b = (byte)i1;
            bytes[i-1] = b;
        }
        String s = new String(bytes,"UTF-8");
        return s;
    }
}
