package com.aleiye.common.tools;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @PackageName: com.aleiye.common.tools
 * @ClassName: TimeTools
 * @Date: 2019/4/29 15:28
 * @Auther: Linxu
 * @Description: 时间工具类
 */
public class TimeTools {
    /**
     * 将时间戳转换为时间
     * @param timeStamp 10位或13位的时间戳
     * @return time “yyyy-MM-dd HH:mm:ss” 时间格式
     */
    public static String stampToTime(String timeStamp){
        if (timeStamp.length() == 10){
            timeStamp += "000";
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(timeStamp);
        Date date = new Date(lt);
        String time = simpleDateFormat.format(date);
        return time;
    }

    /**
     * 将时间转换为时间戳
     * @param time “yyyy-MM-dd HH:mm:ss”时间格式 或 “yyyy-MM-dd”日期格式
     * @return timeStamp 13位的时间戳
     * @throws ParseException
     */
    public static String timeToStamp(String time) throws ParseException {
        if (time.length() == 10){
            time += " 00:00:00";
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(time);
        long ts = date.getTime();
        String timeStamp = String.valueOf(ts);
        return timeStamp;
    }

    /**
     * 将时间转换为时间戳
     * @param time “yyyy-MM-dd HH:mm:ss”时间格式 或 “yyyy-MM-dd”日期格式
     * @param num 返回时间戳位数
     * @return timeStamp 时间戳
     * @throws ParseException
     */
    public static String timeToStamp(String time,int num) throws ParseException {
        if (time.length() == 10){
            time += " 00:00:00";
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(time);
        long ts = date.getTime();
        String timeStamp = String.valueOf(ts);
        return timeStamp.substring(0,num);
    }

    public static void main(String[] args) throws ParseException {
//        System.out.println("13位时间戳转时间：" + stampToTime("1555574761973"));
//        System.out.println("10位时间戳转时间：" + stampToTime("1555574761"));

//        System.out.println("标准时间格式转13位时间戳：" + timeToStamp("2019-04-18 16:06:01"));
//        System.out.println("标准日期格式转10位时间戳：" + timeToStamp("2019-04-18",10));
    }
}
