package com.miao.logmobile.timeTransform;

import com.miao.logmobile.common.DateTypeEnum;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间工具类
 */
public class TimeTransform {


    private static final String DEFAULT_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static SimpleDateFormat simpleDateFormat;

    private static String getDateInfoValue = null;
    /**
     * 按照当前时间 获取昨天的日期  yyyy-MM-dd
     * @return
     */
    public static String getYestarday(){

        Calendar calendar = Calendar.getInstance();

        calendar.add(Calendar.DATE, -1);

        Date date = calendar.getTime();


        return long2String(date.getTime(), "yyyy-MM-dd");
    }

    public static long getYesterDay(long timestamp){
        return 0;
    }

    /**
     * 将指定时间戳转化为String,指定格式
     * @param timestamp
     * @param pattern
     * @return
     */
    public static String long2String(long timestamp,String pattern){

        simpleDateFormat = new SimpleDateFormat(pattern);

        String result = simpleDateFormat.format(new Date(timestamp));

        return result;
    }

    /**
     * 将指定时间戳转化为String 使用默认格式
     * @param timestamp
     * @return
     */
    public static String long2String(long timestamp){
        return long2String(timestamp, DEFAULT_PATTERN);
    }

    /**
     * 将指定日期转换为long格式
     * @param date
     * @return
     */
    public static long String2long(String date){

        return String2long(date, DEFAULT_PATTERN);
    }

    /**
     * 将指定日期转化为long，指定解析格式
     * @param date
     * @param pattern
     * @return
     */
    public static long String2long(String date,String pattern){
        simpleDateFormat = new SimpleDateFormat(pattern);
        long result = 0;

        try {
            result = simpleDateFormat.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 根据传进来的枚举不同对时间戳进行解析
     * @param timestamp
     * @param type
     * @return
     */
    public static int getDateInfo(long timestamp, DateTypeEnum type) {



        getDateInfoValue = long2String(timestamp);

        int year = 0;
        int season = 0;
        int month = 0;
        int week = 0;
        int day = 0;

        if(type==null){
            throw new RuntimeException("找不到时间维度类型");
        }
        switch (type){
            //获得指定时间戳的年
            case YEAR:{
                year = Integer.valueOf(getDateInfoValue.substring(0,4));
                return year;
            }
            //获得指定时间戳对应的季度
            case SEASON:{

                int index = getDateInfoValue.indexOf("-");
                if(index>0)
                    month = Integer.valueOf(getDateInfoValue.substring(index+1, index + 3));
                else {
                    throw new RuntimeException("月解析异常");
                }
                return (month-1)/3+1;
            }
            //获得指定时间戳的月份
            case MONTH:{
                int index = getDateInfoValue.indexOf("-");

                month = Integer.valueOf(getDateInfoValue.substring(index+1, index + 3));

                return month;
            }
            //获得指定时间戳是本年的第几周
            case WEEK:{
                Calendar calendar = Calendar.getInstance();
                calendar.clear();
                calendar.setTime(new Date(timestamp));
                calendar.setFirstDayOfWeek(Calendar.MONDAY);
                week = calendar.get(Calendar.WEEK_OF_YEAR);
                return week;
            }
            //获得指定时间戳的day
            case DAY:{
                day = Integer.valueOf(getDateInfoValue.substring(8, 10));
                return day;
            }

        }

        return 0;
    }

    /**
     * 返回当前时间戳指定日期所在周的第一天，也就是该周的周一
     * @param timestamp
     * @return
     */
    public static int getFirstDayOfWeek(long timestamp){

        Date date = new Date(timestamp);

        Calendar calendar = Calendar.getInstance();

        calendar.setFirstDayOfWeek(Calendar.MONDAY);

        calendar.setTime(date);
        //calendar.get(calendar.DAY_OF_WEEK)-1得到当天是周几，通过当前的天日期减去前面的 再减1就是
        //这周的第一天  比如  2018-12-05  -3 -1 = 2018-12-03

        return getDateInfo(timestamp,DateTypeEnum.DAY)-(calendar.get(calendar.DAY_OF_WEEK)-2);

    }



}
