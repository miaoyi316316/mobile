package com.miao.logmobile.etl.util;

import com.miao.logmobile.etl.ip.IPSeeker;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

public class IpUtil {

    private static final String DEFAULT = "unknown";

    private static Map<String,String> regionInfo  = null;

//    static {
//        regionInfo = new HashMap<String, String>();
//
//        regionInfo.put("内蒙古",)
//    }

    public static IpInfo getIpInfo(String ip){

        if(ip==null||ip.length()==0){
            return null;
        }

        String info = IPSeeker.getInstance().getCountry(ip);
        IpInfo ipInfo = null;



        String country = "中国";
        String province = DEFAULT;
        String city = DEFAULT;

        if(info!=null&&!info.trim().equals("")) {
            if (info.equals("局域网")) {

                province = "北京市";
                city = "昌平区";
            } else if (info.contains("省")) {

                province = info.substring(0, info.indexOf("省") + 1);
                if(info.contains("市"))
                    city = info.substring(info.indexOf("省") + 1, info.indexOf("市") + 1);
            } else {
                String info2 = info.substring(0, 2);
                switch (info2){
                    case "内蒙":{
                        province = info2 + "古";
                        if (info.length() > 3) {
                            String suffx = info.substring(3);
                            int index = suffx.indexOf("市");
                            if(index>0){
                                city = suffx.substring(0, index + 1);
                            }
                        }
                         break;
                    }
                    case "新疆":{
                        province = info2 + "维吾尔族自治区";
                        if (info.length() > 3) {
                            String suffx = info.substring(3);
                            int index = suffx.indexOf("市");
                            if(index>0){
                                city = suffx.substring(0, index + 1);
                            }
                        }
                        break;
                    }
                    case "宁夏":{
                        province = info2 + "回族自治区";
                        if (info.length() > 3) {
                            String suffx = info.substring(3);
                            int index = suffx.indexOf("市");
                            if(index>0){
                                city = suffx.substring(0, index + 1);
                            }
                        }
                        break;
                    }
                    case "广西":{
                        province = info2 + "壮族自治区";
                        if (info.length() > 3) {
                            String suffx = info.substring(3);
                            int index = suffx.indexOf("市");
                            if(index>0){
                                city = suffx.substring(0, index + 1);
                            }
                        }
                        break;
                    }
                    case "西藏":{
                        province = info2 + "自治区";
                        if (info.length() > 3) {
                            String suffx = info.substring(3);
                            int index = suffx.indexOf("市");
                            if(index>0){
                                city = suffx.substring(0, index + 1);
                            }
                        }
                        break;
                    }
                    case "香港":{
                        province = info2 + "特别行政区";
                        if (info.length() > 3) {
                            String suffx = info.substring(3);
                            int index = suffx.indexOf("市");
                            if(index>0){
                                city = suffx.substring(0, index + 1);
                            }
                        }
                        break;
                    }
                    case "澳门":{
                        province = info2 + "特别行政区";
                        if (info.length() > 3) {
                            String suffx = info.substring(3);
                            int index = suffx.indexOf("市");
                            if(index>0){
                                city = suffx.substring(0, index + 1);
                            }
                        }
                        break;
                    }
                    case "北京":
                    case "上海":
                    case "重庆":
                    case "天津":
                    {
                        province = info2+"市";
                        if(info.length()>3) {
                            String suffix = info.substring(3);
                            int indexArea = suffix.indexOf("区");
                            int indexCouty = suffix.indexOf("县");
                            if (indexArea>0){
                                String area = suffix.substring(0, indexArea + 1);
                                char areaPrefix = suffix.charAt(indexArea-1);

                                if(areaPrefix!='小'&&areaPrefix!='校'&&areaPrefix!='军'){
                                    city = area;
                                }
                            }else if(indexCouty>0){
                                city = suffix.substring(0, indexCouty + 1);
                            }

                        }
                        break;
                    }

                }



            }
            if(province.equals(DEFAULT)){
                country = info;
            }
            ipInfo = new IpInfo();
            ipInfo.setCountry(country);
            ipInfo.setProvince(province);
            ipInfo.setCity(city);
        }



        return ipInfo;
    }

    public static class IpInfo{

        private static final String DEFAULT_REGION = "UNKNOWN";

        private String country = DEFAULT_REGION;

        private String province = DEFAULT_REGION;

        private String city = DEFAULT_REGION;

        public static String getDefaultRegion() {
            return DEFAULT_REGION;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return country+","+province+","+city;
        }
    }


    public static void main(String[] args) {

//      System.out.println(IPSeeker.getInstance().getCountry("106.38.62.154"));

        String ss = "Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F46.0.2490.71%20Safari%2F537.36";
        try {
            String result = URLEncoder.encode(ss, "utf-8");
            System.out.println(result+"\n");
            System.out.println(UserAgentUtil.getUserAgentInfo(result));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

    }
}
