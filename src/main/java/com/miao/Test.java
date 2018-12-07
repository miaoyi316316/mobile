package com.miao;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Test {

    private static final int MAX_SIZE = 5000;


    public List readFile2List(String path){

        List<String> ipinfos = new ArrayList<>();

        BufferedReader bfr = null;

        try {
            bfr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
            String line = null;

            while ((line = bfr.readLine())!=null){

                String[] info = line.split(",");
                String startIp = info[0];
                String stopIp = info[1];
                String code = info[2];
                ipinfos.add(startIp + ":" + code);
                ipinfos.add(stopIp + ":" + code);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        return ipinfos;

    }


    public String getCodeByIp(List<String> ipinfos,String ip,int low,int high){


        if(low+1==high){
            return ipinfos.get(high).split(":")[1];
        }

        int mid = low + ((high - low) >> 1);

        String getIp = ipinfos.get(mid).split(":")[0];


        if(getIp.equals(ip)){
            return ipinfos.get(mid).split(":")[1];
        }

        if(low==high){
            return null;
        }

        if(ip.compareTo(getIp)<0){
            return getCodeByIp(ipinfos, ip, low, mid);
        }else if(ip.compareTo(getIp)>0){
            return getCodeByIp(ipinfos, ip, mid,high);
        }

        return null;


    }

    public static void main(String[] args) {
        Test test = new Test();



        List<String> list = test.readFile2List("E:\\hadoop组件\\iprule");




        System.out.println(test.getCodeByIp(list,"212.228.0.1",0,list.size()-1));
    }
}
