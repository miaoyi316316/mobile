package com.miao.logmobile.parser;

public class Test {

    public static void main(String[] args) {

        String[] test = "a\u0001b".split("\\u0001");

        for(String t:test){
            System.out.println(t);
        }


    }
}
