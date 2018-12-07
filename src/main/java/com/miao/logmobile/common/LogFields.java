package com.miao.logmobile.common;

public class LogFields {


    //分隔符
    public static final String LOG_SEPARATOR = "\\^A";

    //浏览器参数
    public static final String LOG_EVENT = "en";
    public static final String LOG_VERSION = "ver";
    public static final String LOG_PLATFORM = "pl";
    public static final String LOG_SDK = "sdk";
    public static final String LOG_BROWSE_RESOLUTION = "b_rst";
    public static final String LOG_USER_AGENT = "b_iev";

    //用户参数
    public static final String LOG_USER_ID = "u_ud";
    public static final String LOG_LANGUAGE = "l";
    public static final String LOG_MEMBER_ID = "u_mid";
    public static final String LOG_SESSION_ID = "u_sd";
    public static final String LOG_CLIENT_TIME = "c_time";
    public static final String LOG_CURRENT_URL = "p_url";
    public static final String LOG_PREFIX_URL = "p_ref";
    public static final String LOG_TITLE = "tt";

    //事件字段
    public static final String LOG_EVENT_CATEGORY = "ca";
    public static final String LOG_EVENT_ACTION = "ac";
    public static final String LOG_EVENT_KEY_VALUE = "kv_*";
    public static final String LOG_EVENT_DURATION = "du";

    //订单字段
    public static final String LOG_ORDER_ID = "oid";
    public static final String LOG_ORDER_NAME = "on";
    public static final String LOG_CURRENCY_AMOUNT = "cua";
    public static final String LOG_CURRENCY_TYPE = "cut";
    public static final String LOG_PAYMENT_TYPE = "pt";

    public static final String LOG_IP = "ip";
    public static final String LOG_SERVER_TIME = "s_time";

    //ip解析后的信息
    public static final String LOG_COUNTRY = "country";
    public static final String LOG_PROVINCE = "province";
    public static final String LOG_CITY = "city";

    //userAgent解析信息
    public static final String LOG_BROWSE_NAME = "browse_name";
    public static final String LOG_BROWSE_VERSION = "browse_version";
    public static final String LOG_OS_NAME = "os_name";
    public static final String LOG_OS_VERSION = "os_version";

}
