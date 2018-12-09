package com.miao.logmobile.common;

public enum  KpiTypeEnum {

    NEW_USER("new_user"),
    NEW_ALL_USER("new_all_user"),
    BROWSE_NEW_USER("browse_new_user"),
    BROWSE_NEW_ALL_USER("browse_new_all_user"),
    ACTION_USER("action_user"),
    ACTION_BROWSE_USER("action_browse_user"),
    NEW_MEMBER("new_member"),
    BROWSE_NEW_MEMBER("browse_new_member"),
    BROWSE_NEW_ALL_MEMBER("browse_new_all_member"),
    NEW_ALL_MEMBER("new_all_member"),
    ACTION_MEMBER("action_member"),
    BROWSE_ACTION_MEMBER("browse_action_member"),
    SESSION("session"),
    BROWSE_SESSION("browse_session"),
    BRWOSE_PV("browse_pv"),
    HOURLY_ACTIVE_USER("hourly_active_user"),
    HOURLY_SESSIONS("hourly_sessions"),
    HOURLY_SESSIONS_LENGTH("hourly_sessions_length"),
    LOCATION_ACTIVE_SESSION_LEAP("location_active_session_leap")
    ;

    private String kpiType;

    public String getKpiType() {
        return kpiType;
    }

    public void setKpiType(String kpiType) {
        this.kpiType = kpiType;
    }

    private KpiTypeEnum(String kpiType) {
        this.kpiType = kpiType;
    }

    public static KpiTypeEnum valueOfKpiType(String kpiType){

        KpiTypeEnum[] kpiTypeEnums = KpiTypeEnum.values();

        for(KpiTypeEnum kpiTypeEnum:kpiTypeEnums){
            if(kpiTypeEnum.getKpiType().equals(kpiType)){
                return kpiTypeEnum;
            }
        }

        return null;
    }
}
