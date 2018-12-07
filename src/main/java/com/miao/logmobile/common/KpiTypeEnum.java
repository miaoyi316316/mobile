package com.miao.logmobile.common;

public enum  KpiTypeEnum {

    NEW_USER("new user"),
    NEW_ALL_USER("new all user"),
    BROWSE_NEW_USER("browse new user"),
    BROWSE_NEW_ALL_USER("browse new all user"),
    ACTION_NEW_USER("action new user")
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
