package com.miao.logmobile.common;

public enum  DateTypeEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    String type;

    DateTypeEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public static DateTypeEnum valueOfType(String type){

        DateTypeEnum[] dateTypeEnums = DateTypeEnum.values();

        for(DateTypeEnum dateTypeEnum:dateTypeEnums){
            if(dateTypeEnum.getType().equals(type)){
                return dateTypeEnum;
            }
        }

        return null;
    }
}
