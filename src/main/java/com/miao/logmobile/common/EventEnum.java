package com.miao.logmobile.common;

public enum  EventEnum {

    LAUNCH(1,"launch event ","e_l"),
    PAGE_VIEW(2,"page event","e_pv"),
    CHARGE_REQUEST(3,"charge request event","e_crt"),
    CHARGE_SUCCESS(4,"charge success event","e_cs"),
    CHARGE_REFUND(5,"charge refund","e_cr"),
    EVENT(6,"event event","e_e");

    private int eventId;
    private String eventComments;
    private String eventName;

    public int getEventId() {
        return eventId;
    }

    public void setEventId(int eventId) {
        this.eventId = eventId;
    }

    public String getEventComments() {
        return eventComments;
    }

    public void setEventComments(String eventComments) {
        this.eventComments = eventComments;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    private EventEnum(int eventId, String eventComment, String eventName) {
        this.eventId = eventId;
        this.eventComments = eventComment;
        this.eventName = eventName;
     }

    public static EventEnum valuesOf(String eventName){

        EventEnum[] eventEnums = EventEnum.values();

         for(EventEnum eventEnum:eventEnums){

             if (eventEnum.getEventName().equals(eventName)){
                 return eventEnum;
             }

         }

        return null;

    }

}
