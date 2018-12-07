package com.miao.logmobile.etl.etlDimension;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogDimension implements Writable {


    private String log_ip;
    private String log_server_time;
    private String log_country;
    private String log_province;
    private String log_city;
    private String log_event;
    private String log_version;
    private String log_platform;
    private String log_sdk;
    private String log_browse_resolution;
    private String log_user_agent;
    private String log_user_id;
    private String log_language;
    private String log_member_id;
    private String log_session_id;
    private String log_client_time;
    private String log_current_url;
    private String log_prefix_url;
    private String log_title;
    private String log_event_category;
    private String log_event_action;
    private String log_event_key_value;
    private String log_event_duration;
    private String log_order_id;
    private String log_order_name;
    private String log_currency_amount;
    private String log_currency_type;
    private String log_payment_type;
    private String log_browse_name;
    private String log_browse_version;
    private String log_os_name;
    private String log_os_version;

    public String getLog_browse_name() {
        return log_browse_name;
    }

    public void setLog_browse_name(String log_browse_name) {
        this.log_browse_name = log_browse_name;
    }

    public String getLog_browse_version() {
        return log_browse_version;
    }

    public void setLog_browse_version(String log_browse_version) {
        this.log_browse_version = log_browse_version;
    }

    public String getLog_os_name() {
        return log_os_name;
    }

    public void setLog_os_name(String log_os_name) {
        this.log_os_name = log_os_name;
    }

    public String getLog_os_version() {
        return log_os_version;
    }

    public void setLog_os_version(String log_os_version) {
        this.log_os_version = log_os_version;
    }

    public String getLog_event() {
        return log_event;
    }

    public void setLog_event(String log_event) {
        this.log_event = log_event;
    }

    public String getLog_version() {
        return log_version;
    }

    public void setLog_version(String log_version) {
        this.log_version = log_version;
    }

    public String getLog_platform() {
        return log_platform;
    }

    public void setLog_platform(String log_platform) {
        this.log_platform = log_platform;
    }

    public String getLog_sdk() {
        return log_sdk;
    }

    public void setLog_sdk(String log_sdk) {
        this.log_sdk = log_sdk;
    }

    public String getLog_browse_resolution() {
        return log_browse_resolution;
    }

    public void setLog_browse_resolution(String log_browse_resolution) {
        this.log_browse_resolution = log_browse_resolution;
    }

    public String getLog_user_agent() {
        return log_user_agent;
    }

    public void setLog_user_agent(String log_user_agent) {
        this.log_user_agent = log_user_agent;
    }

    public String getLog_user_id() {
        return log_user_id;
    }

    public void setLog_user_id(String log_user_id) {
        this.log_user_id = log_user_id;
    }

    public String getLog_language() {
        return log_language;
    }

    public void setLog_language(String log_language) {
        this.log_language = log_language;
    }

    public String getLog_member_id() {
        return log_member_id;
    }

    public void setLog_member_id(String log_member_id) {
        this.log_member_id = log_member_id;
    }

    public String getLog_session_id() {
        return log_session_id;
    }

    public void setLog_session_id(String log_session_id) {
        this.log_session_id = log_session_id;
    }

    public String getLog_client_time() {
        return log_client_time;
    }

    public void setLog_client_time(String log_client_time) {
        this.log_client_time = log_client_time;
    }

    public String getLog_current_url() {
        return log_current_url;
    }

    public void setLog_current_url(String log_current_url) {
        this.log_current_url = log_current_url;
    }

    public String getLog_prefix_url() {
        return log_prefix_url;
    }

    public void setLog_prefix_url(String log_prefix_url) {
        this.log_prefix_url = log_prefix_url;
    }

    public String getLog_title() {
        return log_title;
    }

    public void setLog_title(String log_title) {
        this.log_title = log_title;
    }

    public String getLog_event_category() {
        return log_event_category;
    }

    public void setLog_event_category(String log_event_category) {
        this.log_event_category = log_event_category;
    }

    public String getLog_event_action() {
        return log_event_action;
    }

    public void setLog_event_action(String log_event_action) {
        this.log_event_action = log_event_action;
    }

    public String getLog_event_key_value() {
        return log_event_key_value;
    }

    public void setLog_event_key_value(String log_event_key_value) {
        this.log_event_key_value = log_event_key_value;
    }

    public String getLog_event_duration() {
        return log_event_duration;
    }

    public void setLog_event_duration(String log_event_duration) {
        this.log_event_duration = log_event_duration;
    }

    public String getLog_order_id() {
        return log_order_id;
    }

    public void setLog_order_id(String log_order_id) {
        this.log_order_id = log_order_id;
    }

    public String getLog_order_name() {
        return log_order_name;
    }

    public void setLog_order_name(String log_order_name) {
        this.log_order_name = log_order_name;
    }

    public String getLog_currency_amount() {
        return log_currency_amount;
    }

    public void setLog_currency_amount(String log_currency_amount) {
        this.log_currency_amount = log_currency_amount;
    }

    public String getLog_currency_type() {
        return log_currency_type;
    }

    public void setLog_currency_type(String log_currency_type) {
        this.log_currency_type = log_currency_type;
    }

    public String getLog_payment_type() {
        return log_payment_type;
    }

    public void setLog_payment_type(String log_payment_type) {
        this.log_payment_type = log_payment_type;
    }

    public String getLog_ip() {
        return log_ip;
    }

    public void setLog_ip(String log_ip) {
        this.log_ip = log_ip;
    }

    public String getLog_server_time() {
        return log_server_time;
    }

    public void setLog_server_time(String log_server_time) {
        this.log_server_time = log_server_time;
    }

    public String getLog_country() {
        return log_country;
    }

    public void setLog_country(String log_country) {
        this.log_country = log_country;
    }

    public String getLog_province() {
        return log_province;
    }

    public void setLog_province(String log_province) {
        this.log_province = log_province;
    }

    public String getLog_city() {
        return log_city;
    }

    public void setLog_city(String log_city) {
        this.log_city = log_city;
    }

    @Override
    public String toString() {
        return log_ip + "\u0001" + log_server_time + "\u0001" + log_country + "\u0001" + log_province + "\u0001" + log_city + "\u0001" + log_event + "\u0001" + log_version + "\u0001" + log_platform + "\u0001" + log_sdk + "\u0001" + log_browse_name +"\u0001"+
                log_browse_version+"\u0001"+log_os_name+"\u0001"+log_os_version+"\u0001"+log_browse_resolution + "\u0001" +
                log_user_agent + "\u0001" + log_user_id + "\u0001" + log_language + "\u0001" + log_member_id + "\u0001" + log_session_id + "\u0001" + log_client_time + "\u0001" + log_current_url + "\u0001" + log_prefix_url + "\u0001" +
                log_title + "\u0001" + log_event_category + "\u0001" + log_event_action + "\u0001" + log_event_key_value + "\u0001" + log_event_duration + "\u0001" + log_order_id + "\u0001" + log_order_name + "\u0001" + log_currency_amount + "\u0001" +
                log_currency_type + "\u0001" + log_payment_type ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(log_event);
        dataOutput.writeUTF(log_version);
        dataOutput.writeUTF(log_platform);
        dataOutput.writeUTF(log_sdk);
        dataOutput.writeUTF(log_browse_resolution);
        dataOutput.writeUTF(log_user_agent);
        dataOutput.writeUTF(log_user_id);
        dataOutput.writeUTF(log_language);
        dataOutput.writeUTF(log_member_id);
        dataOutput.writeUTF(log_session_id);
        dataOutput.writeUTF(log_client_time);
        dataOutput.writeUTF(log_current_url);
        dataOutput.writeUTF(log_prefix_url);
        dataOutput.writeUTF(log_title);
        dataOutput.writeUTF(log_event_category);
        dataOutput.writeUTF(log_event_action);
        dataOutput.writeUTF(log_event_key_value);
        dataOutput.writeUTF(log_event_duration);
        dataOutput.writeUTF(log_order_id);
        dataOutput.writeUTF(log_order_name);
        dataOutput.writeUTF(log_currency_amount);
        dataOutput.writeUTF(log_currency_type);
        dataOutput.writeUTF(log_payment_type);
        dataOutput.writeUTF(log_ip);
        dataOutput.writeUTF(log_server_time);
        dataOutput.writeUTF(log_country);
        dataOutput.writeUTF(log_province);
        dataOutput.writeUTF(log_city);
        dataOutput.writeUTF(log_browse_name);
        dataOutput.writeUTF(log_browse_version);
        dataOutput.writeUTF(log_os_name);
        dataOutput.writeUTF(log_os_version);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.log_event = dataInput.readUTF();
        this.log_version = dataInput.readUTF();
        this.log_platform = dataInput.readUTF();
        this.log_sdk = dataInput.readUTF();
        this.log_browse_resolution = dataInput.readUTF();
        this.log_user_agent = dataInput.readUTF();
        this.log_user_id = dataInput.readUTF();
        this.log_language = dataInput.readUTF();
        this.log_member_id = dataInput.readUTF();
        this.log_session_id = dataInput.readUTF();
        this.log_client_time = dataInput.readUTF();
        this.log_current_url = dataInput.readUTF();
        this.log_prefix_url = dataInput.readUTF();
        this.log_title = dataInput.readUTF();
        this.log_event_category = dataInput.readUTF();
        this.log_event_action = dataInput.readUTF();
        this.log_event_key_value = dataInput.readUTF();
        this.log_event_duration = dataInput.readUTF();
        this.log_order_id = dataInput.readUTF();
        this.log_order_name = dataInput.readUTF();
        this.log_currency_amount = dataInput.readUTF();
        this.log_currency_type = dataInput.readUTF();
        this.log_payment_type = dataInput.readUTF();
        this.log_ip = dataInput.readUTF();
        this.log_server_time = dataInput.readUTF();
        this.log_country = dataInput.readUTF();
        this.log_province = dataInput.readUTF();
        this.log_city = dataInput.readUTF();
        this.log_browse_name = dataInput.readUTF();
        this.log_browse_version = dataInput.readUTF();
        this.log_os_name = dataInput.readUTF();
        this.log_os_version = dataInput.readUTF();

    }
}
