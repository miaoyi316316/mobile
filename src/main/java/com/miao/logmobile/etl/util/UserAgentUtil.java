package com.miao.logmobile.etl.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

public class UserAgentUtil {


    private static Logger logger = Logger.getLogger(UserAgentUtil.class);

    private static UASparser uaSparser;

    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.warn(e);
        }
    }


    public static UserAgentInfo getUserAgentInfo(String userAgent){
        UserAgentInfo info = null;

        if(StringUtils.isEmpty(userAgent)){

            return info;
        }
        cz.mallat.uasparser.UserAgentInfo userAgentInfo = null;
        try {
            userAgentInfo = uaSparser.parse(userAgent);
        } catch (IOException e) {
            logger.warn(e);
        }

        info = new UserAgentInfo();

        info.setBrowseName(userAgentInfo.getUaFamily());

        info.setBrowseVersion(userAgentInfo.getBrowserVersionInfo());

        info.setOsName(userAgentInfo.getOsFamily());

        info.setOsVersion(userAgentInfo.getOsName());

        return info;
    }

    public static class UserAgentInfo{

        private String browseName;

        private String browseVersion;

        private String osName;

        private String osVersion;

        public String getBrowseName() {
            return browseName;
        }

        public void setBrowseName(String browseName) {
            this.browseName = browseName;
        }

        public String getBrowseVersion() {
            return browseVersion;
        }

        public void setBrowseVersion(String browseVersion) {
            this.browseVersion = browseVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return browseName +","+ browseVersion+"," + osName+","+ osVersion;
        }
    }


}
