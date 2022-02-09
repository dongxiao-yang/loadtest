package com.conviva.clickhouse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PIISessionWithMap {
    public static final char DEFAULT_SEPARATOR = '\t';
    public static final String DEFAULT_LINE_END = "\n";

    public int customerId;
    public String clientId;
    public int sessionId;
    public int segmentId;
    public long interval_startTimeMs;

    public int ip=0;
    public String ipv6="";
    public String ipHash="";
    public String ipv6Hash="";
    public String ipType;
    public long latestReceivedTimeMs=0;
    public int isEnded;
    public long firstReceivedTimeMs=0;

    public Map<String,String> tags = new HashMap<>();

    public PIISessionWithMap() {
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public void setSegmentId(int segmentId) {
        this.segmentId = segmentId;
    }

    public void setInterval_startTimeMs(long interval_startTimeMs) {
        this.interval_startTimeMs = interval_startTimeMs;
    }

    public void setIp(int ip) {
        this.ip = ip;
    }

    public void setIpv6(String ipv6) {
        this.ipv6 = ipv6;
    }

    public void setIpHash(String ipHash) {
        this.ipHash = ipHash;
    }

    public void setIpv6Hash(String ipv6Hash) {
        this.ipv6Hash = ipv6Hash;
    }

    public void setIpType(String ipType) {
        this.ipType = ipType;
    }

    public void setLatestReceivedTimeMs(long latestReceivedTimeMs) {
        this.latestReceivedTimeMs = latestReceivedTimeMs;
    }

    public void setIsEnded(int isEnded) {
        this.isEnded = isEnded;
    }

    public void setFirstReceivedTimeMs(long firstReceivedTimeMs) {
        this.firstReceivedTimeMs = firstReceivedTimeMs;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "PIISession{" +
                "customerId=" + customerId +
                ", clientId='" + clientId + '\'' +
                ", sessionId=" + sessionId +
                ", segmentId=" + segmentId +
                ", interval_startTimeMs=" + interval_startTimeMs +
                ", ip=" + ip +
                ", ipv6='" + ipv6 + '\'' +
                ", ipHash='" + ipHash + '\'' +
                ", ipv6Hash='" + ipv6Hash + '\'' +
                ", ipType='" + ipType + '\'' +
                ", latestReceivedTimeMs=" + latestReceivedTimeMs +
                ", isEnded=" + isEnded +
                ", firstReceivedTimeMs=" + firstReceivedTimeMs +
                ", tags" + tags.toString() +
                '}';
    }


    public String toTSV(){

        StringBuilder sb = new StringBuilder();
        sb.append(customerId).append(DEFAULT_SEPARATOR)
                .append(clientId).append(DEFAULT_SEPARATOR)
                .append(sessionId).append(DEFAULT_SEPARATOR)
                .append(segmentId).append(DEFAULT_SEPARATOR)
                .append(interval_startTimeMs).append(DEFAULT_SEPARATOR)
                .append(ip).append(DEFAULT_SEPARATOR)
                .append(ipv6).append(DEFAULT_SEPARATOR)
                .append(ipHash).append(DEFAULT_SEPARATOR)
                .append(ipv6Hash).append(DEFAULT_SEPARATOR)
                .append(ipType).append(DEFAULT_SEPARATOR)
                .append(latestReceivedTimeMs).append(DEFAULT_SEPARATOR)
                .append(isEnded).append(DEFAULT_SEPARATOR)
                .append(firstReceivedTimeMs).append(DEFAULT_SEPARATOR)
                .append(tags.toString().replaceAll("=",":"))
                .append(DEFAULT_LINE_END);

        return sb.toString();
    }





//    static String buildClickhouseStrMap(HashMap<String,String> tags) {
//
//        if (tags != null && tags.size()>0)
//        {
//            StringBuilder sb = new StringBuilder("{");
//            for(int i=0 ; i< tags.size();i++)
//            {
//                sb.append(quote).append(tags[).append(quote);
//                if(i!= tags_column.length-1)
//                {
//                    sb.append(",");
//                }
//            }
//            sb.append("}");
//            return sb.toString();
//        }
//        else
//        {
//            return "{}";
//        }
//    }

}
