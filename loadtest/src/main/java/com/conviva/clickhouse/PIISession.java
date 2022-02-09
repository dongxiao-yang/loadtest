package com.conviva.clickhouse;

import java.util.ArrayList;
import java.util.Arrays;

public class PIISession {

    public static final char DEFAULT_SEPARATOR = '\t';
    public static final String DEFAULT_LINE_END = "\n";
    public static final String quote = "'";

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

    public String[] tags_key = new String[0];
    public String[] tags_value = new String[0];;

    public PIISession() {
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

    public void setTags_key(String[] tags_key) {
        this.tags_key = tags_key;
    }

    public void setTags_value(String[] tags_value) {
        this.tags_value = tags_value;
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
                ", tags_key=" + Arrays.toString(tags_key) +
                ", tags_value=" + Arrays.toString(tags_value) +
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
                .append(buildClickhouseStrArray(tags_key)).append(DEFAULT_SEPARATOR)
                .append(buildClickhouseStrArray(tags_value))
                .append(DEFAULT_LINE_END);

        return sb.toString();
    }





    static String buildClickhouseStrArray(String[] tags_column) {

        if (tags_column != null && tags_column.length>0)
        {
            StringBuilder sb = new StringBuilder("[");
            for(int i=0 ; i< tags_column.length;i++)
            {
                sb.append(quote).append(tags_column[i]).append(quote);
                if(i!= tags_column.length-1)
                {
                    sb.append(",");
                }
            }
            sb.append("]");
            return sb.toString();
        }
        else
        {
            return "[]";
        }
    }

}
