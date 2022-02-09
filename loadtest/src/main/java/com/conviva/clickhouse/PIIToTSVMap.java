package com.conviva.clickhouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PIIToTSVMap {
    private static final Logger logger = Logger
            .getLogger(PIIToTSVMap.class);

    public static final char DEFAULT_SEPARATOR = '\t';
    public static final String DEFAULT_LINE_END = "\n";
    public static final String quote = "'";


    public static void main(String[] args) {

//        String jsonfilename = "/Users/dyang/part-00002-64edcea8-84f4-4264-8c2f-55bc2237fe56.c000.txt";
        String jsonfilename = "/Users/dyang/Documents/workspace/loadtest/loadtest/20220116_hour";

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            LineNumberReader lineReader = new LineNumberReader(new FileReader(jsonfilename));

            String lineText = null;

            StringBuilder sb = new StringBuilder();
            sb.append("customerId").append(DEFAULT_SEPARATOR)
                    .append("clientId").append(DEFAULT_SEPARATOR)
                    .append("sessionId").append(DEFAULT_SEPARATOR)
                    .append("segmentId").append(DEFAULT_SEPARATOR)
                    .append("interval_startTimeMs").append(DEFAULT_SEPARATOR)
                    .append("ip").append(DEFAULT_SEPARATOR)
                    .append("ipv6").append(DEFAULT_SEPARATOR)
                    .append("ipHash").append(DEFAULT_SEPARATOR)
                    .append("ipv6Hash").append(DEFAULT_SEPARATOR)
                    .append("ipType").append(DEFAULT_SEPARATOR)
                    .append("latestReceivedTimeMs").append(DEFAULT_SEPARATOR)
                    .append("isEnded").append(DEFAULT_SEPARATOR)
                    .append("firstReceivedTimeMs").append(DEFAULT_SEPARATOR)
                    .append("tags")
                    .append(DEFAULT_LINE_END);

            String head = sb.toString();


            PrintWriter writer = new PrintWriter(new File("PII_map_20220116.tsv"));

            writer.write(head);

            while ((lineText = lineReader.readLine()) != null) {

                JsonNode jsonTree = new ObjectMapper().readTree(lineText);
                CsvMapper csvMapper = new CsvMapper();

                PIISessionWithMap ps = new PIISessionWithMap();


                Map<String, String> tags = new HashMap<>();

                for (Iterator<Map.Entry<String, JsonNode>> it = jsonTree.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> jn = it.next();

                    String nodeName = jn.getKey();
                    String valueStr = jn.getValue().asText();


                    if ("customerId".equals(nodeName)) {
                        ps.setCustomerId(Integer.parseInt(valueStr));
                    } else if ("clientId".equals(nodeName)) {
                        ps.setClientId(valueStr);
                    } else if ("sessionId".equals(nodeName)) {
                        ps.setSessionId(Integer.parseInt(valueStr));
                    } else if ("segmentId".equals(nodeName)) {
                        ps.setSegmentId(Integer.parseInt(valueStr));
                    } else if ("interval.startTimeMs".equals(nodeName)) {
                        ps.setInterval_startTimeMs(Long.parseLong(valueStr));
                    } else if ("ip".equals(nodeName)) {
                        ps.setIp(Integer.parseInt(valueStr));
                    } else if ("ipv6".equals(nodeName)) {
                        ps.setIpv6(valueStr);
                    } else if ("ipHash".equals(nodeName)) {
                        ps.setIpv6(valueStr);
                    } else if ("ipv6Hash".equals(nodeName)) {
                        ps.setIpv6(valueStr);
                    } else if ("ipType".equals(nodeName)) {
                        ps.setIpType(valueStr);
                    } else if ("latestReceivedTimeMs".equals(nodeName)) {
                        ps.setLatestReceivedTimeMs(Long.parseLong(valueStr));
                    } else if ("isEnded".equals(nodeName)) {
                        int endInt = "true".equals(valueStr) ? 1 : 0;
                        ps.setIsEnded(endInt);
                    } else if ("firstReceivedTimeMs".equals(nodeName)) {
                        ps.setFirstReceivedTimeMs(Long.parseLong(valueStr));
                    } else if (nodeName.startsWith("tags.")) {
                        String key = quote + nodeName.substring(5) + quote;
                        tags.put(key, quote + valueStr + quote);
                    } else {
                        logger.warn(">>> unknown field:" + jn.toString());
                    }


                }

                ps.setTags(tags);


                writer.write(ps.toTSV());

            }
            writer.flush();
            writer.close();


        } catch (IOException ex) {
            System.err.println(ex);
        }
    }


}
