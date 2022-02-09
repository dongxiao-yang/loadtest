package com.conviva.clickhouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

public class PIIToCSV {


    private static final Logger logger = Logger
            .getLogger(PIIToCSV.class);

    public static final char DEFAULT_SEPARATOR = '\t';
    public static final String DEFAULT_LINE_END = "\n";


    public static void main(String[] args) {
        String jsonfilename = "/Users/dyang/part-00002-64edcea8-84f4-4264-8c2f-55bc2237fe56.c000.txt";

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
                    .append("tags.key").append(DEFAULT_SEPARATOR)
                    .append("tags.value")
                    .append(DEFAULT_LINE_END);

            String head = sb.toString();

//            CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();
//
//            csvSchemaBuilder.addColumn("customerId");
//            csvSchemaBuilder.addColumn("clientId");
//            csvSchemaBuilder.addColumn("sessionId");
//            csvSchemaBuilder.addColumn("segmentId");
//            csvSchemaBuilder.addColumn("interval_startTimeMs");
//            csvSchemaBuilder.addColumn("ip");
//            csvSchemaBuilder.addColumn("ipv6");
//            csvSchemaBuilder.addColumn("ipHash");
//            csvSchemaBuilder.addColumn("ipv6Hash");
//            csvSchemaBuilder.addColumn("ipType");
//            csvSchemaBuilder.addColumn("latestReceivedTimeMs");
//            csvSchemaBuilder.addColumn("isEnded");
//            csvSchemaBuilder.addColumn("firstReceivedTimeMs");
//            csvSchemaBuilder.addColumn("tags_key");
//            csvSchemaBuilder.addColumn("tags_value");


//            CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();

            PrintWriter writer = new PrintWriter(new File("PII.tsv"));

            writer.write(head);

            while ((lineText = lineReader.readLine()) != null) {

                JsonNode jsonTree = new ObjectMapper().readTree(lineText);
                CsvMapper csvMapper = new CsvMapper();

                PIISession ps = new PIISession();

                ArrayList<String> tags_key = new ArrayList<>();
                ArrayList<String> tags_value = new ArrayList<>();

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
                        String key = nodeName.substring(5);
                        tags_key.add(key);
                        tags_value.add(valueStr);
                    } else {
                        logger.warn(">>> unknown field:" + jn.toString());
                    }


                }

                ps.setTags_key(tags_key.toArray(new String[tags_key.size()]));
                ps.setTags_value(tags_value.toArray(new String[tags_key.size()]));


//                logger.info( csvMapper.writerFor(PIISession[].class)
//                        .with(csvSchema)
//                        .writeValueAsString(ps));
                writer.write(ps.toTSV());

            }
            writer.flush();
            writer.close();


        } catch (IOException ex) {
            System.err.println(ex);
        }
    }

}
