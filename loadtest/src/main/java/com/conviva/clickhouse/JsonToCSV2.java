package com.conviva.clickhouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.wnameless.json.flattener.JsonFlattener;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Map;

public class JsonToCSV2 {
    private static final Logger logger = Logger
            .getLogger(JsonToCSV2.class);

    public static void main(String[] args) {
        String jsonfilename = "/Users/dyang/part-00001-64edcea8-84f4-4264-8c2f-55bc2237fe56.c000.txt";

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            LineNumberReader lineReader = new LineNumberReader(new FileReader(jsonfilename));

            String lineText = null;
            CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();

//            for (String field : columnames) {
//
//            }


            while ((lineText = lineReader.readLine()) != null) {

                int linenumber = lineReader.getLineNumber();
//                System.out.println(linenumber + ": " + lineText);

//
//                Map<String, Object> map = JsonFlattener.flattenAsMap(lineText);
////                System.out.println(map.keySet());
////                Object ret = map.get("geo.continent");
////
////                String aaa = (String) map.getOrDefault("geo.continent","3333");
//                System.out.println(map);

                JsonNode jsonTree = new ObjectMapper().readTree(lineText);
//
                JsonNode rt =  jsonTree.get("a");

                System.out.println(linenumber);
//
//                jsonTree.fieldNames().forEachRemaining(f -> csvSchemaBuilder.addColumn(f));
//
//
//                CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();
//
//                CsvMapper csvMapper = new CsvMapper();
//                csvMapper.writerFor(JsonNode.class)
//                        .with(csvSchema)
//                        .writeValue(new File("a.csv"), jsonTree);

            }


        } catch (IOException ex) {
            System.err.println(ex);
        }
    }


}
