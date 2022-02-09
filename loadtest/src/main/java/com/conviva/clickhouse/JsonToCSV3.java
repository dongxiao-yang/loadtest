package com.conviva.clickhouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.wnameless.json.flattener.JsonFlattener;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JsonToCSV3 {

    private static final Logger logger = Logger
            .getLogger(JsonToCSV3.class);


    public static void main(String[] args) {
//        String jsonfilename = "/Users/dyang/part-00002-64edcea8-84f4-4264-8c2f-55bc2237fe56.c000.txt";
        String jsonfilename = "/Users/dyang/part-00001-64edcea8-84f4-4264-8c2f-55bc2237fe56.c000.txt";

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            LineNumberReader lineReader = new LineNumberReader(new FileReader(jsonfilename));

            String lineText = null;
            CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();


            Set<String> finalKeySet = new HashSet<String>();

            while ((lineText = lineReader.readLine()) != null) {

//                int linenumber = lineReader.getLineNumber();

                Map<String, Object> map = JsonFlattener.flattenAsMap(lineText);

                 Set<String> keyset =  map.keySet();
                finalKeySet.addAll(keyset);

            }

            for(String colunmName : finalKeySet){
                csvSchemaBuilder.addColumn(colunmName);
            }
            System.out.println(finalKeySet);
            CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();

//            while ((lineText2 = lineReader.readLine()) != null) {
//
//                JsonNode jsonTree = new ObjectMapper().readTree(lineText2);
//                CsvMapper csvMapper = new CsvMapper();
//                csvMapper.writerFor(JsonNode.class)
//                        .with(csvSchema)
//                        .writeValue(new File("a.csv"), jsonTree);
//
//            }

        } catch (IOException ex) {
            System.err.println(ex);
        }
    }
}
