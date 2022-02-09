package com.conviva.clickhouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Iterator;
import java.util.List;

public class JsonToCSV {

    private static final Logger logger = Logger
            .getLogger(JsonToCSV.class);
    public static void main(String[] args)  {

        String jsonfilename ="/Users/dyang/part-00002-64edcea8-84f4-4264-8c2f-55bc2237fe56.c000.txt";

        try {
            LineNumberReader lineReader = new LineNumberReader(new FileReader(jsonfilename));

            String lineText = null;
            CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();

            while ((lineText = lineReader.readLine()) != null) {

                int linenumber = lineReader.getLineNumber();
//                System.out.println(linenumber + ": " + lineText);

                            JsonNode jsonTree = new ObjectMapper().readTree(lineText);


                            if(linenumber == 1)
                            {

                                jsonTree.fieldNames().forEachRemaining(f -> csvSchemaBuilder.addColumn(f));

                            }

                CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();

            CsvMapper csvMapper = new CsvMapper();
            csvMapper.writerFor(JsonNode.class)
                    .with(csvSchema)
                    .writeValue(new File("a.csv"), jsonTree);

            }



        } catch (IOException ex) {
        System.err.println(ex);
    }


    }
}
