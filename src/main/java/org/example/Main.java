package org.example;

import java.io.*;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        String sourcePath = "tpch";
        if (args[0]!=null)
        {
            sourcePath = args[0];
        }
        String[] tableNames = new String[]{"customer", "nation", "part", "partsupp", "region", "supplier", "lineitem", "orders"};
        //String[] tableNames = new String[]{"orders"};

        try {
            CreateBinFiles(tableNames);
            //CreateBinFilesBuffer(tableNames);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void CreateBinFiles(String[] tableNames) throws IOException {
        System.out.println(new Date(System.currentTimeMillis()));
        long startTime = System.currentTimeMillis();
        for ( String tableName : tableNames ) {
            TableBinaryStorage tbsW = new TableBinaryStorage(tableName, "sourcePath", "rw");
            BufferedReader reader = new BufferedReader(new FileReader("sourcePath\\" + tableName + ".tbl"));
            String line = reader.readLine();
            System.out.println(tableName + "........Started!");
            while (line != null) {
                var parts = line.split("\\|");
                var row = new Object[ tbsW.Meta().Columns.length ];
                for ( var i = 0; i < tbsW.Meta().Columns.length; i++ ) {
                    switch (tbsW.Meta().Types[ i ]) {
                        case LONG -> row[ i ] = Long.parseLong(parts[ i ]);
                        case INTEGER -> row[ i ] = Integer.parseInt(parts[ i ]);
                        case FLOAT -> row[ i ] = Float.parseFloat(parts[ i ]);
                        case DOUBLE -> row[ i ] = Double.parseDouble(parts[ i ]);
                        case BOOLEAN -> row[ i ] = Boolean.parseBoolean(parts[ i ]);
                        case STRING -> row[ i ] = parts[ i ];
                        case DATE, TIME, DATETIME -> {

                            SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                            try {
                                Date d = f.parse(parts[ i ]);
                                row[ i ] = d.getTime();
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        }
                        default -> {
                        }
                    }
                }

                tbsW.AddData(row);
                line = reader.readLine();
            }
            reader.close();
            System.out.println(tableName + "........Done!");
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }

    private static void CreateBinFilesBuffer(String[] tableNames) throws IOException {
        long maxStings = 10000;
        System.out.println(new Date(System.currentTimeMillis()));
        long startTime = System.currentTimeMillis();
        for ( String tableName : tableNames ) {
            byte[] rows = new byte[]{};
            boolean writed = false;
            int writeSize = 0;
            TableBinaryStorage tbsW = new TableBinaryStorage(tableName, "tpch", "rw");
            BufferedReader reader = new BufferedReader(new FileReader("tpch\\" + tableName + ".tbl"));
            String line = reader.readLine();
            System.out.println(tableName + "........Started!");
            while (line != null) {
                var parts = line.split("\\|");
                var row = new Object[ tbsW.Meta().Columns.length ];
                for ( var i = 0; i < tbsW.Meta().Columns.length; i++ ) {
                    switch (tbsW.Meta().Types[ i ]) {
                        case LONG -> row[ i ] = Long.parseLong(parts[ i ]);
                        case INTEGER -> row[ i ] = Integer.parseInt(parts[ i ]);
                        case FLOAT -> row[ i ] = Float.parseFloat(parts[ i ]);
                        case DOUBLE -> row[ i ] = Double.parseDouble(parts[ i ]);
                        case BOOLEAN -> row[ i ] = Boolean.parseBoolean(parts[ i ]);
                        case STRING -> row[ i ] = parts[ i ];
                        case DATE, TIME, DATETIME -> {

                            SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                            try {
                                Date d = f.parse(parts[ i ]);
                                row[ i ] = d.getTime();
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        }
                        default -> {
                        }
                    }
                }
                rows = CreateByteArray(rows, tbsW.TranswormData(row));
                writeSize ++;
                if (writeSize > maxStings - 1)
                {
                    System.out.println(tableName + "........" + maxStings  + " Write");

                    writed = true;
                    tbsW.Write(rows);
                    rows  = new byte[]{};
                    writeSize = 0;
                }
                //tbsW.AddData(row);
                line = reader.readLine();
            }
            if(!writed)
            {
                System.out.println(tableName + "........Write");
                tbsW.Write(rows);
            }
            reader.close();
            System.out.println(tableName + "........Done!");
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }

    private static byte[] CreateByteArray(byte[] dest, byte[] source) {
        byte[] output = new byte[dest.length + source.length];
        System.arraycopy(dest, 0, output, 0, dest.length);
        System.arraycopy(source, 0, output, dest.length, source.length);

        return output;
    }
}

