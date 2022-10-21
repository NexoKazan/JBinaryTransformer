package org.example;

import java.io.*;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) {
        if (args.length < 2) System.exit(-1);

        System.out.print(args[0]);
        System.out.print(" -> ");
        System.out.println(args[1]);

        try {
            CreateBinFile(args[0], args[1]);
        } catch (Exception e) {
            System.out.println(e);
            System.exit(-1);
        }
    }

    private static void CreateBinFile(String input, String output) throws IOException {
        Path path = Paths.get(output);
        String tableName = path.getFileName().toString();
        TableBinaryStorage tbsW = new TableBinaryStorage(tableName, path.getParent().toString(), "rw");
        BufferedReader reader = new BufferedReader(new FileReader(input));
        String line = reader.readLine();
        System.out.println(tableName + "........Started!");
        while (line != null) {
            var parts = line.split("\\|");
            var row = new Object[tbsW.Meta().Columns.length];
            for (var i = 0; i < tbsW.Meta().Columns.length; i++) {
                switch (tbsW.Meta().Types[i]) {
                    case LONG -> row[i] = Long.parseLong(parts[i]);
                    case INTEGER -> row[i] = Integer.parseInt(parts[i]);
                    case FLOAT -> row[i] = Float.parseFloat(parts[i]);
                    case DOUBLE -> row[i] = Double.parseDouble(parts[i]);
                    case BOOLEAN -> row[i] = Boolean.parseBoolean(parts[i]);
                    case STRING -> row[i] = parts[i];
                    case DATE, TIME, DATETIME -> {

                        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                        try {
                            Date d = f.parse(parts[i]);
                            row[i] = d.getTime();
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
}

