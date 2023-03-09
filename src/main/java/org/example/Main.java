package org.example;

import org.apache.commons.lang3.time.FastDateFormat;

import java.io.*;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.nio.file.Paths;
import java.util.TimeZone;

public class Main {

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    static {
        final TimeZone gmt = TimeZone.getTimeZone("GMT");
        TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
        TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
        TIME_FORMAT_TIMESTAMP =
                FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    }
    public static void main(String[] args) {
        if (args.length < 2) System.exit(-1);

        System.out.print(args[0]);
        System.out.print(" -> ");
        System.out.println(args[1]);

        boolean useCompression = false;

        if (args.length > 2) {
            useCompression = args[2].equals("compress");
        }

        try {
            CreateBinFile(args[0], args[1], useCompression);
        } catch (Exception e) {
            System.out.println(e);
            System.exit(-1);
        }
    }

    private static void CreateBinFile(String input, String output, boolean useCompression) throws IOException {
        Path path = Paths.get(output);
        String tableName = path.getFileName().toString();
        TableBinaryStorage tbsW = new TableBinaryStorage(tableName, path.getParent().toString(), "rw", useCompression);
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


                        try {
                            Date d = TIME_FORMAT_DATE.parse(parts[i]);
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

