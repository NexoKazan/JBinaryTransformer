package org.example;

import org.apache.commons.lang3.ArrayUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class TableRow {

    private Object[] _columns;
    private ColumnType[] _columnTypes;
    public static long _size = 0;
    private static long _rowSize = 0;

    public TableRow(Object[] columns, ColumnType[] types) {
        _columns     = columns;
        _columnTypes = types;
    }

    public Object[] Columns() {
        return _columns;
    }

    public ColumnType[] ColumnTypes() {
        return _columnTypes;
    }

    public void Serialize(OutputStream stream) throws IOException {
        for ( var i = 0; i < Columns().length; i++ ) {
            WriteData(_columns[ i ], _columnTypes[ i ], stream);
        }
    }

    public byte[] GetBinaryRow() throws IOException {
        byte[] output = TransformRow(_columns[0], _columnTypes[0]);
        for ( var i = 1; i < Columns().length; i++ ) {
            byte[] column = TransformRow(_columns[ i ], _columnTypes[ i ]);
            byte[] row = new byte[output.length + column.length];
            System.arraycopy(output, 0, row,0, output.length);
            System.arraycopy(column, 0, row, output.length, column.length);
            output = row;
        }
        return output;
    }

    private void WriteData(Object data, ColumnType type, OutputStream stream) throws IOException {
        byte[] bytes = new byte[]{};
        switch (type) {
            case LONG -> bytes = BytesConverter.LongToByteArray((long) data);
            case INTEGER -> bytes = BytesConverter.IntToByteArray((int) data);
            case FLOAT -> bytes = BytesConverter.FloatToByteArray((float) data);
            case DOUBLE -> bytes = BytesConverter.DoubleToByteArray((double) data);
            case BOOLEAN -> bytes = BytesConverter.BooleanToByteArray((boolean) data);
            case STRING -> {
                var str = (String) data;
                var strBytes = str.getBytes(StandardCharsets.UTF_8);
                var strStruct = new StringColumn(strBytes);
                bytes = strStruct.writeObject();
            }
            case DATE, TIME, DATETIME ->
                    bytes = BytesConverter.LongToByteArray((long) data);

            default -> {
            }
        }

        stream.write(bytes);
    }
    private byte[] TransformRow(Object data, ColumnType type) throws IOException {
        byte[] bytes = new byte[]{};
        switch (type) {
            case LONG -> bytes = BytesConverter.LongToByteArray((long) data);
            case INTEGER -> bytes = BytesConverter.IntToByteArray((int) data);
            case FLOAT -> bytes = BytesConverter.FloatToByteArray((float) data);
            case DOUBLE -> bytes = BytesConverter.DoubleToByteArray((double) data);
            case BOOLEAN -> bytes = BytesConverter.BooleanToByteArray((boolean) data);
            case STRING -> {
                var str = (String) data;
                var strBytes = str.getBytes(StandardCharsets.UTF_8);
                var strStruct = new StringColumn(strBytes);
                bytes = strStruct.writeObject();
            }
            case DATE, TIME, DATETIME ->
                    bytes = BytesConverter.LongToByteArray(((Date) data).getTime());

            default -> {
            }
        }

        return bytes;
    }


   }
