package org.example;

import com.google.gson.Gson;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class TableBinaryStorage implements IDatabaseReader, AutoCloseable {

    private final String _table;
    private final String _db;
    private final String _mode;
    private final FileMeta _meta;
    //private final RandomAccessFile _file;
    private FileInputStream _fis = null;
    MappedByteBuffer _inputBuffer = null;
    FileChannel _inputChannel = null;
    private OutputStream _fos = null;

    public long readedSize;
    private long _channelSize;
    private long METAINT = Integer.MAX_VALUE;

    public TableBinaryStorage(String table, String db, String mode, boolean useCompression) throws IOException {
        _table = table;
        _db    = db;
        if(Objects.equals(mode, "rw") || Objects.equals(mode, "r")) {
            _mode = mode;
        }
        else {
            _mode = "rw";
        }
        var path = Path.of(db,table + "_meta.json");

        BufferedReader br = new BufferedReader(new FileReader(String.valueOf(path)));
        Gson gson = new Gson();
        _meta = gson.fromJson(br, FileMeta.class);
        br.close();

//        _file = new RandomAccessFile(Path.of(db,table).toString(), mode);
        if(_mode.equals("rw")) {
            _fos = new FileOutputStream(Path.of(db, table).toString(), true);
            if (useCompression)
                _fos = new CompressedOutputStream(_fos);
        }
    }

    @Override
    public FileMeta GetMeta() {
        return _meta;
    }

    public FileMeta Meta() {
        //Разницы с GetMeta нет, но пусть будет.
        return _meta;
    }

    public void AddData(Object[] cols) throws IOException {
        if(!Objects.equals(_mode, "r")) {
            TableRow row = new TableRow(cols, _meta.Types);
            row.Serialize(_fos);
        }
    }

    public void Write(byte[] rows) throws IOException {
       _fos.write(rows);

    }

    public byte[] TranswormData(Object[] cols) throws IOException {
        if(!Objects.equals(_mode, "r")) {
            TableRow row = new TableRow(cols, _meta.Types);

            return row.GetBinaryRow();
        }
        else return null;
    }


    @Override
    public void close() throws Exception {
        _inputChannel.close();    }


}
