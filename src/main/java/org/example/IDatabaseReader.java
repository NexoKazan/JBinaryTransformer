package org.example;

import java.io.IOException;
import java.util.List;

public interface IDatabaseReader {
    FileMeta GetMeta();
    void close() throws Exception;
}
