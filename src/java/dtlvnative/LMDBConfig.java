package dtlvnative;

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

@Properties(
            value = @Platform(
                              includepath = {"../../src/lmdb/libraries/liblmdb/"},
                              linkpath = {"../../src/lmdb/libraries/liblmdb/"},
                              include = {"lmdb.h"},
                              link = {"LMDB"}
                              ),
            target = "dtlvnative.LMDB"
            )

public class LMDBConfig {
    public void map(InfoMap infoMap) {
    }
}
