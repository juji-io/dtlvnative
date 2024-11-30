package dtlvnative;

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

@Properties(
            value = @Platform(
                              includepath = {"../../src/lmdb/libraries/liblmdb/",
                                             "../../src/"},
                              // preloadpath = {"."},
                              linkpath = {"../../src/"},
                              include = {"lmdb.h", "dtlv.h"},
                              // preload = {"LMDB", "DTLV"},
                              link = {"DTLV"}
                              ),
            target = "dtlvnative.DTLV"
            )

public class DTLVConfig {
    public void map(InfoMap infoMap) {
    }
}
