package dtlvnative;

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

@Properties(
            value = @Platform(
                              includepath = {"../../src/lmdb/libraries/liblmdb/",
                                             "../../src/"},
                              include = {"lmdb.h", "dtlv.h"},
                              // preloadpath = {"."},
                              preload = {"lmdb", "dtlv"},
                              linkpath = {"../../src/"},
                              link = {"DTLV"}
                              ),
            target = "dtlvnative.DTLV"
            )

public class DTLVConfig {
    public void map(InfoMap infoMap) {
    }
}
