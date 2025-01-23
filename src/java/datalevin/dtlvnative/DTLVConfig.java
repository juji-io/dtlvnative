package datalevin.dtlvnative;

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

@Properties(
            value = {
                @Platform( // Common
                           includepath = {
                              "../../src/lmdb/libraries/liblmdb/",
                              "../../src/usearch/include/",
                              "../../src/" },
                           include = { "lmdb.h", "usearch.hpp", "dtlv.h" },
                           linkpath = { "../../src/" }
                           ),
                @Platform( // Windows
                           value = "windows",
                           link = { "lmdb", "dtlv", "Advapi32" }
                           ),
                @Platform( // Unix-like
                           value = { "linux", "macosx" },
                           link = { "dtlv" }
                           )
            },
            target = "datalevin.dtlvnative.DTLV"
            )

public class DTLVConfig {
    public void map(InfoMap infoMap) {
    }
}
