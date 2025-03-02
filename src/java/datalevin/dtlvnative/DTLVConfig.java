package datalevin.dtlvnative;

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

@Properties(
            value = {
                @Platform( // Common
                           includepath = {
                              "../../src/lmdb/libraries/liblmdb/",
                              "../../src/usearch/c/",
                              "../../src/" },
                           include = { "lmdb.h", "usearch.h", "dtlv.h" },
                           linkpath = { "../../src/" }
                           ),
                @Platform( // Windows
                           value = "windows",
                           define = {"USEARCH_USE_OPENMP 0"},
                           convention = "stdcall",
                           catchExceptions = true,
                           link = { "lmdb", "libusearch_static_c", "dtlv", "Advapi32" }
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
        infoMap.put(new Info("USEARCH_EXPORT").cppTypes().annotations());
    }
}
