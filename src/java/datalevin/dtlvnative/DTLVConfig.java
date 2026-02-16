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
                           link = { "lmdb", "libusearch_static_c", "dtlv", "Advapi32" }
                           ),
                @Platform( // Linux
                           value = { "linux" },
                           link = { "dtlv" },
                           preload = { "gomp", "omp" }),
                @Platform( // macOS
                           value = { "macosx" },
                           link = { "dtlv" },
                           preload = { "omp" }),
                @Platform( // FreeBSD
                           value = { "freebsd" },
                           link = { "dtlv", "usearch" })
            },
            target = "datalevin.dtlvnative.DTLV"
            )

public class DTLVConfig {
    public void map(InfoMap infoMap) {
        infoMap.put(new Info("USEARCH_EXPORT").cppTypes().annotations());
    }
}
