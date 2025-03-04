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
                           compiler = {"-D_CRT_SECURE_NO_WARNINGS"},
                           link = { "lmdb", "vcomp", "ucrt", "vcruntime", "Advapi32", "libusearch_static_c", "dtlv" }
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
        infoMap.put(new Info("usearch_init_options_t").pointerTypes("usearch_init_options_t"));

    }
}
