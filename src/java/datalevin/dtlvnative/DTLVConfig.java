package datalevin.dtlvnative;

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

@Properties(
            value = @Platform(
                              includepath = {"../../src/lmdb/libraries/liblmdb/",
                                             "../../src/"},
                              include = {"lmdb.h", "dtlv.h"},
                              linkpath = {"../../src/"},
                              link = {"lmdb", "dtlv"}
                              ),
            target = "datalevin.dtlvnative.DTLV"
            )

public class DTLVConfig {
    public void map(InfoMap infoMap) {
    }
}
