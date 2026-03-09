package datalevin.dtlvnative;

import org.bytedeco.javacpp.annotation.*;

@Properties(
    value = {
        @Platform(
            includepath = {
                "../../src/llama.cpp/include/",
                "../../src/llama.cpp/ggml/include/",
                "../../src/"
            },
            include = { "dtlv_embed.h" },
            linkpath = { "../../src/" }
        ),
        @Platform(
            value = "windows",
            linkpath = {
                "../../src/",
                "../../src/Release/",
                "../../src/build_dtlv/Release/"
            },
            link = { "dtlv_embed", "llama", "ggml", "ggml-base", "ggml-cpu", "Advapi32" }
        ),
        @Platform(
            value = { "linux", "macosx", "freebsd" },
            link = { "dtlv_embed" }
        )
    },
    target = "datalevin.dtlvnative.DTLVEmbed"
)
public class DTLVEmbedConfig {}
