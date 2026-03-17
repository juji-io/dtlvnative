# dtlvnative

Provides pre-built native dependencies for
[Datalevin](https://github.com/juji-io/datalevin) database. This is done by
packaging the compiled native libraries and JavaCPP JNI library files in the
platform specific JAR files.

In addition to JavaCPP's JNI library, these native libraries are included:

* [`dlmdb`](https://github.com/huahaiy/dlmdb) a fork of
  [LMDB](https://www.symas.com/mdb) key value storage library.
* [`usearch`](https://github.com/unum-cloud/USearch) a vector indexing and
  similarity search library that is exposed directly for callers.
* [`llama.cpp`](https://github.com/ggml-org/llama.cpp) built as a minimal
  CPU-only GGUF embedding runtime and exposed directly for callers.
* `dtlv` wraps DLMDB. It implements Datalevin iterators, counters and
  samplers.

The following platforms are currently supported:

* macosx-arm64
* freebsd-x86_64
* linux-arm64
* linux-x86_64
* windows-x86_64

The name of the released JAR is `org.clojars.huahaiy/dtlvnative-PLATFORM`, where
`PLATFORM` is one of the above.

Vector support using usearch on Windows is experimental.

## llama.cpp embedding

`dtlvnative` includes a reduced `llama.cpp` build for embeddings only. It keeps
the CPU backend enabled, uses OpenMP, and strips chat, generation, and other
non-embedding model/runtime code from the packaged static library.

The upstream `src/llama.cpp` submodule stays clean in git. The reduction is
applied by the parent build scripts with
`script/patches/llama-embed-only.patch`.

The exposed native/JavaCPP embedding API is:

| Function | Description |
|---|---|
| `dtlv_llama_embedder_create` | Load a GGUF model and create an embedder |
| `dtlv_llama_embedder_n_embd` | Return the embedding dimension |
| `dtlv_llama_embedder_n_ctx` | Return the context size (max tokens) |
| `dtlv_llama_token_count` | Count tokens for a string without allocating |
| `dtlv_llama_tokenize` | Tokenize a string into a caller-owned `int[]` buffer |
| `dtlv_llama_detokenize` | Convert tokens back to a UTF-8 string |
| `dtlv_llama_embed` | Compute an embedding for a single string |
| `dtlv_llama_embed_batch` | Compute embeddings for multiple strings in one call |
| `dtlv_llama_embedder_destroy` | Free the embedder |

The model must be a GGUF embedding model. The current smoke test uses
`multilingual-e5-small-Q8_0.gguf`.

`dtlv_llama_embedder_create` takes `model_path`, `n_ctx`, `n_batch`,
`n_threads`, and `normalize`. Pass `0` for `n_ctx` and `n_batch` to use model
defaults. A non-zero `normalize` returns L2-normalized embeddings.

### Single embedding

```java
DTLV.dtlv_llama_embedder embedder = new DTLV.dtlv_llama_embedder();
int rc = DTLV.dtlv_llama_embedder_create(
        embedder,
        "multilingual-e5-small-Q8_0.gguf",
        0, 0, 4, 1);

int nEmbd = DTLV.dtlv_llama_embedder_n_embd(embedder);
float[] output = new float[nEmbd];
rc = DTLV.dtlv_llama_embed(embedder, "query: hello world", output, nEmbd);

DTLV.dtlv_llama_embedder_destroy(embedder);
```

### Token counting and tokenization

```java
// check token count before embedding
int nTokens = DTLV.dtlv_llama_token_count(embedder, text);
int maxTokens = DTLV.dtlv_llama_embedder_n_ctx(embedder);

// tokenize, truncate, detokenize
int[] tokens = new int[maxTokens];
int actual = DTLV.dtlv_llama_tokenize(embedder, text, tokens, maxTokens);
if (actual > maxTokens) {
    // truncate to fit
    actual = maxTokens;
}
byte[] buf = new byte[text.length() * 4];
int len = DTLV.dtlv_llama_detokenize(embedder, tokens, actual, buf, buf.length);
String truncated = new String(buf, 0, len, StandardCharsets.UTF_8);
```

### Batch embedding

```java
PointerPointer texts = new PointerPointer("query: hello", "query: world");
int nTexts = 2;
float[] output = new float[nTexts * nEmbd];
rc = DTLV.dtlv_llama_embed_batch(embedder, texts, nTexts, output, output.length);
// output[0..nEmbd-1] = embedding for "query: hello"
// output[nEmbd..2*nEmbd-1] = embedding for "query: world"
```

The Java test in `src/java/datalevin/dtlvnative/Test.java` will use
`target/embedding-models/multilingual-e5-small-Q8_0.gguf` if present, fall back
to a repository-root copy if present, and otherwise download the model from
Hugging Face before running the embedding smoke test.

## Additional dependencies

Right now, the included shared libraries depend on some system libraries.

* `libc`
* `libmvec`
* `libomp` or `libgomp`

We bundle `libomp` in the Jar. However, on systems that the bundled library is
not working, or `libc` is not available, you will have to install them yourself.
For example, on Ubuntu/Debian, `apt install libgomp1`, or `apt install gcc-12
g++-12`; on MacOS, `brew install libomp libllvm`

## License

Copyright © 2021-2026 Huahai Yang

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
