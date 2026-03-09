package datalevin.dtlvnative;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;

public class EmbedTest {

    public static final String DEFAULT_MODEL_ID =
        "keisuke-miyako/multilingual-e5-small-gguf-q8_0";
    public static final String DEFAULT_MODEL_FILE =
        "multilingual-e5-small-Q8_0.gguf";
    public static final String DEFAULT_MODEL_SHA256 =
        "0d5a5a0b0ad84faad6357a6145e769b0661f0efbf53acf74598afc34dab454f4";
    public static final String DEFAULT_MODEL_URL =
        "https://huggingface.co/keisuke-miyako/multilingual-e5-small-gguf-q8_0/resolve/main/" +
        DEFAULT_MODEL_FILE;
    public static final Path DEFAULT_MODEL_DIR =
        Path.of(System.getenv().getOrDefault("DTLV_EMBED_MODEL_DIR",
                                             Path.of(System.getProperty("user.dir"),
                                                     "target",
                                                     "embedding-models").toString()));

    private static final int EXPECTED_DIMS = 384;
    private static final int EXPECTED_TRAINING_CONTEXT = 512;
    private static final int EXPECTED_POOLING =
        DTLVEmbed.DTLV_LLAMA_POOLING_TYPE_MEAN;
    private static final int BATCH_N_BATCH = 512;
    private static final int BATCH_N_SEQ_MAX = 8;
    private static final int BATCH_TEXT_COUNT = 17;
    private static final float MAX_BATCH_DIFF = 0.02f;
    private static final HttpClient HTTP =
        HttpClient.newBuilder()
                  .followRedirects(HttpClient.Redirect.NORMAL)
                  .build();

    private static void expect(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static String poolingName(int poolingType) {
        switch (poolingType) {
        case DTLVEmbed.DTLV_LLAMA_POOLING_TYPE_NONE:
            return "none";
        case DTLVEmbed.DTLV_LLAMA_POOLING_TYPE_MEAN:
            return "mean";
        case DTLVEmbed.DTLV_LLAMA_POOLING_TYPE_CLS:
            return "cls";
        case DTLVEmbed.DTLV_LLAMA_POOLING_TYPE_LAST:
            return "last";
        case DTLVEmbed.DTLV_LLAMA_POOLING_TYPE_RANK:
            return "rank";
        case DTLVEmbed.DTLV_LLAMA_POOLING_TYPE_UNSPECIFIED:
            return "unspecified";
        default:
            return Integer.toString(poolingType);
        }
    }

    private static double l2Norm(float[] vector) {
        double sum = 0.0;
        for (float value : vector) {
            expect(Float.isFinite(value), "embedding contains non-finite values");
            sum += value * value;
        }
        return Math.sqrt(sum);
    }

    private static float maxAbsDiff(float[] expected, float[] actual, int offset) {
        float max = 0.0f;
        for (int i = 0; i < expected.length; ++i) {
            float diff = Math.abs(expected[i] - actual[offset + i]);
            if (diff > max) {
                max = diff;
            }
        }
        return max;
    }

    private static String sampleText(int i) {
        switch (i % 6) {
        case 0:
            return "query: hello world " + i;
        case 1:
            return "passage: the cat sits on the mat " + i;
        case 2:
            return "query: bonjour le monde " + i;
        case 3:
            return "passage: el gato duerme en la silla " + i;
        case 4:
            return "query: ni hao shi jie " + i;
        default:
            return "passage: das ist ein kurzer test " + i;
        }
    }

    private static String[] sampleTexts() {
        String[] texts = new String[BATCH_TEXT_COUNT];
        for (int i = 0; i < texts.length; ++i) {
            texts[i] = sampleText(i);
        }
        return texts;
    }

    private static Path configuredDefaultModelPath() {
        String path = System.getenv("DTLV_EMBED_MODEL_PATH");
        if (path != null && !path.isBlank()) {
            return Path.of(path);
        }
        return DEFAULT_MODEL_DIR.resolve(DEFAULT_MODEL_FILE);
    }

    private static URI configuredDefaultModelUri() {
        String url = System.getenv("DTLV_EMBED_MODEL_URL");
        if (url != null && !url.isBlank()) {
            return URI.create(url);
        }
        return URI.create(DEFAULT_MODEL_URL);
    }

    private static String sha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) >= 0) {
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void verifyDefaultModel(Path path) throws Exception {
        String actual = sha256(path);
        expect(DEFAULT_MODEL_SHA256.equals(actual),
               "unexpected checksum for default model at " + path +
               " expected " + DEFAULT_MODEL_SHA256 + " got " + actual);
    }

    private static void downloadDefaultModel(Path path) throws Exception {
        Path parent = path.toAbsolutePath().normalize().getParent();
        expect(parent != null, "default model path has no parent: " + path);
        Files.createDirectories(parent);

        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.deleteIfExists(tmp);

        URI uri = configuredDefaultModelUri();
        System.err.println("Downloading default embedding model " + DEFAULT_MODEL_ID);
        System.err.println("  from " + uri);
        System.err.println("  to   " + path);

        HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
        HttpResponse<Path> response =
            HTTP.send(request,
                      HttpResponse.BodyHandlers.ofFile(tmp,
                                                       StandardOpenOption.CREATE,
                                                       StandardOpenOption.WRITE,
                                                       StandardOpenOption.TRUNCATE_EXISTING));

        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            Files.deleteIfExists(tmp);
            throw new IllegalStateException(
                "failed to download default model, HTTP status " + status);
        }

        verifyDefaultModel(tmp);
        try {
            Files.move(tmp, path,
                       StandardCopyOption.REPLACE_EXISTING,
                       StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public static Path ensureDefaultModelAvailable() throws Exception {
        Path path = configuredDefaultModelPath().toAbsolutePath().normalize();
        if (!Files.isRegularFile(path)) {
            downloadDefaultModel(path);
        }
        verifyDefaultModel(path);
        return path;
    }

    public static void runModelTest(Path modelPath) throws Exception {
        expect(Files.isRegularFile(modelPath),
               "model file does not exist: " + modelPath);

        int threads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        String[] texts = sampleTexts();

        try (DTLVEmbed.Model model =
                 DTLVEmbed.loadModel(modelPath.toString(), true, false)) {
            expect(model.trainingContextLength() == EXPECTED_TRAINING_CONTEXT,
                   "unexpected training context for " + DEFAULT_MODEL_ID +
                   ": " + model.trainingContextLength());
            expect(model.embeddingDimensions() == EXPECTED_DIMS,
                   "unexpected embedding dimensions for " + DEFAULT_MODEL_ID +
                   ": " + model.embeddingDimensions());

            // Batch contexts take total context across all sequences.
            int batchContextLength =
                Math.multiplyExact(model.trainingContextLength(), BATCH_N_SEQ_MAX);

            try (DTLVEmbed.Context single =
                     model.createContext(0, BATCH_N_BATCH, threads, threads);
                 DTLVEmbed.Context batch =
                     model.createBatchContext(batchContextLength, BATCH_N_BATCH,
                                              BATCH_N_SEQ_MAX, threads, threads)) {
                expect(single.embeddingDimensions() == EXPECTED_DIMS,
                       "single context reported unexpected embedding dimensions");
                expect(batch.embeddingDimensions() == EXPECTED_DIMS,
                       "batch context reported unexpected embedding dimensions");
                expect(single.poolingType() == EXPECTED_POOLING,
                       "single context reported pooling " +
                       poolingName(single.poolingType()));
                expect(batch.poolingType() == EXPECTED_POOLING,
                       "batch context reported pooling " +
                       poolingName(batch.poolingType()));

                float[] hello = single.embed("query: hello world");
                double helloNorm = l2Norm(hello);
                expect(helloNorm > 0.0,
                       "default model returned a zero vector for hello world");

                float[] flat = new float[Math.multiplyExact(texts.length, EXPECTED_DIMS)];
                batch.embed(texts, flat);

                float maxDiff = 0.0f;
                for (int i = 0; i < texts.length; ++i) {
                    float[] vector = single.embed(texts[i]);
                    double norm = l2Norm(vector);
                    expect(norm > 0.0, "single embedding returned a zero vector at row " + i);
                    maxDiff = Math.max(maxDiff,
                                       maxAbsDiff(vector, flat, i * EXPECTED_DIMS));
                }

                expect(maxDiff <= MAX_BATCH_DIFF,
                       String.format(Locale.ROOT,
                                     "batch embeddings drifted too far from single embeddings: %.8f",
                                     maxDiff));

                System.out.printf(Locale.ROOT,
                                  "embed default ok model=%s dims=%d pooling=%s ctx_train=%d batch_seq_max=%d max_abs_diff=%.8f hello_norm=%.6f%n",
                                  DEFAULT_MODEL_ID,
                                  EXPECTED_DIMS,
                                  poolingName(EXPECTED_POOLING),
                                  EXPECTED_TRAINING_CONTEXT,
                                  BATCH_N_SEQ_MAX,
                                  maxDiff,
                                  helloNorm);
            }
        }
    }

    public static void runDefaultModelTest() throws Exception {
        runModelTest(ensureDefaultModelAvailable());
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            runDefaultModelTest();
            return;
        }
        expect(args.length == 1,
               "usage: datalevin.dtlvnative.EmbedTest [/path/to/model.gguf]");
        runModelTest(Path.of(args[0]));
    }
}
