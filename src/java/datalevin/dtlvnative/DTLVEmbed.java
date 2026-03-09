package datalevin.dtlvnative;

import java.util.Objects;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

public class DTLVEmbed extends DTLVEmbedConfig {
    static {
        Loader.load();
    }

    public static final int DTLV_LLAMA_POOLING_TYPE_UNSPECIFIED = -1;
    public static final int DTLV_LLAMA_POOLING_TYPE_NONE = 0;
    public static final int DTLV_LLAMA_POOLING_TYPE_MEAN = 1;
    public static final int DTLV_LLAMA_POOLING_TYPE_CLS = 2;
    public static final int DTLV_LLAMA_POOLING_TYPE_LAST = 3;
    public static final int DTLV_LLAMA_POOLING_TYPE_RANK = 4;

    @Opaque public static class dtlv_llama_model extends Pointer {
        public dtlv_llama_model() { super((Pointer) null); }
        public dtlv_llama_model(Pointer p) { super(p); }
    }

    @Opaque public static class dtlv_llama_context extends Pointer {
        public dtlv_llama_context() { super((Pointer) null); }
        public dtlv_llama_context(Pointer p) { super(p); }
    }

    public static String lastError() {
        BytePointer pointer = dtlv_llama_last_error();
        return pointer == null ? "" : pointer.getString();
    }

    private static IllegalStateException failure(String action) {
        String error = lastError();
        if (error == null || error.isEmpty()) {
            error = "unknown native failure";
        }
        return new IllegalStateException(action + ": " + error);
    }

    public static native @Cast("const char*") BytePointer dtlv_llama_last_error();

    public static native dtlv_llama_model dtlv_llama_model_load(String model_path,
                                                                int use_mmap,
                                                                int use_mlock);
    public static native void dtlv_llama_model_destroy(dtlv_llama_model model);
    public static native int dtlv_llama_model_n_ctx_train(dtlv_llama_model model);
    public static native int dtlv_llama_model_n_embd_out(dtlv_llama_model model);
    public static native int dtlv_llama_model_has_encoder(dtlv_llama_model model);
    public static native int dtlv_llama_model_has_decoder(dtlv_llama_model model);

    public static native dtlv_llama_context dtlv_llama_context_create(dtlv_llama_model model,
                                                                      @Cast("uint32_t") int n_ctx,
                                                                      @Cast("uint32_t") int n_batch,
                                                                      @Cast("int32_t") int n_threads,
                                                                      @Cast("int32_t") int n_threads_batch);
    public static native dtlv_llama_context dtlv_llama_context_create_nseq(
        dtlv_llama_model model,
        @Cast("uint32_t") int n_ctx,
        @Cast("uint32_t") int n_batch,
        @Cast("uint32_t") int n_seq_max,
        @Cast("int32_t") int n_threads,
        @Cast("int32_t") int n_threads_batch);
    public static native void dtlv_llama_context_destroy(dtlv_llama_context ctx);
    public static native int dtlv_llama_context_pooling_type(dtlv_llama_context ctx);
    public static native int dtlv_llama_context_n_embd_out(dtlv_llama_context ctx);

    public static native int dtlv_llama_embed_text(dtlv_llama_context ctx,
                                                   String text,
                                                   FloatPointer output,
                                                   @Cast("size_t") long output_len);
    public static native int dtlv_llama_embed_texts(
        dtlv_llama_context ctx,
        @Cast("const char* const*") PointerPointer texts,
        @Cast("size_t") long text_count,
        FloatPointer output,
        @Cast("size_t") long output_len);

    public static Model loadModel(String modelPath, boolean useMmap, boolean useMlock) {
        dtlv_llama_model model =
            dtlv_llama_model_load(modelPath, useMmap ? 1 : 0, useMlock ? 1 : 0);
        if (model == null || model.isNull()) {
            throw failure("failed to load embedding model");
        }
        return new Model(model);
    }

    public static final class Model implements AutoCloseable {
        private dtlv_llama_model handle;
        private int openContexts;

        private Model(dtlv_llama_model handle) {
            this.handle = handle;
        }

        private synchronized dtlv_llama_model handle() {
            if (handle == null || handle.isNull()) {
                throw new IllegalStateException("embedding model is closed");
            }
            return handle;
        }

        private synchronized void retainContext() {
            handle();
            openContexts += 1;
        }

        private synchronized void releaseContext() {
            if (openContexts > 0) {
                openContexts -= 1;
            }
        }

        public synchronized int trainingContextLength() {
            return dtlv_llama_model_n_ctx_train(handle());
        }

        public synchronized int embeddingDimensions() {
            return dtlv_llama_model_n_embd_out(handle());
        }

        public synchronized boolean hasEncoder() {
            return dtlv_llama_model_has_encoder(handle()) != 0;
        }

        public synchronized boolean hasDecoder() {
            return dtlv_llama_model_has_decoder(handle()) != 0;
        }

        public synchronized Context createContext(int nCtx,
                                                  int nBatch,
                                                  int nThreads,
                                                  int nThreadsBatch) {
            dtlv_llama_context ctx =
                dtlv_llama_context_create(handle(), nCtx, nBatch, nThreads, nThreadsBatch);
            if (ctx == null || ctx.isNull()) {
                throw failure("failed to create embedding context");
            }
            retainContext();
            return new Context(this, ctx);
        }

        // nCtx is the total context budget across all sequences in the batch.
        public synchronized Context createBatchContext(int nCtx,
                                                       int nBatch,
                                                       int nSeqMax,
                                                       int nThreads,
                                                       int nThreadsBatch) {
            dtlv_llama_context ctx =
                dtlv_llama_context_create_nseq(handle(), nCtx, nBatch, nSeqMax,
                                               nThreads, nThreadsBatch);
            if (ctx == null || ctx.isNull()) {
                throw failure("failed to create embedding batch context");
            }
            retainContext();
            return new Context(this, ctx);
        }

        @Override
        public synchronized void close() {
            if (openContexts > 0) {
                throw new IllegalStateException(
                    "embedding model still has " + openContexts + " open context(s)");
            }
            if (handle != null && !handle.isNull()) {
                dtlv_llama_model_destroy(handle);
                handle = null;
            }
        }
    }

    public static final class Context implements AutoCloseable {
        private final Model model;
        private dtlv_llama_context handle;

        private Context(Model model, dtlv_llama_context handle) {
            this.model = model;
            this.handle = handle;
        }

        private dtlv_llama_context handle() {
            if (handle == null || handle.isNull()) {
                throw new IllegalStateException("embedding context is closed");
            }
            return handle;
        }

        public Model model() {
            return model;
        }

        public synchronized int poolingType() {
            return dtlv_llama_context_pooling_type(handle());
        }

        public synchronized int embeddingDimensions() {
            return dtlv_llama_context_n_embd_out(handle());
        }

        public synchronized float[] embed(String text) {
            int dims = embeddingDimensions();
            try (FloatPointer output = new FloatPointer(dims)) {
                int rc = dtlv_llama_embed_text(handle(), text, output, dims);
                if (rc != 0) {
                    throw failure("failed to embed text");
                }
                float[] vector = new float[dims];
                output.get(vector);
                return vector;
            }
        }

        public synchronized void embed(String text, float[] output) {
            Objects.requireNonNull(output, "output");
            int dims = embeddingDimensions();
            if (output.length < dims) {
                throw new IllegalArgumentException(
                    "output buffer is too small, need at least " + dims + " floats");
            }
            try (FloatPointer pointer = new FloatPointer(output.length)) {
                int rc = dtlv_llama_embed_text(handle(), text, pointer, output.length);
                if (rc != 0) {
                    throw failure("failed to embed text");
                }
                pointer.get(output, 0, dims);
            }
        }

        public synchronized float[][] embed(String[] texts) {
            Objects.requireNonNull(texts, "texts");
            int dims = embeddingDimensions();
            float[][] matrix = new float[texts.length][dims];
            if (texts.length == 0) {
                return matrix;
            }

            float[] flat = new float[Math.multiplyExact(texts.length, dims)];
            embed(texts, flat);

            for (int i = 0; i < texts.length; ++i) {
                System.arraycopy(flat, i * dims, matrix[i], 0, dims);
            }
            return matrix;
        }

        public synchronized void embed(String[] texts, float[] output) {
            Objects.requireNonNull(texts, "texts");
            Objects.requireNonNull(output, "output");

            int dims = embeddingDimensions();
            int needed = Math.multiplyExact(texts.length, dims);
            if (output.length < needed) {
                throw new IllegalArgumentException(
                    "output buffer is too small, need at least " + needed + " floats");
            }
            if (texts.length == 0) {
                return;
            }

            try (PointerScope scope = new PointerScope();
                 PointerPointer<BytePointer> textPointers = new PointerPointer<>(texts.length);
                 FloatPointer pointer = new FloatPointer(needed)) {
                for (int i = 0; i < texts.length; ++i) {
                    Objects.requireNonNull(texts[i], "texts[" + i + "]");
                    BytePointer text = new BytePointer(texts[i]);
                    scope.attach(text);
                    textPointers.put(i, text);
                }

                int rc = dtlv_llama_embed_texts(handle(), textPointers, texts.length,
                                                pointer, needed);
                if (rc != 0) {
                    throw failure("failed to embed texts");
                }
                pointer.get(output, 0, needed);
            }
        }

        @Override
        public synchronized void close() {
            if (handle != null && !handle.isNull()) {
                dtlv_llama_context_destroy(handle);
                handle = null;
                model.releaseContext();
            }
        }
    }
}
