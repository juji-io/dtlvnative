package datalevin.dtlvnative;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.function.LongPredicate;
import java.nio.file.*;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

import datalevin.dtlvnative.DTLV.usearch_metric_t;

public class Test {

    static void deleteDirectoryFiles(final String path) {
        File directory = new File(path);
        if (!directory.isDirectory()) {
            directory.delete();
            return;
        }

        for (File f : directory.listFiles()) f.delete();
        directory.delete();
    }

    static void testLMDB() {

        System.err.println("Testing LMDB ...");

        DTLV.MDB_env env = new DTLV.MDB_env();
        int result = DTLV.mdb_env_create(env);
        if (result != 0) {
            System.err.println("Failed to create DTLV environment: " + result);
            return;
        }

        result = DTLV.mdb_env_set_maxdbs(env, 5);
        if (result != 0) {
            System.err.println("Failed to set max dbs: " + result);
            return;
        }

        String dir = "db";
        try {
            Files.createDirectories(Paths.get(dir));
        } catch(IOException e) {
            System.err.println("Failed to create directory: " + dir);
            e.printStackTrace();
        }

        result = DTLV.mdb_env_open(env, dir, 0, 0664);
        if (result != 0) {
            System.err.println("Failed to open DTLV environment: " + result);
            return;
        }

        DTLV.MDB_txn txn = new DTLV.MDB_txn();
        result = DTLV.mdb_txn_begin(env, null, 0, txn);
        if (result != 0) {
            System.err.println("Failed to begin a r/w transaction: " + result);
            return;
        }

        IntPointer dbi = new IntPointer(1);
        String dbiName = "a";
        result = DTLV.mdb_dbi_open(txn, dbiName, DTLV.MDB_CREATE, dbi);
        if (result != 0) {
            System.err.println("Failed to open dbi: " + result);
            return;
        }

        result = DTLV.dtlv_set_comparator(txn, dbi.get());
        if (result != 0) {
            System.err.println("Failed to set comparator for db: " + result);
            return;
        }

        String keyword = "Meaning of life?";
        int klen = keyword.getBytes().length;

        BytePointer key = new BytePointer(klen);
        DTLV.MDB_val kval = new DTLV.MDB_val();
        kval.mv_size(klen);
        kval.mv_data(key);

        ByteBuffer kb = key.position(0).limit(klen).asByteBuffer();
        kb.put(keyword.getBytes());

        int answer = 42;
        IntPointer value = new IntPointer(1);
        DTLV.MDB_val vval = new DTLV.MDB_val();
        vval.mv_size(4);
        vval.mv_data(value);

        ByteBuffer vb = value.position(0).limit(1).asByteBuffer();
        vb.putInt(answer);

        result = DTLV.mdb_put(txn, dbi.get(), kval, vval, 0);
        if (result != 0) {
            System.err.println("Failed to put key value: " + result);
            return;
        }

        result = DTLV.mdb_txn_commit(txn);
        if (result != 0) {
            System.err.println("Failed to commit transaction: " + result);
            return;
        }

        result = DTLV.mdb_env_sync(env, 1);
        if (result != 0) {
            System.err.println("Failed to sync: " + result);
            return;
        }

        IntPointer res = new IntPointer(1);
        DTLV.MDB_val rval = new DTLV.MDB_val();
        rval.mv_size(4);
        rval.mv_data(res);

        DTLV.MDB_txn rtxn = new DTLV.MDB_txn();
        result = DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, rtxn);
        if (result != 0) {
            System.err.println("Failed to begin a read only transaction: " + result);
            return;
        }

        result = DTLV.mdb_get(rtxn, dbi.get(), kval, rval);
        if (result != 0) {
            System.err.println("Failed to get key value: " + result);
            return;
        }

        result = DTLV.mdb_cmp(rtxn, dbi.get(), vval, rval);
        if (result != 0) {
            System.err.println("Failed to compare values: " + result);
            return;
        }

        ByteBuffer rb = rval.mv_data().limit(rval.mv_size()).asByteBuffer();

        System.out.println("Got correct value? " + (rb.getInt() == answer));

        DTLV.mdb_txn_abort(rtxn);

        DTLV.mdb_env_close(env);

        dbi.close();
        key.close();
        value.close();
        res.close();

        deleteDirectoryFiles(dir);

        System.out.println("Passed LMDB tests.");
    }

    static float[][] randomVectors(final int n, final int dimensions) {
        Random rand = new Random();
        float[][] data = new float[n][dimensions];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < dimensions; j++) {
                data[i][j] = rand.nextFloat();
            }
        }
        return data;
    }

    static DTLV.usearch_init_options_t createOpts(final long dimensions) {

        DTLV.usearch_init_options_t opts = new DTLV.usearch_init_options_t();
        opts.zero();

        opts.metric_kind(DTLV.usearch_metric_ip_k)
            .metric((usearch_metric_t) null)
            .quantization(DTLV.usearch_scalar_f32_k)
            .dimensions(dimensions)
            .connectivity(3)
            .expansion_add(40)
            .expansion_search(16)
            .multi(false);
        return opts;
    }

    static void expect(boolean must_be_true, String message) {
        if (must_be_true)
            return;

        message = (message != null ? message : "Unit test failed");
        System.out.println(message);
        System.exit(-1);
    }

    static void expectNoError(PointerPointer<BytePointer> error, String message) {

        BytePointer errPtr = error.get(BytePointer.class);
        if (errPtr != null) {
            String msg = errPtr.getString();
            System.out.println(message + ": " + msg);
            System.exit(-1);
        }
    }

    static void testUsearchInit(int collSize, int dimensions) {
        try {
            DTLV.usearch_init_options_t opts = createOpts(dimensions);
            opts.position(0);
            opts.limit(opts.capacity());

            expect(opts.metric_kind() == DTLV.usearch_metric_ip_k, "fail to get metric_kind");
            expect(opts.quantization() == DTLV.usearch_scalar_f32_k, "fail to get quantization");
            expect(opts.connectivity() == 3, "fail to get connectivity");
            expect(opts.dimensions() == dimensions, "fail to get dimensions");
            expect(opts.expansion_add() == 40, "fail to get expansion_add");
            expect(opts.expansion_search() == 16, "fail to get expansion_search");
            expect(opts.multi() == false, "fail to get multi");

            PointerPointer<BytePointer> error = new PointerPointer<BytePointer>(1);
            error.put(new BytePointer[] { null });

            System.gc();
            Thread.sleep(100);

            System.out.println("About to call usearch_init");
            opts.position(0);
            DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
            System.out.println("Successfully called init");
            expect(index != null, "Failed to init index");

            error.put(0, (BytePointer) null);
            DTLV.usearch_free(index, error);
            expectNoError(error, "Fail to free index");

            error.put(0, (BytePointer) null);
            index = DTLV.usearch_init(opts, error);
            expect(index != null, "Failed to init index");

            error.put(0, (BytePointer) null);
            long size = DTLV.usearch_size(index, error);
            expect(size == 0, "Failed to get index size");

            error.put(0, (BytePointer) null);
            long capacity = DTLV.usearch_capacity(index, error);
            expect(capacity == 0, "Failed to get index capacity");

            error.put(0, (BytePointer) null);
            long dims = DTLV.usearch_dimensions(index, error);
            expect(dimensions == dims, "Failed to get index dimensions");

            error.put(0, (BytePointer) null);
            long connectivity = DTLV.usearch_connectivity(index, error);
            expect(connectivity == opts.connectivity(),
                    "Failed to get index connectivity");

            error.put(0, (BytePointer) null);
            DTLV.usearch_reserve(index, collSize, error);
            expectNoError(error, "Fail to reserve");

            error.put(0, (BytePointer) null);
            size = DTLV.usearch_size(index, error);
            expect(size == 0, "Failed to get index size");

            error.put(0, (BytePointer) null);
            capacity = DTLV.usearch_capacity(index, error);
            expect(capacity >= collSize, "Failed to get index capacity");

            error.put(0, (BytePointer) null);
            dims = DTLV.usearch_dimensions(index, error);
            expect(dimensions == dims, "Failed to get index dimensions");

            error.put(0, (BytePointer) null);
            connectivity = DTLV.usearch_connectivity(index, error);
            expect(connectivity == opts.connectivity(),
                    "Failed to get index connectivity");

            error.put(0, (BytePointer) null);
            BytePointer hardware = DTLV.usearch_hardware_acceleration(index, error);
            expectNoError(error, "Fail to get hardware");
            System.out.println("SIMD Hardware ISA Name is: " + hardware.getString());

            error.put(0, (BytePointer) null);
            long memory = DTLV.usearch_memory_usage(index, error);
            expect(memory > 0, "Failed to get memory usage");
            System.out.println("Memory Usage is: " + memory);

            error.put(0, (BytePointer) null);
            DTLV.usearch_free(index, error);
            expectNoError(error, "Fail to free index");
        } catch (Exception e) {
            System.out.println("Java exception: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Passed init.");
    }


    static void testUsearchAdd(int collSize, int dimensions) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        error.put(0, (BytePointer) null);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);

        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, (long)i, vecPtr, DTLV.usearch_scalar_f32_k, error);
            expectNoError(error, "Fail to add vector");
        }

        error.put(0, (BytePointer) null);
        long size = DTLV.usearch_size(index, error);
        expect(size == collSize, "Failed to get index size");

        error.put(0, (BytePointer) null);
        long capacity = DTLV.usearch_capacity(index, error);
        expect(capacity >= collSize, "Failed to get index capacity");

        for (int i = 0; i < collSize; i++) {
            error.put(0, (BytePointer) null);
            expect(DTLV.usearch_contains(index, (long) i, error),
                    "Failed to find key in index");
        }
        error.put(0, (BytePointer) null);
        expect(!DTLV.usearch_contains(index, (long) -1, error),
                "Found non existing key in index");

        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);
        System.out.println("Passed add.");
    }

    static void testUsearchFind(int collSize, int dimensions) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        error.put(0, (BytePointer) null);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, (long) i, vecPtr, DTLV.usearch_scalar_f32_k, error);
            expectNoError(error, "Fail to add vector");
        }

        LongPointer keys = new LongPointer(new long[collSize]);
        FloatPointer distances = new FloatPointer(new float[collSize]);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            error.put(0, (BytePointer) null);
            long found = DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f32_k,
                                             (long)collSize, keys, distances, error);
            expectNoError(error, "Fail to search");
            expect(found >= 1 && found <= collSize, "Vector cannot be found");
        }

        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);
        System.out.println("Passed find.");
    }

    static void testUsearchGet(int collSize, int dimensions) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        opts.multi(true);
        error.put(0, (BytePointer) null);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        long key = 1;
        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f32_k, error);
            expectNoError(error, "Fail to add vector");
        }

        float[] vectors = new float[collSize * dimensions];
        FloatPointer vPtr= new FloatPointer(vectors);
        error.put(0, (BytePointer) null);
        long found = DTLV.usearch_get(index, key, (long) collSize, vPtr,
                                      DTLV.usearch_scalar_f32_k, error);
        expectNoError(error, "Fail to get");
        expect(found == collSize, "Key is missing");

        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);

        System.out.println("Passed get.");
    }

    static void testUsearchRemove(int collSize, int dimensions) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        error.put(0, (BytePointer) null);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, (long)i, vecPtr, DTLV.usearch_scalar_f32_k, error);
            expectNoError(error, "Fail to add");
        }

        for (int i = 0; i < collSize; i++) {
            error.put(0, (BytePointer) null);
            DTLV.usearch_remove(index, (long) i, error);
            expectNoError(error, "Fail to remove");
        }

        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);
        System.out.println("Passed remove.");
    }

    static void testUsearchLoad(int collSize, int dimensions) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        DTLV.usearch_init_options_t weird_opts = createOpts(dimensions);
        weird_opts.connectivity(11)
                .expansion_add(15)
                .expansion_search(19)
                .metric_kind(DTLV.usearch_metric_pearson_k)
                .quantization(DTLV.usearch_scalar_f64_k);
        error.put(0, (BytePointer) null);
        DTLV.usearch_index_t index = DTLV.usearch_init(weird_opts, error);
        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, (long) i, vecPtr, DTLV.usearch_scalar_f32_k, error);
            expectNoError(error, "Fail to add");
        }

        String dir = "us";

        error.put(0, (BytePointer) null);
        DTLV.usearch_save(index, dir, error);
        expectNoError(error, "Fail to save");
        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);

        error.put(0, (BytePointer) null);
        index = DTLV.usearch_init(null, error);
        expectNoError(error, "Fail to init");

        error.put(0, (BytePointer) null);
        DTLV.usearch_load(index, "us", error);
        expectNoError(error, "Fail to load");

        error.put(0, (BytePointer) null);
        long size = DTLV.usearch_size(index, error);
        expect(size == collSize, "Failed to get index size");
        error.put(0, (BytePointer) null);
        long capacity = DTLV.usearch_capacity(index, error);
        expect(capacity == collSize, "Failed to get index capacity");
        error.put(0, (BytePointer) null);
        long dims = DTLV.usearch_dimensions(index, error);
        expect(dimensions == dims, "Failed to get index dimensions");
        error.put(0, (BytePointer) null);
        long connectivity = DTLV.usearch_connectivity(index, error);
        expect(connectivity == weird_opts.connectivity(),
                "Failed to get index connectivity" + weird_opts.connectivity());

        for (int i = 0; i < collSize; i++) {
            error.put(0, (BytePointer) null);
            expect(DTLV.usearch_contains(index, (long)i, error),
                   "Fail to find key in index");
        }

        LongPointer keys = new LongPointer(new long[collSize]);
        FloatPointer distances = new FloatPointer(new float[collSize]);

        error.put(0, (BytePointer) null);
        DTLV.usearch_change_threads_search(index, 1, error);
        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            error.put(0, (BytePointer) null);
            long found = DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f32_k,
                                             (long)collSize, keys, distances, error);
            expectNoError(error, "Fail to search");
            expect(found >= 1 && found <= collSize, "Vector cannot be found");
        }

        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);

        deleteDirectoryFiles(dir);

        System.out.println("Passed load.");
    }

    static void testUsearchView(int collSize, int dimensions) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        error.put(0, (BytePointer) null);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            error.put(0, (BytePointer) null);
            DTLV.usearch_add(index, (long)i, vecPtr, DTLV.usearch_scalar_f32_k, error);
            expectNoError(error, "Fail to add");
        }

        String dir = "us";

        error.put(0, (BytePointer) null);
        DTLV.usearch_save(index, dir, error);
        expectNoError(error, "Fail to save");

        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);

        error.put(0, (BytePointer) null);
        index = DTLV.usearch_init(opts, error);

        error.put(0, (BytePointer) null);
        DTLV.usearch_view(index, dir, error);
        expectNoError(error, "Fail to view");

        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);

        deleteDirectoryFiles(dir);

        System.out.println("Passed view.");
    }

    static void testUsearch() {
        System.err.println("Testing usearch ...");

        int[] collSizes = { 11, 512 };
        int[] dims = { 83, 2 };

        for (int i = 0; i < collSizes.length; i++) {
            for (int j = 0; j < dims.length; j++) {
                System.err.println("Testing " + collSizes[i] + " " + dims[j]);
                testUsearchInit(collSizes[i], dims[j]);
                testUsearchAdd(collSizes[i], dims[j]);
                testUsearchFind(collSizes[i], dims[j]);
                testUsearchGet(collSizes[i], dims[j]);
                testUsearchRemove(collSizes[i], dims[j]);
                testUsearchLoad(collSizes[i], dims[j]);
                testUsearchView(collSizes[i], dims[j]);
            }
        }

        System.out.println("Passed all usearch tests.");
    }

    public static void main(String[] args) {
        testLMDB();
        System.out.println("----");
        testUsearch();
    }
}
