package datalevin.dtlvnative;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.function.LongPredicate;
import java.nio.file.*;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

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
        opts.connectivity(3)
            .dimensions(dimensions)
                .expansion_add(40)
                .expansion_search(16)
                .metric_kind(DTLV.usearch_metric_ip_k)
                .quantization(DTLV.usearch_scalar_f32_k)
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

    static void testUsearchInit(int collSize, int dimensions) {

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        System.out.println("created opts");

        DTLV.usearch_error_t error = new DTLV.usearch_error_t();
        System.out.println("created error");

        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        System.out.println("created index");
        expect(index != null, "Failed to init index");

        DTLV.usearch_free(index, error);
        // expect(error.noError(), "Failed to free index");
        System.out.println("freed index");

        index = DTLV.usearch_init(opts, error);
        expect(index != null, "Failed to init index");

        long size = DTLV.usearch_size(index, error);
        expect(size == 0, "Failed to get index size");

        long capacity = DTLV.usearch_capacity(index, error);
        expect(capacity == 0, "Failed to get index capacity");

        long dims = DTLV.usearch_dimensions(index, error);
        expect(dimensions == dims, "Failed to get index dimensions");

        long connectivity = DTLV.usearch_connectivity(index, error);
        expect(connectivity == opts.connectivity(),
                "Failed to get index connectivity");

        DTLV.usearch_reserve(index, collSize, error);
        // expect(error.noError(), "Failed to reserve");

        size = DTLV.usearch_size(index, error);
        expect(size == 0, "Failed to get index size");

        capacity = DTLV.usearch_capacity(index, error);
        expect(capacity >= collSize, "Failed to get index capacity");

        dims = DTLV.usearch_dimensions(index, error);
        expect(dimensions == dims, "Failed to get index dimensions");

        connectivity = DTLV.usearch_connectivity(index, error);
        expect(connectivity == opts.connectivity(),
                "Failed to get index connectivity");

        BytePointer hardware = DTLV.usearch_hardware_acceleration(index, error);
        // expect(error.noError(), "Failed to get hardware");
        System.out.println("SIMD Hardware ISA Name is: " + hardware.getString());

        long memory = DTLV.usearch_memory_usage(index, error);
        expect(memory > 0, "Failed to get memory usage");
        System.out.println("Memory Usage is: " + memory);

        DTLV.usearch_free(index, error);
        // expect(error.noError(), "Failed to free index");

        System.out.println("Passed init.");
    }


    static void testUsearchAdd(int collSize, int dimensions) {

        DTLV.usearch_error_t error = new DTLV.usearch_error_t();

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);

        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            DTLV.usearch_add(index, (long)i, vecPtr, DTLV.usearch_scalar_f32_k, error);
            // expect(error.noError(), "Failed to add to index");
        }

        long size = DTLV.usearch_size(index, error);
        expect(size == collSize, "Failed to get index size");

        long capacity = DTLV.usearch_capacity(index, error);
        expect(capacity >= collSize, "Failed to get index capacity");

        for (int i = 0; i < collSize; i++) {
            expect(DTLV.usearch_contains(index, (long) i, error),
                    "Failed to find key in index");
        }
        expect(!DTLV.usearch_contains(index, (long) -1, error),
                "Found non existing key in index");

        DTLV.usearch_free(index, error);
        System.out.println("Passed add.");
    }

    static void testUsearchFind(int collSize, int dimensions) {

        DTLV.usearch_error_t error = new DTLV.usearch_error_t();

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            DTLV.usearch_add(index, (long) i, vecPtr, DTLV.usearch_scalar_f32_k, error);
            // expect(error.noError(), "Failed to add to index");
        }

        LongPointer keys = new LongPointer(new long[collSize]);
        FloatPointer distances = new FloatPointer(new float[collSize]);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            long found = DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f32_k,
                                             (long)collSize, keys, distances, error);
            // expect(error.noError(), "Failed to search index");
            expect(found >= 1 && found <= collSize, "Vector cannot be found");
        }

        DTLV.usearch_free(index, error);
        System.out.println("Passed find.");
    }

    static void testUsearchGet(int collSize, int dimensions) {

        DTLV.usearch_error_t error = new DTLV.usearch_error_t();

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        opts.multi(true);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        long key = 1;
        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f32_k, error);
            // expect(error.noError(), "Failed to add to index");
        }

        float[] vectors = new float[collSize * dimensions];
        FloatPointer vPtr= new FloatPointer(vectors);
        long found = DTLV.usearch_get(index, key, (long) collSize, vPtr,
                                      DTLV.usearch_scalar_f32_k, error);
        // expect(error.noError(), "Failed to get key from index");
        expect(found == collSize, "Key is missing");

        DTLV.usearch_free(index, error);

        System.out.println("Passed get.");
    }

    static void testUsearchRemove(int collSize, int dimensions) {

        DTLV.usearch_error_t error = new DTLV.usearch_error_t();

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            DTLV.usearch_add(index, (long)i, vecPtr, DTLV.usearch_scalar_f32_k, error);
            // expect(error.noError(), "Failed to add to index");
        }

        for (int i = 0; i < collSize; i++) {
            DTLV.usearch_remove(index, (long) i, error);
            // expect(error.noError(), "Failed to remove key from index");
        }

        DTLV.usearch_free(index, error);
        System.out.println("Passed remove.");
    }

    static void testUsearchLoad(int collSize, int dimensions) {

        DTLV.usearch_error_t error = new DTLV.usearch_error_t();

        DTLV.usearch_init_options_t weird_opts = createOpts(dimensions);
        weird_opts.connectivity(11)
                .expansion_add(15)
                .expansion_search(19)
                .metric_kind(DTLV.usearch_metric_pearson_k)
                .quantization(DTLV.usearch_scalar_f64_k);
        DTLV.usearch_index_t index = DTLV.usearch_init(weird_opts, error);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            DTLV.usearch_add(index, (long) i, vecPtr, DTLV.usearch_scalar_f32_k, error);
            // expect(error.noError(), "Failed to add to index");
        }

        String dir = "us";

        DTLV.usearch_save(index, dir, error);
        // expect(error.noError(), "Failed to save index");
        DTLV.usearch_free(index, error);

        index = DTLV.usearch_init(null, error);
        // expect(error.noError(), "Failed to init index");

        DTLV.usearch_load(index, "us", error);
        // expect(error.noError(), "Failed to load index");

        long size = DTLV.usearch_size(index, error);
        expect(size == collSize, "Failed to get index size");
        long capacity = DTLV.usearch_capacity(index, error);
        expect(capacity == collSize, "Failed to get index capacity");
        long dims = DTLV.usearch_dimensions(index, error);
        expect(dimensions == dims, "Failed to get index dimensions");
        long connectivity = DTLV.usearch_connectivity(index, error);
        expect(connectivity == weird_opts.connectivity(),
                "Failed to get index connectivity" + weird_opts.connectivity());

        for (int i = 0; i < collSize; i++) {
            expect(DTLV.usearch_contains(index, (long)i, error),
                   "Fail to find key in index");
        }

        LongPointer keys = new LongPointer(new long[collSize]);
        FloatPointer distances = new FloatPointer(new float[collSize]);

        DTLV.usearch_change_threads_search(index, 1, error);
        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            long found = DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f32_k,
                                             (long)collSize, keys, distances, error);
            // expect(error.noError(), "Failed to search index");
            expect(found >= 1 && found <= collSize, "Vector cannot be found");
        }

        DTLV.usearch_free(index, error);

        deleteDirectoryFiles(dir);

        System.out.println("Passed load.");
    }

    static void testUsearchView(int collSize, int dimensions) {

        DTLV.usearch_error_t error = new DTLV.usearch_error_t();

        DTLV.usearch_init_options_t opts = createOpts(dimensions);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        DTLV.usearch_reserve(index, collSize, error);

        float[][] data = randomVectors(collSize, dimensions);

        for (int i = 0; i < collSize; i++) {
            FloatPointer vecPtr = new FloatPointer(data[i]);
            DTLV.usearch_add(index, (long)i, vecPtr, DTLV.usearch_scalar_f32_k, error);
            // expect(error.noError(), "Failed to add to index");
        }

        String dir = "us";

        DTLV.usearch_save(index, dir, error);
        // expect(error.noError(), "Failed to save index");
        DTLV.usearch_free(index, error);

        index = DTLV.usearch_init(opts, error);

        DTLV.usearch_view(index, dir, error);
        // expect(error.noError(), "Failed to view index");

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
