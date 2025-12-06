package datalevin.dtlvnative;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.function.LongPredicate;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

import datalevin.dtlvnative.DTLV.usearch_metric_t;

public class Test {

    private static int completedTests = 0;
    private static final List<Pointer> cursorKeepAlive = new ArrayList<>();

    static void fail(String message) {
        System.err.println(message);
        System.exit(-1);
    }

    static void fail(String message, Throwable t) {
        System.err.println(message);
        t.printStackTrace(System.err);
        System.exit(-1);
    }

    static void pass(String message) {
        completedTests++;
        System.out.println(message);
    }

    static void runTest(String name, Runnable test) {
        int before = completedTests;
        try {
            test.run();
        } catch (Throwable t) {
            fail("Test '" + name + "' threw an exception", t);
        }
        if (completedTests == before) {
            fail("Test '" + name + "' did not report success");
        }
    }

    static void keepAlive(Pointer p) {
        if (p != null)
            cursorKeepAlive.add(p);
    }

    private static final int MULTI_PROCESS_ENV_FLAGS = DTLV.MDB_NOLOCK;

    static void deleteDirectoryFiles(final String path) {
        if (path == null) return;
        File directory = new File(path);
        if (!directory.exists()) return;
        if (!directory.isDirectory()) {
            directory.delete();
            return;
        }

        File[] entries = directory.listFiles();
        if (entries != null) {
            for (File f : entries) {
                if (f.isDirectory()) {
                    deleteDirectoryFiles(f.getAbsolutePath());
                } else {
                    f.delete();
                }
            }
        }
        directory.delete();
    }

    static boolean directoryHasSuffix(final String path, final String suffix) {
        if (path == null || suffix == null) return false;
        File directory = new File(path);
        if (!directory.exists() || !directory.isDirectory()) return false;
        File[] entries = directory.listFiles();
        if (entries == null) return false;
        for (File entry : entries) {
            if (entry.getName().endsWith(suffix)) return true;
        }
        return false;
    }

    static void testLMDBBasic() {

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

        int envFlags = DTLV.MDB_NOLOCK;
        result = DTLV.mdb_env_open(env, dir, envFlags, 0664);
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

        pass("Passed basic LMDB test.");
    }

    static void testLMDB() {

        System.err.println("Testing LMDB ...");

        runTest("basic LMDB operations", Test::testLMDBBasic);
        runTest("counted/prefix LMDB", Test::testLMDBCountedPrefix);
        runTest("rank-based list sample iterator", Test::testRankSampleIterator);
        runTest("key rank sample iterator", Test::testKeyRankSampleIterator);
        runTest("key rank sample iterator shrink", Test::testKeyRankSampleIteratorShrink);
        runTest("key rank sample iterator on dupsort", Test::testKeyRankSampleIteratorDupsort);
        runTest("list value iterator bounds", Test::testListValIteratorBounds);

        pass("Passed LMDB tests.");
    }

    static void testLMDBCountedPrefix() {

        System.err.println("Testing counted/prefix compressed LMDB ...");

        String dir = "db-counted";

        DTLV.MDB_env env = new DTLV.MDB_env();
        IntPointer dbi = null;
        DTLV.MDB_txn txn = null;
        DTLV.MDB_txn rtxn = null;
        boolean writeTxnActive = false;
        boolean readTxnActive = false;
        boolean envCreated = false;

        List<BytePointer> keyPointers = new ArrayList<>();
        List<IntPointer> valuePointers = new ArrayList<>();

        try {
            int result = DTLV.mdb_env_create(env);
            if (result != 0) {
                System.err.println("Failed to create counted/prefix environment: " + result);
                return;
            }
            envCreated = true;

            result = DTLV.mdb_env_set_maxdbs(env, 5);
            if (result != 0) {
                System.err.println("Failed to set max dbs for counted/prefix env: " + result);
                return;
            }

            try {
                Files.createDirectories(Paths.get(dir));
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + dir);
                e.printStackTrace();
                return;
            }

            int envFlags = DTLV.MDB_NOLOCK;
            result = DTLV.mdb_env_open(env, dir, envFlags, 0664);
            if (result != 0) {
                System.err.println("Failed to open counted/prefix environment: " + result);
                return;
            }

            txn = new DTLV.MDB_txn();
            result = DTLV.mdb_txn_begin(env, null, 0, txn);
            if (result != 0) {
                System.err.println("Failed to begin counted/prefix write txn: " + result);
                return;
            }
            writeTxnActive = true;

            dbi = new IntPointer(1);
            String dbiName = "counted_prefix";
            result = DTLV.mdb_dbi_open(txn, dbiName,
                                       DTLV.MDB_CREATE | DTLV.MDB_COUNTED | DTLV.MDB_PREFIX_COMPRESSION,
                                       dbi);
            if (result != 0) {
                System.err.println("Failed to open counted/prefix dbi: " + result);
                return;
            }

            String[] keys = { "alpha:001", "alpha:002", "alpha:010" };
            int[] values = { 101, 202, 303 };
            int[] keySizes = new int[keys.length];

            for (int i = 0; i < keys.length; i++) {
                byte[] keyBytes = keys[i].getBytes();
                keySizes[i] = keyBytes.length;

                BytePointer keyPtr = new BytePointer(keyBytes.length);
                ByteBuffer keyBuffer = keyPtr.position(0).limit(keyBytes.length).asByteBuffer();
                keyBuffer.put(keyBytes);
                keyPtr.position(0);

                IntPointer valuePtr = new IntPointer(1);
                ByteBuffer valueBuffer = valuePtr.position(0).limit(1).asByteBuffer();
                valueBuffer.putInt(values[i]);
                valuePtr.position(0);

                DTLV.MDB_val kval = new DTLV.MDB_val();
                kval.mv_size(keySizes[i]);
                kval.mv_data(keyPtr);

                DTLV.MDB_val vval = new DTLV.MDB_val();
                vval.mv_size(4);
                vval.mv_data(valuePtr);

                result = DTLV.mdb_put(txn, dbi.get(), kval, vval, 0);
                if (result != 0) {
                    System.err.println("Failed to put key/value into counted/prefix db: " + result);
                    return;
                }

                keyPointers.add(keyPtr);
                valuePointers.add(valuePtr);
            }

            result = DTLV.mdb_txn_commit(txn);
            writeTxnActive = false;
            if (result != 0) {
                System.err.println("Failed to commit counted/prefix txn: " + result);
                return;
            }

            result = DTLV.mdb_env_sync(env, 1);
            if (result != 0) {
                System.err.println("Failed to sync counted/prefix env: " + result);
                return;
            }

            for (IntPointer vp : valuePointers)
                vp.close();
            valuePointers.clear();

            rtxn = new DTLV.MDB_txn();
            result = DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, rtxn);
            if (result != 0) {
                System.err.println("Failed to begin counted/prefix read txn: " + result);
                return;
            }
            readTxnActive = true;

            for (int i = 0; i < keys.length; i++) {
                BytePointer keyPtr = keyPointers.get(i);
                keyPtr.position(0);

                DTLV.MDB_val kval = new DTLV.MDB_val();
                kval.mv_size(keySizes[i]);
                kval.mv_data(keyPtr);

                DTLV.MDB_val rval = new DTLV.MDB_val();

                result = DTLV.mdb_get(rtxn, dbi.get(), kval, rval);
                if (result != 0) {
                    System.err.println("Failed to get value for key " + keys[i] + ": " + result);
                    return;
                }

                ByteBuffer data = rval.mv_data().position(0).limit(rval.mv_size()).asByteBuffer();
                int readValue = data.getInt(0);
                expect(readValue == values[i], "Counted/prefix db read mismatch for key " + keys[i]);
            }

            long[] total = new long[1];
            result = DTLV.mdb_count_all(rtxn, dbi.get(), 0, total);
            if (result != 0) {
                System.err.println("Failed to count entries in counted/prefix db: " + result);
                return;
            }
            expect(total[0] == keys.length, "Counted/prefix db total mismatch");

            DTLV.mdb_txn_abort(rtxn);
            readTxnActive = false;

            pass("Passed counted/prefix LMDB test.");
        } finally {
            if (readTxnActive && rtxn != null)
                DTLV.mdb_txn_abort(rtxn);
            if (writeTxnActive && txn != null)
                DTLV.mdb_txn_abort(txn);

            for (IntPointer vp : valuePointers)
                vp.close();
            for (BytePointer kp : keyPointers)
                kp.close();

            if (dbi != null)
                dbi.close();

            if (envCreated)
                DTLV.mdb_env_close(env);
            deleteDirectoryFiles(dir);
        }
    }

    static void testListValIteratorBounds() {

        System.err.println("Testing list value iterator bounds ...");

        String dir = "db-list-val-iter";
        List<BytePointer> allocations = new ArrayList<>();

        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.MDB_txn txn = new DTLV.MDB_txn();
        DTLV.MDB_txn rtxn = new DTLV.MDB_txn();
        DTLV.MDB_cursor cursor = new DTLV.MDB_cursor();
        keepAlive(cursor);
        DTLV.dtlv_list_val_iter iter = null;
        IntPointer dbi = new IntPointer(1);

        boolean envCreated = false;
        boolean writeTxnActive = false;
        boolean readTxnActive = false;
        boolean cursorOpened = false;

        try {
            int result = DTLV.mdb_env_create(env);
            if (result != 0) {
                System.err.println("Failed to create list value iterator env: " + result);
                return;
            }
            envCreated = true;

            result = DTLV.mdb_env_set_maxdbs(env, 5);
            if (result != 0) {
                System.err.println("Failed to set max dbs for list value iterator env: " + result);
                return;
            }

            try {
                Files.createDirectories(Paths.get(dir));
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + dir);
                e.printStackTrace();
                return;
            }

            int envFlags = DTLV.MDB_NOLOCK;
            result = DTLV.mdb_env_open(env, dir, envFlags, 0664);
            if (result != 0) {
                System.err.println("Failed to open list value iterator env: " + result);
                return;
            }

            result = DTLV.mdb_txn_begin(env, null, 0, txn);
            if (result != 0) {
                System.err.println("Failed to begin list value iterator write txn: " + result);
                return;
            }
            writeTxnActive = true;

            int flags = DTLV.MDB_CREATE | DTLV.MDB_DUPSORT;
            result = DTLV.mdb_dbi_open(txn, "list_values", flags, dbi);
            if (result != 0) {
                System.err.println("Failed to open list value iterator dbi: " + result);
                return;
            }

            DTLV.MDB_val kval = new DTLV.MDB_val();
            fillValWithString(kval, "alpha", allocations);

            String[] values = { "aa", "ab", "ac", "ad" };
            for (String valueText : values) {
                DTLV.MDB_val vval = new DTLV.MDB_val();
                fillValWithString(vval, valueText, allocations);
                result = DTLV.mdb_put(txn, dbi.get(), kval, vval, 0);
                if (result != 0) {
                    System.err.println("Failed to put list value iterator data: " + result);
                    return;
                }
            }

            result = DTLV.mdb_txn_commit(txn);
            if (result != 0) {
                System.err.println("Failed to commit list value iterator data: " + result);
                return;
            }
            writeTxnActive = false;

            result = DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, rtxn);
            if (result != 0) {
                System.err.println("Failed to begin list value iterator read txn: " + result);
                return;
            }
            readTxnActive = true;

            result = DTLV.mdb_cursor_open(rtxn, dbi.get(), cursor);
            if (result != 0) {
                System.err.println("Failed to open list value iterator cursor: " + result);
                return;
            }
            cursorOpened = true;

            DTLV.MDB_val keyHolder = new DTLV.MDB_val();
            DTLV.MDB_val valHolder = new DTLV.MDB_val();

            DTLV.MDB_val startVal = new DTLV.MDB_val();
            fillValWithString(startVal, "ab", allocations);
            DTLV.MDB_val endVal = new DTLV.MDB_val();
            fillValWithString(endVal, "ac", allocations);

            iter = new DTLV.dtlv_list_val_iter();
            result = DTLV.dtlv_list_val_iter_create(
                iter, cursor, keyHolder, valHolder, startVal, endVal);
            if (result != 0) {
                System.err.println("Failed to create list value iterator: " + result);
                return;
            }

            DTLV.MDB_val seekKey = new DTLV.MDB_val();
            fillValWithString(seekKey, "alpha", allocations);

            int seekResult = DTLV.dtlv_list_val_iter_seek(iter, seekKey);
            expect(seekResult == DTLV.DTLV_TRUE,
                   "List value iterator should position on inclusive start value");

            List<String> valuesInRange = new ArrayList<>();
            valuesInRange.add(mdbValToString(valHolder));

            int iterResult;
            while ((iterResult = DTLV.dtlv_list_val_iter_has_next(iter)) == DTLV.DTLV_TRUE) {
                valuesInRange.add(mdbValToString(valHolder));
            }
            expect(iterResult == DTLV.DTLV_FALSE,
                   "List value iterator should stop after inclusive end");

            List<String> expected = Arrays.asList("ab", "ac");
            expect(valuesInRange.equals(expected),
                   "List value iterator bounds mismatch: " + valuesInRange);

            pass("Passed list value iterator bounds test.");
        } finally {
            if (iter != null)
                DTLV.dtlv_list_val_iter_destroy(iter);
            if (cursorOpened)
                DTLV.mdb_cursor_close(cursor);
            if (readTxnActive)
                DTLV.mdb_txn_abort(rtxn);
            if (writeTxnActive)
                DTLV.mdb_txn_abort(txn);
            dbi.close();
            if (envCreated)
                DTLV.mdb_env_close(env);
            for (BytePointer ptr : allocations)
                ptr.close();
            deleteDirectoryFiles(dir);
        }
    }

    static void testRankSampleIterator() {

        System.err.println("Testing rank-based list sample iterator ...");

        String dir = "db-rank-sample";
        List<BytePointer> allocations = new ArrayList<>();

        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.MDB_txn txn = new DTLV.MDB_txn();
        DTLV.MDB_txn rtxn = new DTLV.MDB_txn();
        DTLV.MDB_cursor cursor = new DTLV.MDB_cursor();
        keepAlive(cursor);
        IntPointer dbi = new IntPointer(1);

        boolean envCreated = false;
        boolean writeTxnActive = false;
        boolean readTxnActive = false;
        boolean cursorOpened = false;

        try {
            int result = DTLV.mdb_env_create(env);
            if (result != 0) {
                System.err.println("Failed to create rank sample env: " + result);
                return;
            }
            envCreated = true;

            result = DTLV.mdb_env_set_maxdbs(env, 5);
            if (result != 0) {
                System.err.println("Failed to set max dbs for rank sample env: " + result);
                return;
            }

            try {
                Files.createDirectories(Paths.get(dir));
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + dir);
                e.printStackTrace();
                return;
            }

            int envFlags = DTLV.MDB_NOLOCK;
            result = DTLV.mdb_env_open(env, dir, envFlags, 0664);
            if (result != 0) {
                System.err.println("Failed to open rank sample env: " + result);
                return;
            }

            result = DTLV.mdb_txn_begin(env, null, 0, txn);
            if (result != 0) {
                System.err.println("Failed to begin rank sample write txn: " + result);
                return;
            }
            writeTxnActive = true;

            int flags = DTLV.MDB_CREATE | DTLV.MDB_DUPSORT | DTLV.MDB_COUNTED | DTLV.MDB_PREFIX_COMPRESSION;
            result = DTLV.mdb_dbi_open(txn, "rank_sample", flags, dbi);
            if (result != 0) {
                System.err.println("Failed to open rank sample dbi: " + result);
                return;
            }

            String[][] dataset = {
                { "alpha", "aa" },
                { "alpha", "ab" },
                { "alpha", "ac" },
                { "bravo", "aa" },
                { "bravo", "ab" },
                { "charlie", "aa" }
            };

            List<String> orderedEntries = new ArrayList<>();

            for (String[] pair : dataset) {
                DTLV.MDB_val kval = new DTLV.MDB_val();
                fillValWithString(kval, pair[0], allocations);
                DTLV.MDB_val vval = new DTLV.MDB_val();
                fillValWithString(vval, pair[1], allocations);
                result = DTLV.mdb_put(txn, dbi.get(), kval, vval, 0);
                if (result != 0) {
                    System.err.println("Failed to put rank sample kv: " + result);
                    return;
                }
                orderedEntries.add(pair[0] + ":" + pair[1]);
            }

            result = DTLV.mdb_txn_commit(txn);
            if (result != 0) {
                System.err.println("Failed to commit rank sample data: " + result);
                return;
            }
            writeTxnActive = false;

            result = DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, rtxn);
            if (result != 0) {
                System.err.println("Failed to begin rank sample read txn: " + result);
                return;
            }
            readTxnActive = true;

            result = DTLV.mdb_cursor_open(rtxn, dbi.get(), cursor);
            if (result != 0) {
                System.err.println("Failed to open rank sample cursor: " + result);
                return;
            }
            cursorOpened = true;

            DTLV.MDB_val keyHolder = new DTLV.MDB_val();
            DTLV.MDB_val valHolder = new DTLV.MDB_val();
            final long unlimitedBudget = 1_000_000L;
            final long budgetStep = 1L;

            long[] baseSample = { 0L, 3L, 5L };
            SizeTPointer indices = toSizeTPointer(baseSample);
            List<String> fullRankSamples = new ArrayList<>();

            DTLV.dtlv_list_rank_sample_iter iter =
                new DTLV.dtlv_list_rank_sample_iter();
            result = DTLV.dtlv_list_rank_sample_iter_create(
                iter, indices, baseSample.length,
                cursor, keyHolder, valHolder,
                null, null);
            if (result != 0) {
                System.err.println("Failed to create full-range rank iterator: " + result);
                indices.close();
                return;
            }
            for (int i = 0; i < baseSample.length; i++) {
                int hasNext = DTLV.dtlv_list_rank_sample_iter_has_next(iter);
                expect(hasNext == DTLV.DTLV_TRUE, "Rank iterator missing expected sample");
                String actual = mdbValToString(keyHolder) + ":" + mdbValToString(valHolder);
                expect(actual.equals(orderedEntries.get((int) baseSample[i])),
                       "Rank iterator produced unexpected entry");
                fullRankSamples.add(actual);
            }
            expect(DTLV.dtlv_list_rank_sample_iter_has_next(iter) == DTLV.DTLV_FALSE,
                   "Rank iterator should be exhausted");
            DTLV.dtlv_list_rank_sample_iter_destroy(iter);
            indices.close();

            SizeTPointer listIndices = toSizeTPointer(baseSample);
            DTLV.dtlv_list_sample_iter listIter =
                new DTLV.dtlv_list_sample_iter();
            result = DTLV.dtlv_list_sample_iter_create(
                listIter, listIndices, baseSample.length,
                unlimitedBudget, budgetStep,
                cursor, keyHolder, valHolder,
                null, null);
            if (result != 0) {
                System.err.println("Failed to create list iterator: " + result);
                listIndices.close();
                return;
            }
            List<String> fullListSamples = new ArrayList<>();
            for (int i = 0; i < baseSample.length; i++) {
                int hasNext = DTLV.dtlv_list_sample_iter_has_next(listIter);
                expect(hasNext == DTLV.DTLV_TRUE, "List iterator missing expected sample");
                String actual = mdbValToString(keyHolder) + ":" + mdbValToString(valHolder);
                expect(actual.equals(orderedEntries.get((int) baseSample[i])),
                       "List iterator produced unexpected entry");
                fullListSamples.add(actual);
            }
            expect(DTLV.dtlv_list_sample_iter_has_next(listIter) == DTLV.DTLV_FALSE,
                   "List iterator should be exhausted");
            DTLV.dtlv_list_sample_iter_destroy(listIter);
            listIndices.close();

            expect(fullRankSamples.equals(fullListSamples),
                   "Rank and list iterators differ on full-range samples");

            DTLV.dtlv_list_key_range_full_val_iter fullValIter =
                new DTLV.dtlv_list_key_range_full_val_iter();
            result = DTLV.dtlv_list_key_range_full_val_iter_create(
                fullValIter, cursor, keyHolder, valHolder,
                DTLV.DTLV_TRUE, DTLV.DTLV_TRUE,
                null, null);
            if (result != 0) {
                System.err.println("Failed to create full value iterator: " + result);
                return;
            }
            List<String> viaFullIter = new ArrayList<>();
            while (DTLV.dtlv_list_key_range_full_val_iter_has_next(fullValIter)
                   == DTLV.DTLV_TRUE) {
                viaFullIter.add(mdbValToString(keyHolder) + ":" + mdbValToString(valHolder));
            }
            expect(viaFullIter.equals(orderedEntries),
                   "Full value iterator produced unexpected results");
            DTLV.dtlv_list_key_range_full_val_iter_destroy(fullValIter);

            long[] boundedSample = { 0L, 1L };
            SizeTPointer bounded = toSizeTPointer(boundedSample);

            DTLV.MDB_val startKey = new DTLV.MDB_val();
            fillValWithString(startKey, "bravo", allocations);
            DTLV.MDB_val endKey = new DTLV.MDB_val();
            fillValWithString(endKey, "bravo", allocations);

            DTLV.dtlv_list_rank_sample_iter boundedIter =
                new DTLV.dtlv_list_rank_sample_iter();
            result = DTLV.dtlv_list_rank_sample_iter_create(
                boundedIter, bounded, 2, cursor, keyHolder, valHolder,
                startKey, endKey);
            if (result != 0) {
                System.err.println("Failed to create bounded rank iterator: " + result);
                bounded.close();
                return;
            }

            String[] expectedBounded = { "bravo:aa", "bravo:ab" };
            List<String> boundedRankSamples = new ArrayList<>();
            for (String expected : expectedBounded) {
                int hasNext = DTLV.dtlv_list_rank_sample_iter_has_next(boundedIter);
                expect(hasNext == DTLV.DTLV_TRUE, "Bounded iterator missing expected sample");
                String actual = mdbValToString(keyHolder) + ":" + mdbValToString(valHolder);
                expect(actual.equals(expected),
                       "Bounded iterator produced unexpected entry");
                boundedRankSamples.add(actual);
            }
            expect(DTLV.dtlv_list_rank_sample_iter_has_next(boundedIter) == DTLV.DTLV_FALSE,
                   "Bounded iterator should be exhausted");
            DTLV.dtlv_list_rank_sample_iter_destroy(boundedIter);

            DTLV.dtlv_list_sample_iter boundedListIter =
                new DTLV.dtlv_list_sample_iter();
            result = DTLV.dtlv_list_sample_iter_create(
                boundedListIter, bounded, 2,
                unlimitedBudget, budgetStep,
                cursor, keyHolder, valHolder,
                startKey, endKey);
            if (result != 0) {
                System.err.println("Failed to create bounded list iterator: " + result);
                bounded.close();
                return;
            }
            List<String> boundedListSamples = new ArrayList<>();
            for (String expected : expectedBounded) {
                int hasNext = DTLV.dtlv_list_sample_iter_has_next(boundedListIter);
                expect(hasNext == DTLV.DTLV_TRUE, "Bounded list iterator missing expected sample");
                String actual = mdbValToString(keyHolder) + ":" + mdbValToString(valHolder);
                expect(actual.equals(expected),
                       "Bounded list iterator produced unexpected entry");
                boundedListSamples.add(actual);
            }
            expect(DTLV.dtlv_list_sample_iter_has_next(boundedListIter) == DTLV.DTLV_FALSE,
                   "Bounded list iterator should be exhausted");
            DTLV.dtlv_list_sample_iter_destroy(boundedListIter);

            expect(boundedRankSamples.equals(boundedListSamples),
                   "Rank and list iterators differ on bounded samples");
            bounded.close();

            pass("Passed rank-based list sample iterator test.");
        } finally {
            if (cursorOpened)
                DTLV.mdb_cursor_close(cursor);
            if (readTxnActive)
                DTLV.mdb_txn_abort(rtxn);
            if (writeTxnActive)
                DTLV.mdb_txn_abort(txn);
            dbi.close();
            for (BytePointer ptr : allocations)
                ptr.close();
            if (envCreated)
                DTLV.mdb_env_close(env);
            deleteDirectoryFiles(dir);
        }
    }

    static void testKeyRankSampleIteratorShrink() {

        System.err.println("Testing key rank sample iterator with shrinking data ...");

        String dir = "db-key-rank-sample-shrink";
        List<BytePointer> allocations = new ArrayList<>();

        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.MDB_txn txn = new DTLV.MDB_txn();
        DTLV.MDB_cursor cursor = new DTLV.MDB_cursor();
        keepAlive(cursor);
        IntPointer dbi = new IntPointer(1);
        SizeTPointer indices = null;
        DTLV.dtlv_key_rank_sample_iter iter = null;
        boolean iterCreated = false;

        boolean envCreated = false;
        boolean writeTxnActive = false;
        boolean cursorOpened = false;

        try {
            int result = DTLV.mdb_env_create(env);
            if (result != 0) {
                System.err.println("Failed to create shrink test env: " + result);
                return;
            }
            envCreated = true;

            result = DTLV.mdb_env_set_maxdbs(env, 5);
            if (result != 0) {
                System.err.println("Failed to set max dbs for shrink test: " + result);
                return;
            }

            try {
                Files.createDirectories(Paths.get(dir));
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + dir);
                e.printStackTrace();
                return;
            }

            int envFlags = DTLV.MDB_NOLOCK;
            result = DTLV.mdb_env_open(env, dir, envFlags, 0664);
            if (result != 0) {
                System.err.println("Failed to open shrink test env: " + result);
                return;
            }

            result = DTLV.mdb_txn_begin(env, null, 0, txn);
            if (result != 0) {
                System.err.println("Failed to begin shrink test write txn: " + result);
                return;
            }
            writeTxnActive = true;

            int flags = DTLV.MDB_CREATE | DTLV.MDB_COUNTED;
            result = DTLV.mdb_dbi_open(txn, "key_rank_sample_shrink", flags, dbi);
            if (result != 0) {
                System.err.println("Failed to open shrink test dbi: " + result);
                return;
            }

            String[][] dataset = {
                { "alpha", "v1" },
                { "bravo", "v2" },
                { "charlie", "v3" }
            };

            for (String[] pair : dataset) {
                DTLV.MDB_val kval = new DTLV.MDB_val();
                fillValWithString(kval, pair[0], allocations);
                DTLV.MDB_val vval = new DTLV.MDB_val();
                fillValWithString(vval, pair[1], allocations);
                result = DTLV.mdb_put(txn, dbi.get(), kval, vval, 0);
                if (result != 0) {
                    System.err.println("Failed to put shrink test kv: " + result);
                    return;
                }
            }

            result = DTLV.mdb_cursor_open(txn, dbi.get(), cursor);
            if (result != 0) {
                System.err.println("Failed to open shrink test cursor: " + result);
                return;
            }
            cursorOpened = true;

            long[] sample = { 0L, 1L };
            indices = toSizeTPointer(sample);

            DTLV.MDB_val keyHolder = new DTLV.MDB_val();
            DTLV.MDB_val valHolder = new DTLV.MDB_val();

            iter = new DTLV.dtlv_key_rank_sample_iter();
            result = DTLV.dtlv_key_rank_sample_iter_create(
                iter, indices, sample.length,
                cursor, keyHolder, valHolder,
                null, null);
            if (result != 0) {
                System.err.println("Failed to create shrink test iterator: " + result);
                return;
            }
            iterCreated = true;

            for (String[] pair : dataset) {
                DTLV.MDB_val deleteKey = new DTLV.MDB_val();
                fillValWithString(deleteKey, pair[0], allocations);
                result = DTLV.mdb_del(txn, dbi.get(), deleteKey, null);
                expect(result == 0, "Failed to delete key during shrink test");
            }

            long[] remaining = new long[1];
            result = DTLV.mdb_count_all(txn, dbi.get(), 0, remaining);
            expect(result == 0, "Failed to count entries after shrink");
            expect(remaining[0] == 0, "Shrink test did not remove all entries");

            int hasNext = DTLV.dtlv_key_rank_sample_iter_has_next(iter);
            expect(hasNext == DTLV.DTLV_FALSE,
                   "Key rank iterator should treat missing rank as exhausted after shrink");
            expect(DTLV.dtlv_key_rank_sample_iter_has_next(iter) == DTLV.DTLV_FALSE,
                   "Key rank iterator should stay exhausted after shrink");

            DTLV.dtlv_key_rank_sample_iter_destroy(iter);
            iter = null;
            iterCreated = false;

            if (indices != null) {
                indices.close();
                indices = null;
            }

            result = DTLV.mdb_txn_commit(txn);
            if (result != 0) {
                System.err.println("Failed to commit shrink test txn: " + result);
                return;
            }
            writeTxnActive = false;
            cursorOpened = false;

            pass("Passed key rank sample iterator shrink test.");
        } finally {
            if (cursorOpened)
                DTLV.mdb_cursor_close(cursor);
            if (writeTxnActive)
                DTLV.mdb_txn_abort(txn);
            if (indices != null)
                indices.close();
            if (iterCreated && iter != null)
                DTLV.dtlv_key_rank_sample_iter_destroy(iter);
            dbi.close();
            for (BytePointer ptr : allocations)
                ptr.close();
            if (envCreated)
                DTLV.mdb_env_close(env);
            deleteDirectoryFiles(dir);
        }
    }

    static void testKeyRankSampleIterator() {

        System.err.println("Testing key rank sample iterator ...");

        String dir = "db-key-rank-sample";
        List<BytePointer> allocations = new ArrayList<>();

        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.MDB_txn txn = new DTLV.MDB_txn();
        DTLV.MDB_txn rtxn = new DTLV.MDB_txn();
        DTLV.MDB_cursor cursor = new DTLV.MDB_cursor();
        keepAlive(cursor);
        IntPointer dbi = new IntPointer(1);

        boolean envCreated = false;
        boolean writeTxnActive = false;
        boolean readTxnActive = false;
        boolean cursorOpened = false;

        try {
            int result = DTLV.mdb_env_create(env);
            if (result != 0) {
                System.err.println("Failed to create key rank sample env: " + result);
                return;
            }
            envCreated = true;

            result = DTLV.mdb_env_set_maxdbs(env, 5);
            if (result != 0) {
                System.err.println("Failed to set max dbs for key rank sample env: " + result);
                return;
            }

            try {
                Files.createDirectories(Paths.get(dir));
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + dir);
                e.printStackTrace();
                return;
            }

            int envFlags = DTLV.MDB_NOLOCK;
            result = DTLV.mdb_env_open(env, dir, envFlags, 0664);
            if (result != 0) {
                System.err.println("Failed to open key rank sample env: " + result);
                return;
            }

            result = DTLV.mdb_txn_begin(env, null, 0, txn);
            if (result != 0) {
                System.err.println("Failed to begin key rank sample write txn: " + result);
                return;
            }
            writeTxnActive = true;

            int flags = DTLV.MDB_CREATE | DTLV.MDB_COUNTED;
            result = DTLV.mdb_dbi_open(txn, "key_rank_sample", flags, dbi);
            if (result != 0) {
                System.err.println("Failed to open key rank sample dbi: " + result);
                return;
            }

            String[][] dataset = {
                { "alpha", "v1" },
                { "bravo", "v2" },
                { "charlie", "v3" },
                { "delta", "v4" },
                { "echo", "v5" }
            };

            List<String> orderedKeys = new ArrayList<>();

            for (String[] pair : dataset) {
                DTLV.MDB_val kval = new DTLV.MDB_val();
                fillValWithString(kval, pair[0], allocations);
                DTLV.MDB_val vval = new DTLV.MDB_val();
                fillValWithString(vval, pair[1], allocations);
                result = DTLV.mdb_put(txn, dbi.get(), kval, vval, 0);
                if (result != 0) {
                    System.err.println("Failed to put key rank sample kv: " + result);
                    return;
                }
                orderedKeys.add(pair[0]);
            }

            result = DTLV.mdb_txn_commit(txn);
            if (result != 0) {
                System.err.println("Failed to commit key rank sample data: " + result);
                return;
            }
            writeTxnActive = false;

            result = DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, rtxn);
            if (result != 0) {
                System.err.println("Failed to begin key rank sample read txn: " + result);
                return;
            }
            readTxnActive = true;

            result = DTLV.mdb_cursor_open(rtxn, dbi.get(), cursor);
            if (result != 0) {
                System.err.println("Failed to open key rank sample cursor: " + result);
                return;
            }
            cursorOpened = true;

            DTLV.MDB_val keyHolder = new DTLV.MDB_val();
            DTLV.MDB_val valHolder = new DTLV.MDB_val();
            final long unlimitedBudget = 1_000_000L;
            final long budgetStep = 1L;

            LongPointer keyCount = new LongPointer(1);
            result = DTLV.mdb_range_count_keys(rtxn, dbi.get(), null, null, 0, keyCount);
            expect(result == 0, "Key range count (full) failed");
            expect(keyCount.get() == orderedKeys.size(),
                   "Key range count (full) returned unexpected total");

            long[] baseSample = { 0L, 2L, 4L };
            SizeTPointer indices = toSizeTPointer(baseSample);
            List<String> fullRankSamples = new ArrayList<>();

            DTLV.dtlv_key_rank_sample_iter iter =
                new DTLV.dtlv_key_rank_sample_iter();
            result = DTLV.dtlv_key_rank_sample_iter_create(
                iter, indices, baseSample.length,
                cursor, keyHolder, valHolder,
                null, null);
            if (result != 0) {
                System.err.println("Failed to create full-range key rank iterator: " + result);
                indices.close();
                return;
            }
            for (int i = 0; i < baseSample.length; i++) {
                int hasNext = DTLV.dtlv_key_rank_sample_iter_has_next(iter);
                expect(hasNext == DTLV.DTLV_TRUE, "Key rank iterator missing expected sample");
                String actual = mdbValToString(keyHolder);
                expect(actual.equals(orderedKeys.get((int) baseSample[i])),
                       "Key rank iterator produced unexpected entry");
                fullRankSamples.add(actual);
            }
            expect(DTLV.dtlv_key_rank_sample_iter_has_next(iter) == DTLV.DTLV_FALSE,
                   "Key rank iterator should be exhausted");
            DTLV.dtlv_key_rank_sample_iter_destroy(iter);
            indices.close();

            SizeTPointer keySampleIndices = toSizeTPointer(baseSample);
            DTLV.dtlv_key_sample_iter keySampleIter =
                new DTLV.dtlv_key_sample_iter();
            result = DTLV.dtlv_key_sample_iter_create(
                keySampleIter, keySampleIndices, baseSample.length,
                unlimitedBudget, budgetStep,
                cursor, keyHolder, valHolder,
                DTLV.DTLV_TRUE, DTLV.DTLV_TRUE, DTLV.DTLV_TRUE,
                null, null);
            if (result != 0) {
                System.err.println("Failed to create key sample iterator: " + result);
                keySampleIndices.close();
                return;
            }
            List<String> fullKeySamples = new ArrayList<>();
            for (int i = 0; i < baseSample.length; i++) {
                int hasNext = DTLV.dtlv_key_sample_iter_has_next(keySampleIter);
                expect(hasNext == DTLV.DTLV_TRUE, "Key sample iterator missing expected sample");
                String actual = mdbValToString(keyHolder);
                expect(actual.equals(orderedKeys.get((int) baseSample[i])),
                       "Key sample iterator produced unexpected entry");
                fullKeySamples.add(actual);
            }
            expect(DTLV.dtlv_key_sample_iter_has_next(keySampleIter) == DTLV.DTLV_FALSE,
                   "Key sample iterator should be exhausted");
            DTLV.dtlv_key_sample_iter_destroy(keySampleIter);
            keySampleIndices.close();

            expect(fullRankSamples.equals(fullKeySamples),
                   "Key rank and list iterators differ on full-range samples");

            long[] boundedSample = { 0L, 2L };
            SizeTPointer bounded = toSizeTPointer(boundedSample);

            DTLV.MDB_val startKey = new DTLV.MDB_val();
            fillValWithString(startKey, "bravo", allocations);
            DTLV.MDB_val endKey = new DTLV.MDB_val();
            fillValWithString(endKey, "delta", allocations);

            LongPointer boundedKeyCount = new LongPointer(1);
            int inclusiveFlags = DTLV.MDB_COUNT_LOWER_INCL | DTLV.MDB_COUNT_UPPER_INCL;
            result = DTLV.mdb_range_count_keys(rtxn, dbi.get(), startKey, endKey, inclusiveFlags, boundedKeyCount);
            expect(result == 0, "Key range count (bounded) failed");
            expect(boundedKeyCount.get() == 3,
                   "Key range count (bounded) returned unexpected total");

            DTLV.dtlv_key_rank_sample_iter boundedIter =
                new DTLV.dtlv_key_rank_sample_iter();
            result = DTLV.dtlv_key_rank_sample_iter_create(
                boundedIter, bounded, 2, cursor, keyHolder, valHolder,
                startKey, endKey);
            if (result != 0) {
                System.err.println("Failed to create bounded key rank iterator: " + result);
                bounded.close();
                return;
            }

            String[] expectedBounded = { "bravo", "delta" };
            List<String> boundedRankSamples = new ArrayList<>();
            for (String expected : expectedBounded) {
                int hasNext = DTLV.dtlv_key_rank_sample_iter_has_next(boundedIter);
                expect(hasNext == DTLV.DTLV_TRUE, "Bounded key iterator missing expected sample");
                String actual = mdbValToString(keyHolder);
                expect(actual.equals(expected),
                       "Bounded key iterator produced unexpected entry");
                boundedRankSamples.add(actual);
            }
            expect(DTLV.dtlv_key_rank_sample_iter_has_next(boundedIter) == DTLV.DTLV_FALSE,
                   "Bounded key iterator should be exhausted");
            DTLV.dtlv_key_rank_sample_iter_destroy(boundedIter);

            DTLV.dtlv_key_sample_iter boundedKeyIter =
                new DTLV.dtlv_key_sample_iter();
            result = DTLV.dtlv_key_sample_iter_create(
                boundedKeyIter, bounded, 2,
                unlimitedBudget, budgetStep,
                cursor, keyHolder, valHolder,
                DTLV.DTLV_TRUE, DTLV.DTLV_TRUE, DTLV.DTLV_TRUE,
                startKey, endKey);
            if (result != 0) {
                System.err.println("Failed to create bounded key sample iterator: " + result);
                bounded.close();
                return;
            }
            List<String> boundedKeySamples = new ArrayList<>();
            for (String expected : expectedBounded) {
                int hasNext = DTLV.dtlv_key_sample_iter_has_next(boundedKeyIter);
                expect(hasNext == DTLV.DTLV_TRUE, "Bounded key sample iterator missing expected sample");
                String actual = mdbValToString(keyHolder);
                expect(actual.equals(expected),
                       "Bounded key sample iterator produced unexpected entry");
                boundedKeySamples.add(actual);
            }
            expect(DTLV.dtlv_key_sample_iter_has_next(boundedKeyIter) == DTLV.DTLV_FALSE,
                   "Bounded key sample iterator should be exhausted");
            DTLV.dtlv_key_sample_iter_destroy(boundedKeyIter);

            expect(boundedRankSamples.equals(boundedKeySamples),
                   "Key rank and list iterators differ on bounded samples");
            bounded.close();

            pass("Passed key rank sample iterator test.");
        } finally {
            if (cursorOpened)
                DTLV.mdb_cursor_close(cursor);
            if (readTxnActive)
                DTLV.mdb_txn_abort(rtxn);
            if (writeTxnActive)
                DTLV.mdb_txn_abort(txn);
            dbi.close();
            for (BytePointer ptr : allocations)
                ptr.close();
            if (envCreated)
                DTLV.mdb_env_close(env);
            deleteDirectoryFiles(dir);
        }
    }

    static void testKeyRankSampleIteratorDupsort() {

        System.err.println("Testing key rank sample iterator on dupsort ...");

        String dir = "db-key-rank-sample-dups";
        List<BytePointer> allocations = new ArrayList<>();

        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.MDB_txn txn = new DTLV.MDB_txn();
        DTLV.MDB_txn rtxn = new DTLV.MDB_txn();
        DTLV.MDB_cursor cursor = new DTLV.MDB_cursor();
        keepAlive(cursor);
        IntPointer dbi = new IntPointer(1);

        boolean envCreated = false;
        boolean writeTxnActive = false;
        boolean readTxnActive = false;
        boolean cursorOpened = false;

        try {
            int result = DTLV.mdb_env_create(env);
            if (result != 0) {
                System.err.println("Failed to create dupsort key rank env: " + result);
                return;
            }
            envCreated = true;

            result = DTLV.mdb_env_set_maxdbs(env, 5);
            if (result != 0) {
                System.err.println("Failed to set max dbs for dupsort key rank env: " + result);
                return;
            }

            try {
                Files.createDirectories(Paths.get(dir));
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + dir);
                e.printStackTrace();
                return;
            }

            int envFlags = DTLV.MDB_NOLOCK;
            result = DTLV.mdb_env_open(env, dir, envFlags, 0664);
            if (result != 0) {
                System.err.println("Failed to open dupsort key rank env: " + result);
                return;
            }

            result = DTLV.mdb_txn_begin(env, null, 0, txn);
            if (result != 0) {
                System.err.println("Failed to begin dupsort key rank write txn: " + result);
                return;
            }
            writeTxnActive = true;

            int flags = DTLV.MDB_CREATE | DTLV.MDB_COUNTED | DTLV.MDB_DUPSORT;
            result = DTLV.mdb_dbi_open(txn, "key_rank_sample_dups", flags, dbi);
            if (result != 0) {
                System.err.println("Failed to open dupsort key rank dbi: " + result);
                return;
            }

            String[][] dataset = {
                { "alpha", "a1" },
                { "alpha", "a2" },
                { "bravo", "b1" },
                { "bravo", "b2" },
                { "bravo", "b3" },
                { "charlie", "c1" }
            };

            for (String[] pair : dataset) {
                DTLV.MDB_val kval = new DTLV.MDB_val();
                fillValWithString(kval, pair[0], allocations);
                DTLV.MDB_val vval = new DTLV.MDB_val();
                fillValWithString(vval, pair[1], allocations);
                result = DTLV.mdb_put(txn, dbi.get(), kval, vval, 0);
                if (result != 0) {
                    System.err.println("Failed to put dupsort key rank kv: " + result);
                    return;
                }
            }

            result = DTLV.mdb_txn_commit(txn);
            if (result != 0) {
                System.err.println("Failed to commit dupsort key rank data: " + result);
                return;
            }
            writeTxnActive = false;

            result = DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, rtxn);
            if (result != 0) {
                System.err.println("Failed to begin dupsort key rank read txn: " + result);
                return;
            }
            readTxnActive = true;

            result = DTLV.mdb_cursor_open(rtxn, dbi.get(), cursor);
            if (result != 0) {
                System.err.println("Failed to open dupsort key rank cursor: " + result);
                return;
            }
            cursorOpened = true;

            DTLV.MDB_val keyHolder = new DTLV.MDB_val();
            DTLV.MDB_val valHolder = new DTLV.MDB_val();

            long[] fullSample = { 0L, 3L, 5L };
            String[] expectedFullKeys = { "alpha", "bravo", "charlie" };
            String[] expectedFullVals = { "a1", "b2", "c1" };
            SizeTPointer fullIndices = toSizeTPointer(fullSample);

            DTLV.dtlv_key_rank_sample_iter iter =
                new DTLV.dtlv_key_rank_sample_iter();
            result = DTLV.dtlv_key_rank_sample_iter_create(
                iter, fullIndices, fullSample.length,
                cursor, keyHolder, valHolder,
                null, null);
            if (result != 0) {
                System.err.println("Failed to create dupsort full-range key rank iterator: " + result);
                fullIndices.close();
                return;
            }
            for (int i = 0; i < fullSample.length; i++) {
                int hasNext = DTLV.dtlv_key_rank_sample_iter_has_next(iter);
                expect(hasNext == DTLV.DTLV_TRUE, "Dupsort key rank iterator missing expected sample");
                String actualKey = mdbValToString(keyHolder);
                String actualVal = mdbValToString(valHolder);
                expect(actualKey.equals(expectedFullKeys[i]),
                       "Dupsort key rank iterator produced unexpected key");
                expect(actualVal.equals(expectedFullVals[i]),
                       "Dupsort key rank iterator produced unexpected value");
            }
            expect(DTLV.dtlv_key_rank_sample_iter_has_next(iter) == DTLV.DTLV_FALSE,
                   "Dupsort key rank iterator should be exhausted");
            DTLV.dtlv_key_rank_sample_iter_destroy(iter);
            fullIndices.close();

            long[] bravoSample = { 0L, 2L };
            String[] expectedBravoVals = { "b1", "b3" };
            SizeTPointer bravoIndices = toSizeTPointer(bravoSample);
            DTLV.MDB_val bravoKey = new DTLV.MDB_val();
            fillValWithString(bravoKey, "bravo", allocations);

            DTLV.dtlv_key_rank_sample_iter bravoIter =
                new DTLV.dtlv_key_rank_sample_iter();
            result = DTLV.dtlv_key_rank_sample_iter_create(
                bravoIter, bravoIndices, bravoSample.length,
                cursor, keyHolder, valHolder,
                bravoKey, bravoKey);
            if (result != 0) {
                System.err.println("Failed to create bounded dupsort key rank iterator: " + result);
                bravoIndices.close();
                return;
            }
            for (int i = 0; i < expectedBravoVals.length; i++) {
                int hasNext = DTLV.dtlv_key_rank_sample_iter_has_next(bravoIter);
                expect(hasNext == DTLV.DTLV_TRUE, "Bounded dupsort key iterator missing expected sample");
                String actualKey = mdbValToString(keyHolder);
                String actualVal = mdbValToString(valHolder);
                expect(actualKey.equals("bravo"),
                       "Bounded dupsort key iterator produced unexpected key");
                expect(actualVal.equals(expectedBravoVals[i]),
                       "Bounded dupsort key iterator produced unexpected value");
            }
            expect(DTLV.dtlv_key_rank_sample_iter_has_next(bravoIter) == DTLV.DTLV_FALSE,
                   "Bounded dupsort key iterator should be exhausted");
            DTLV.dtlv_key_rank_sample_iter_destroy(bravoIter);
            bravoIndices.close();

            pass("Passed dupsort key rank sample iterator test.");
        } finally {
            if (cursorOpened)
                DTLV.mdb_cursor_close(cursor);
            if (readTxnActive)
                DTLV.mdb_txn_abort(rtxn);
            if (writeTxnActive)
                DTLV.mdb_txn_abort(txn);
            dbi.close();
            for (BytePointer ptr : allocations)
                ptr.close();
            if (envCreated)
                DTLV.mdb_env_close(env);
            deleteDirectoryFiles(dir);
        }
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

        opts.metric_kind(DTLV.usearch_metric_ip_k)
            .metric((DTLV.usearch_metric_t) null)
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

    static void fillValWithString(DTLV.MDB_val target, String value,
                                  List<BytePointer> arena) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        BytePointer ptr = new BytePointer(bytes.length);
        ByteBuffer buffer = ptr.position(0).limit(bytes.length).asByteBuffer();
        buffer.put(bytes);
        ptr.position(0);
        target.mv_size(bytes.length);
        target.mv_data(ptr);
        arena.add(ptr);
    }

    static SizeTPointer toSizeTPointer(long[] values) {
        SizeTPointer pointer = new SizeTPointer(values.length);
        for (int i = 0; i < values.length; i++) {
            pointer.put(i, values[i]);
        }
        return pointer;
    }

    static String mdbValToString(DTLV.MDB_val val) {
        if (val == null || val.mv_data() == null || val.mv_size() == 0) {
            return "";
        }
        ByteBuffer buffer =
            val.mv_data().position(0).limit((int) val.mv_size()).asByteBuffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static long readDomainMetaU64(DTLV.MDB_env env, String domainName, String key) {
        DTLV.MDB_txn txn = new DTLV.MDB_txn();
        expect(DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, txn) == 0,
               "Failed to open meta read txn for " + key);
        IntPointer dbi = new IntPointer(1);
        String metaName = domainName + "/usearch-meta";
        expect(DTLV.mdb_dbi_open(txn, metaName, 0, dbi) == 0,
               "Failed to open usearch-meta DBI for " + domainName);
        BytePointer keyPtr = new BytePointer(key.length() + 1);
        keyPtr.putString(key);
        keyPtr.position(0);
        DTLV.MDB_val mdbKey = new DTLV.MDB_val();
        mdbKey.mv_data(keyPtr);
        mdbKey.mv_size(key.length() + 1);
        DTLV.MDB_val value = new DTLV.MDB_val();
        int rc = DTLV.mdb_get(txn, dbi.get(), mdbKey, value);
        expect(rc == 0, "Failed to read meta key " + key + ": " + rc);
        ByteBuffer buffer = value.mv_data()
            .position(0)
            .limit((int) value.mv_size())
            .asByteBuffer();
        buffer.order(ByteOrder.BIG_ENDIAN);
        long result;
        if (value.mv_size() == Long.BYTES) {
            result = buffer.getLong();
        } else if (value.mv_size() == Integer.BYTES) {
            result = buffer.getInt() & 0xffffffffL;
        } else {
            expect(false, "Unexpected meta size for key " + key);
            result = 0;
        }
        DTLV.mdb_txn_abort(txn);
        keyPtr.close();
        dbi.close();
        return result;
    }

    static Process spawnJavaWorkerProcess(String role,
                                          String envPath,
                                          String fsPath,
                                          String domainName,
                                          String key) throws IOException {
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        ProcessBuilder pb = new ProcessBuilder(javaBin,
                "-cp",
                classpath,
                "datalevin.dtlvnative.Test$MultiProcessWorker",
                role,
                envPath,
                fsPath,
                domainName,
                key);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        return pb.start();
    }

    static void runJavaWorker(String description,
                              String role,
                              String envPath,
                              String fsPath,
                              String domainName,
                              String key,
                              int expectedExit) {
        try {
            Process proc = spawnJavaWorkerProcess(role, envPath, fsPath, domainName, key);
            int exit = proc.waitFor();
            expect(exit == expectedExit,
                   description + " expected exit " + expectedExit + " but got " + exit);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            expect(false, "Failed to run worker " + description + ": " + e.getMessage());
        }
    }

    static class WorkerProcess {
        final Process process;
        final int expectedExit;
        final String description;

        WorkerProcess(Process process, int expectedExit, String description) {
            this.process = process;
            this.expectedExit = expectedExit;
            this.description = description;
        }
    }

    static WorkerProcess startJavaWorkerAsync(String description,
                                              String role,
                                              String envPath,
                                              String fsPath,
                                              String domainName,
                                              String key,
                                              int expectedExit) {
        try {
            Process proc = spawnJavaWorkerProcess(role, envPath, fsPath, domainName, key);
            return new WorkerProcess(proc, expectedExit, description);
        } catch (IOException e) {
            e.printStackTrace();
            expect(false, "Failed to start worker " + description + ": " + e.getMessage());
            return null;
        }
    }

    static void waitForWorkerProcesses(List<WorkerProcess> workers) {
        for (WorkerProcess worker : workers) {
            if (worker == null) continue;
            try {
                int exit = worker.process.waitFor();
                expect(exit == worker.expectedExit,
                       worker.description + " expected exit " + worker.expectedExit + " but got " + exit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                expect(false, "Interrupted while waiting for worker " + worker.description);
            }
        }
    }

    static void testUsearchLMDBIntegration() {
        System.err.println("Testing usearch LMDB integration ...");
        String root = "usearch-lmdb-domain";
        String envPath = root + "/env";
        String fsPath = root + "/fs";
        final String domainName = "vectors";
        deleteDirectoryFiles(root);
        try {
            Files.createDirectories(Paths.get(envPath));
            Files.createDirectories(Paths.get(fsPath));
        } catch (IOException e) {
            System.err.println("Failed to create directories for usearch LMDB test: " + e.getMessage());
            return;
        }

        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.dtlv_usearch_domain domain = new DTLV.dtlv_usearch_domain();
        DTLV.dtlv_usearch_handle handle = null;
        DTLV.dtlv_usearch_handle secondaryHandle = null;
        boolean envCreated = false;
        try {
            expect(DTLV.mdb_env_create(env) == 0, "Failed to create LMDB env");
            envCreated = true;
            expect(DTLV.mdb_env_set_maxdbs(env, 64) == 0, "Failed to set max DBs");
            expect(DTLV.mdb_env_open(env, envPath, DTLV.MDB_NOLOCK, 0664) == 0, "Failed to open LMDB env");

            expect(DTLV.dtlv_usearch_domain_open(env, domainName, fsPath, domain) == 0,
                    "Failed to open usearch domain");

            final int dimensions = 4;
            DTLV.usearch_init_options_t opts = createOpts(dimensions);
            DTLV.MDB_txn txn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "Failed to begin init txn");
            expect(DTLV.dtlv_usearch_store_init_options(domain, txn, opts) == 0,
                    "Failed to store init options");
            expect(DTLV.mdb_txn_commit(txn) == 0, "Failed to commit init txn");

            DTLV.MDB_txn verifyTxn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, verifyTxn) == 0,
                    "Failed to begin verify txn");
            DTLV.usearch_init_options_t loadedOpts = new DTLV.usearch_init_options_t();
            IntPointer initFound = new IntPointer(1);
            expect(DTLV.dtlv_usearch_load_init_options(domain, verifyTxn, loadedOpts, initFound) == 0,
                    "Failed to load init options");
            expect(initFound.get(0) == 1, "Init options not stored");
            expect(loadedOpts.metric_kind() == opts.metric_kind(), "Metric mismatch");
            expect(loadedOpts.quantization() == opts.quantization(), "Scalar mismatch");
            expect(loadedOpts.dimensions() == opts.dimensions(), "Dimensions mismatch");
            expect(loadedOpts.connectivity() == opts.connectivity(), "Connectivity mismatch");
            expect(loadedOpts.expansion_add() == opts.expansion_add(), "Expansion-add mismatch");
            expect(loadedOpts.expansion_search() == opts.expansion_search(), "Expansion-search mismatch");
            expect(loadedOpts.multi() == opts.multi(), "Multi flag mismatch");
            DTLV.mdb_txn_abort(verifyTxn);

            DTLV.dtlv_usearch_handle coldHandle = new DTLV.dtlv_usearch_handle();
            int coldRc = DTLV.dtlv_usearch_activate(domain, coldHandle);
            expect(coldRc == 0, "Failed to activate empty handle");
            DTLV.dtlv_usearch_deactivate(coldHandle);

            handle = new DTLV.dtlv_usearch_handle();
            expect(DTLV.dtlv_usearch_activate(domain, handle) == 0, "Failed to activate initial handle");
            secondaryHandle = new DTLV.dtlv_usearch_handle();
            expect(DTLV.dtlv_usearch_activate(domain, secondaryHandle) == 0,
                    "Failed to activate secondary handle");

            float[] vector = new float[] { 0.1f, 0.2f, 0.3f, 0.4f };
            long vectorKey = 42L;

            txn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "Failed to begin update txn");

            DTLV.dtlv_usearch_update update = new DTLV.dtlv_usearch_update();
            update.op(DTLV.DTLV_USEARCH_OP_ADD);
            BytePointer keyBytes = new BytePointer(Long.BYTES);
            ByteBuffer keyBuffer = keyBytes.position(0).limit(Long.BYTES).asByteBuffer();
            keyBuffer.order(ByteOrder.BIG_ENDIAN);
            keyBuffer.putLong(vectorKey);
            keyBytes.position(0);
            update.key(keyBytes);
            update.key_len(Long.BYTES);
            FloatPointer payload = new FloatPointer(dimensions);
            for (int i = 0; i < dimensions; i++) {
                payload.put(i, vector[i]);
            }
            update.payload(payload);
            update.payload_len(dimensions * Float.BYTES);
            update.scalar_kind((byte) DTLV.usearch_scalar_f32_k);
            update.dimensions((short) dimensions);

            DTLV.dtlv_usearch_txn_ctx txnCtx = new DTLV.dtlv_usearch_txn_ctx();
            expect(DTLV.dtlv_usearch_stage_update(domain, txn, update, txnCtx) == 0,
                    "Failed to stage usearch update");
            expect(DTLV.dtlv_usearch_apply_pending(txnCtx) == 0, "Failed to apply pending updates");
            expect(DTLV.mdb_txn_commit(txn) == 0, "Failed to commit update txn");
            expect(DTLV.dtlv_usearch_publish_log(txnCtx, 1) == 0, "Failed to publish log");
        expect(DTLV.dtlv_usearch_compact(domain, 1) == 0,
               "Failed to compact deltas");
            DTLV.dtlv_usearch_txn_ctx_close(txnCtx);
            payload.close();
            keyBytes.close();

            DTLV.MDB_txn refreshTxn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, refreshTxn) == 0,
                    "Failed to begin refresh txn");
            expect(DTLV.dtlv_usearch_refresh(handle, refreshTxn) == 0, "Failed to refresh handle");
            DTLV.mdb_txn_abort(refreshTxn);
            PointerPointer<BytePointer> error = new PointerPointer<>(1);
            error.put(0, (BytePointer) null);
            long indexSize = DTLV.dtlv_usearch_handle_size(handle, error);
            expectNoError(error, "usearch_size failed");
            expect(indexSize == 1, "Unexpected index size");
            boolean contains = DTLV.dtlv_usearch_handle_contains(handle, vectorKey, error);
            expectNoError(error, "usearch_contains failed");
            FloatPointer query = new FloatPointer(dimensions);
            for (int i = 0; i < dimensions; i++) {
                query.put(i, vector[i]);
            }
            LongPointer keys = new LongPointer(1);
            FloatPointer distances = new FloatPointer(1);
            error.put(0, (BytePointer) null);
            long found = DTLV.dtlv_usearch_handle_search(handle, query, DTLV.usearch_scalar_f32_k,
                    1, keys, distances, error);
            expectNoError(error, "usearch_search failed");
            expect(found >= 1, "usearch_search returned no results");
            expect(keys.get(0) == vectorKey, "usearch_search did not return expected key");
            expect(contains, "Activated handle missing vector");
            query.close();
            keys.close();
            distances.close();
            // Multi-handle WAL replay: second vector staged after both handles exist.
            float[] vectorTwo = new float[] { 0.5f, 0.6f, 0.7f, 0.8f };
            long vectorKeyTwo = 84L;
            DTLV.MDB_txn secondTxn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, secondTxn) == 0, "Failed to begin second update txn");
            DTLV.dtlv_usearch_update secondUpdate = new DTLV.dtlv_usearch_update();
            secondUpdate.op(DTLV.DTLV_USEARCH_OP_ADD);
            BytePointer secondKeyBytes = new BytePointer(Long.BYTES);
            ByteBuffer secondKeyBuffer = secondKeyBytes.position(0).limit(Long.BYTES).asByteBuffer();
            secondKeyBuffer.order(ByteOrder.BIG_ENDIAN).putLong(vectorKeyTwo);
            secondKeyBytes.position(0);
            secondUpdate.key(secondKeyBytes);
            secondUpdate.key_len(Long.BYTES);
            FloatPointer secondPayload = new FloatPointer(dimensions);
            for (int i = 0; i < dimensions; i++) {
                secondPayload.put(i, vectorTwo[i]);
            }
            secondUpdate.payload(secondPayload);
            secondUpdate.payload_len(dimensions * Float.BYTES);
            secondUpdate.scalar_kind((byte) DTLV.usearch_scalar_f32_k);
            secondUpdate.dimensions((short) dimensions);
            DTLV.dtlv_usearch_txn_ctx secondCtx = new DTLV.dtlv_usearch_txn_ctx();
            expect(DTLV.dtlv_usearch_stage_update(domain, secondTxn, secondUpdate, secondCtx) == 0,
                    "Failed to stage multi-handle update");
            expect(DTLV.dtlv_usearch_apply_pending(secondCtx) == 0, "Failed to apply multi-handle update");
            expect(DTLV.mdb_txn_commit(secondTxn) == 0, "Failed to commit second update txn");
            expect(DTLV.dtlv_usearch_publish_log(secondCtx, 1) == 0, "Failed to publish second log");
            DTLV.dtlv_usearch_txn_ctx_close(secondCtx);
            secondPayload.close();
            secondKeyBytes.close();

            error.put(0, (BytePointer) null);
            // Publish applies updates to all activated handles eagerly, so both handles
            // already contain the new vector before refresh. Verify the positive case.
            expect(DTLV.dtlv_usearch_handle_contains(handle, vectorKeyTwo, error),
                    "Primary handle failed to observe published vector");
            expectNoError(error, "primary contains check failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_contains(secondaryHandle, vectorKeyTwo, error),
                    "Secondary handle failed to observe published vector");
            expectNoError(error, "secondary contains check failed");

            DTLV.MDB_txn refreshTxnPrimary = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, refreshTxnPrimary) == 0,
                    "Failed to begin primary refresh txn");
            expect(DTLV.dtlv_usearch_refresh(handle, refreshTxnPrimary) == 0,
                    "Primary handle refresh failed");
            DTLV.mdb_txn_abort(refreshTxnPrimary);

            DTLV.MDB_txn refreshTxnSecondary = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, refreshTxnSecondary) == 0,
                    "Failed to begin secondary refresh txn");
            expect(DTLV.dtlv_usearch_refresh(secondaryHandle, refreshTxnSecondary) == 0,
                    "Secondary handle refresh failed");
            DTLV.mdb_txn_abort(refreshTxnSecondary);

            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_contains(handle, vectorKeyTwo, error),
                    "Primary handle missing refreshed vector");
            expectNoError(error, "primary post-refresh contains failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_contains(secondaryHandle, vectorKeyTwo, error),
                    "Secondary handle missing refreshed vector");
            expectNoError(error, "secondary post-refresh contains failed");
            FloatPointer refreshQuery = new FloatPointer(dimensions);
            for (int i = 0; i < dimensions; i++) {
                refreshQuery.put(i, vectorTwo[i]);
            }
            LongPointer refreshKeys = new LongPointer(1);
            FloatPointer refreshDistances = new FloatPointer(1);
            error.put(0, (BytePointer) null);
            long refreshedFound = DTLV.dtlv_usearch_handle_search(handle, refreshQuery, DTLV.usearch_scalar_f32_k,
                    1, refreshKeys, refreshDistances, error);
            expectNoError(error, "primary refresh search failed");
            expect(refreshedFound >= 1, "Primary refresh search returned no results");
            expect(refreshKeys.get(0) == vectorKeyTwo, "Primary refresh search mismatch");
            error.put(0, (BytePointer) null);
            long refreshedFoundSecondary = DTLV.dtlv_usearch_handle_search(secondaryHandle, refreshQuery,
                    DTLV.usearch_scalar_f32_k, 1, refreshKeys, refreshDistances, error);
            expectNoError(error, "secondary refresh search failed");
            expect(refreshedFoundSecondary >= 1, "Secondary refresh search returned no results");
            expect(refreshKeys.get(0) == vectorKeyTwo, "Secondary refresh search mismatch");
            refreshQuery.close();
            refreshKeys.close();
            refreshDistances.close();

            System.out.println("Passed usearch LMDB integration test.");

        } finally {
            if (handle != null) {
                DTLV.dtlv_usearch_deactivate(handle);
            }
            if (secondaryHandle != null) {
                DTLV.dtlv_usearch_deactivate(secondaryHandle);
            }
            if (domain != null) {
                DTLV.dtlv_usearch_domain_close(domain);
            }
            if (envCreated) {
                DTLV.mdb_env_close(env);
            }
            deleteDirectoryFiles(fsPath);
            deleteDirectoryFiles(envPath);
            deleteDirectoryFiles(root);
        }
    }

    static void testUsearchHandleMetadataAndConvenience() {
        System.err.println("Testing usearch handle metadata and convenience helpers ...");
        String root = "usearch-handle-meta";
        String envPath = root + "/env";
        String fsPath = root + "/fs";
        final String domainName = "vectors-meta";
        deleteDirectoryFiles(root);
        try {
            Files.createDirectories(Paths.get(envPath));
            Files.createDirectories(Paths.get(fsPath));
        } catch (IOException e) {
            System.err.println("Failed to create directories for handle metadata test: " + e.getMessage());
            return;
        }

        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.dtlv_usearch_domain domain = new DTLV.dtlv_usearch_domain();
        DTLV.dtlv_usearch_handle handle = null;
        try {
            expect(DTLV.mdb_env_create(env) == 0, "Failed to create LMDB env (meta)");
            expect(DTLV.mdb_env_set_maxdbs(env, 32) == 0, "Failed to set max DBs (meta)");
            expect(DTLV.mdb_env_open(env, envPath, DTLV.MDB_NOLOCK, 0664) == 0, "Failed to open LMDB env (meta)");
            expect(DTLV.dtlv_usearch_domain_open(env, domainName, fsPath, domain) == 0,
                    "Failed to open usearch domain (meta)");

            final int dimensions = 4;
            DTLV.usearch_init_options_t opts = createOpts(dimensions);
            DTLV.MDB_txn initTxn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, initTxn) == 0, "Failed to begin init txn (meta)");
            expect(DTLV.dtlv_usearch_store_init_options(domain, initTxn, opts) == 0,
                    "Failed to store init options (meta)");
            expect(DTLV.mdb_txn_commit(initTxn) == 0, "Failed to commit init txn (meta)");

            handle = new DTLV.dtlv_usearch_handle();
            expect(DTLV.dtlv_usearch_activate(domain, handle) == 0, "Failed to activate handle (meta)");

            PointerPointer<BytePointer> error = new PointerPointer<>(1);
            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_dimensions(handle) == dimensions, "Dimensions mismatch on handle");
            expect(DTLV.dtlv_usearch_handle_scalar_kind(handle) == DTLV.usearch_scalar_f32_k,
                    "Scalar kind mismatch on handle");
            long cap = DTLV.dtlv_usearch_handle_capacity(handle, error);
            expectNoError(error, "handle_capacity failed");
            expect(cap >= 0, "Capacity negative");
            BytePointer hw = DTLV.dtlv_usearch_handle_hardware(handle, error);
            expectNoError(error, "handle_hardware failed");
            if (hw != null) {
                hw.getString(); // ensure readable
            }
            long mem = DTLV.dtlv_usearch_handle_memory(handle, error);
            expectNoError(error, "handle_memory failed");
            expect(mem >= 0, "Memory usage negative");

            // Stage add via convenience helper.
            float[] vector = new float[] { 1.0f, 2.0f, 3.0f, 4.0f };
            long vectorKey = 7L;
            DTLV.dtlv_usearch_txn_ctx ctx = new DTLV.dtlv_usearch_txn_ctx();
            DTLV.MDB_txn txn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "Failed to begin add txn");
            BytePointer keyBytes = new BytePointer(Long.BYTES);
            ByteBuffer keyBuffer = keyBytes.position(0).limit(Long.BYTES).asByteBuffer();
            keyBuffer.order(ByteOrder.BIG_ENDIAN).putLong(vectorKey);
            keyBytes.position(0);
            FloatPointer payload = new FloatPointer(dimensions);
            for (int i = 0; i < dimensions; i++) {
                payload.put(i, vector[i]);
            }
            expect(DTLV.dtlv_usearch_stage_add(domain, txn, keyBytes, Long.BYTES,
                    payload, dimensions * Float.BYTES, ctx) == 0, "Stage add failed");
            expect(DTLV.dtlv_usearch_apply_pending(ctx) == 0, "Apply pending add failed");
            expect(DTLV.mdb_txn_commit(txn) == 0, "Commit add txn failed");
            expect(DTLV.dtlv_usearch_publish_log(ctx, 1) == 0, "Publish add failed");
            DTLV.dtlv_usearch_txn_ctx_close(ctx);
            payload.close();
            keyBytes.close();

            refreshUsearchHandle(env, domain, handle, "handle add refresh");
            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_contains(handle, vectorKey, error),
                    "Handle missing added vector");
            expectNoError(error, "handle_contains after add failed");
            FloatPointer out = new FloatPointer(dimensions);
            long got = DTLV.dtlv_usearch_handle_get(handle, vectorKey, out, error);
            expectNoError(error, "handle_get after add failed");
            expect(got == 1, "handle_get did not return a vector");
            for (int i = 0; i < dimensions; i++) {
                expect(Math.abs(out.get(i) - vector[i]) < 1e-6, "Retrieved vector mismatch (add)");
            }
            out.close();

            // Replace via helper.
            float[] replacement = new float[] { 9.0f, 8.0f, 7.0f, 6.0f };
            ctx = new DTLV.dtlv_usearch_txn_ctx();
            txn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "Failed to begin replace txn");
            FloatPointer replacementPayload = new FloatPointer(dimensions);
            for (int i = 0; i < dimensions; i++) {
                replacementPayload.put(i, replacement[i]);
            }
            keyBytes = new BytePointer(Long.BYTES);
            keyBuffer = keyBytes.position(0).limit(Long.BYTES).asByteBuffer();
            keyBuffer.order(ByteOrder.BIG_ENDIAN).putLong(vectorKey);
            keyBytes.position(0);
            expect(DTLV.dtlv_usearch_stage_replace(domain, txn, keyBytes, Long.BYTES,
                    replacementPayload, dimensions * Float.BYTES, ctx) == 0, "Stage replace failed");
            expect(DTLV.dtlv_usearch_apply_pending(ctx) == 0, "Apply pending replace failed");
            expect(DTLV.mdb_txn_commit(txn) == 0, "Commit replace txn failed");
            expect(DTLV.dtlv_usearch_publish_log(ctx, 1) == 0, "Publish replace failed");
            DTLV.dtlv_usearch_txn_ctx_close(ctx);
            replacementPayload.close();
            keyBytes.close();

            refreshUsearchHandle(env, domain, handle, "handle replace refresh");
            out = new FloatPointer(dimensions);
            error.put(0, (BytePointer) null);
            got = DTLV.dtlv_usearch_handle_get(handle, vectorKey, out, error);
            expectNoError(error, "handle_get after replace failed");
            expect(got == 1, "handle_get replace returned unexpected count");
            for (int i = 0; i < dimensions; i++) {
                expect(Math.abs(out.get(i) - replacement[i]) < 1e-6, "Retrieved vector mismatch (replace)");
            }
            out.close();

            // Delete via helper.
            ctx = new DTLV.dtlv_usearch_txn_ctx();
            txn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "Failed to begin delete txn");
            keyBytes = new BytePointer(Long.BYTES);
            keyBuffer = keyBytes.position(0).limit(Long.BYTES).asByteBuffer();
            keyBuffer.order(ByteOrder.BIG_ENDIAN).putLong(vectorKey);
            keyBytes.position(0);
            expect(DTLV.dtlv_usearch_stage_delete(domain, txn, keyBytes, Long.BYTES, ctx) == 0,
                    "Stage delete failed");
            expect(DTLV.dtlv_usearch_apply_pending(ctx) == 0, "Apply pending delete failed");
            expect(DTLV.mdb_txn_commit(txn) == 0, "Commit delete txn failed");
            expect(DTLV.dtlv_usearch_publish_log(ctx, 1) == 0, "Publish delete failed");
            DTLV.dtlv_usearch_txn_ctx_close(ctx);
            keyBytes.close();

            refreshUsearchHandle(env, domain, handle, "handle delete refresh");
            error.put(0, (BytePointer) null);
            expect(!DTLV.dtlv_usearch_handle_contains(handle, vectorKey, error),
                    "Handle still contains deleted vector");
            expectNoError(error, "handle_contains after delete failed");
            long finalSize = DTLV.dtlv_usearch_handle_size(handle, error);
            expectNoError(error, "handle_size after delete failed");
            expect(finalSize == 0, "Handle size not zero after delete");
            out = new FloatPointer(dimensions);
            got = DTLV.dtlv_usearch_handle_get(handle, vectorKey, out, error);
            expect(got == 0, "handle_get returned vector after delete");
            out.close();
        } finally {
            if (handle != null && !handle.isNull()) {
                DTLV.dtlv_usearch_deactivate(handle);
            }
            if (domain != null && !domain.isNull()) {
                DTLV.dtlv_usearch_domain_close(domain);
            }
            if (env != null && !env.isNull()) {
                DTLV.mdb_env_close(env);
            }
            deleteDirectoryFiles(fsPath);
            deleteDirectoryFiles(envPath);
            deleteDirectoryFiles(root);
        }
        System.out.println("Passed usearch handle metadata and convenience helper test.");
    }

    static void stageDomainVector(DTLV.MDB_env env,
                                  DTLV.dtlv_usearch_domain domain,
                                  long vectorKey,
                                  float[] vector,
                                  int dimensions,
                                  String description) {
        stageDomainVector(env, domain, vectorKey, vector, dimensions, description, true);
    }

    static void stageDomainVector(DTLV.MDB_env env,
                                  DTLV.dtlv_usearch_domain domain,
                                  long vectorKey,
                                  float[] vector,
                                  int dimensions,
                                  String description,
                                  boolean publishAfter) {
        DTLV.MDB_txn txn = new DTLV.MDB_txn();
        expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0,
                "Failed to begin " + description + " txn");
        DTLV.dtlv_usearch_update update = new DTLV.dtlv_usearch_update();
        update.op(DTLV.DTLV_USEARCH_OP_ADD);
        BytePointer keyBytes = new BytePointer(Long.BYTES);
        ByteBuffer keyBuffer = keyBytes.position(0).limit(Long.BYTES).asByteBuffer();
        keyBuffer.order(ByteOrder.BIG_ENDIAN).putLong(vectorKey);
        keyBytes.position(0);
        update.key(keyBytes);
        update.key_len(Long.BYTES);
        FloatPointer payload = new FloatPointer(dimensions);
        for (int i = 0; i < dimensions; i++) {
            payload.put(i, vector[i]);
        }
        update.payload(payload);
        update.payload_len(dimensions * Float.BYTES);
        update.scalar_kind((byte) DTLV.usearch_scalar_f32_k);
        update.dimensions((short) dimensions);
        DTLV.dtlv_usearch_txn_ctx ctx = new DTLV.dtlv_usearch_txn_ctx();
        expect(DTLV.dtlv_usearch_stage_update(domain, txn, update, ctx) == 0,
                "Failed to stage " + description + " update");
        expect(DTLV.dtlv_usearch_apply_pending(ctx) == 0,
                "Failed to apply pending " + description + " update");
        expect(DTLV.mdb_txn_commit(txn) == 0,
                "Failed to commit " + description + " txn");
        if (publishAfter) {
            expect(DTLV.dtlv_usearch_publish_log(ctx, 1) == 0,
                    "Failed to publish " + description + " log");
        }
        DTLV.dtlv_usearch_txn_ctx_close(ctx);
        payload.close();
        keyBytes.close();
    }

    static void refreshUsearchHandle(DTLV.MDB_env env,
                                     DTLV.dtlv_usearch_domain domain,
                                     DTLV.dtlv_usearch_handle handle,
                                     String description) {
        DTLV.MDB_txn refreshTxn = new DTLV.MDB_txn();
        expect(DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, refreshTxn) == 0,
                "Failed to begin " + description);
        expect(DTLV.dtlv_usearch_refresh(handle, refreshTxn) == 0,
                "Failed to refresh " + description);
        DTLV.mdb_txn_abort(refreshTxn);
    }

    static void stageAndPublishUsearchUpdate(DTLV.MDB_env env,
                                             DTLV.dtlv_usearch_domain domain,
                                             byte op,
                                             long key,
                                             float[] vector,
                                             int dimensions,
                                             String description) {
        DTLV.MDB_txn txn = new DTLV.MDB_txn();
        expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0,
                "Failed to begin " + description + " txn");
        DTLV.dtlv_usearch_update update = new DTLV.dtlv_usearch_update();
        update.op(op);
        BytePointer keyBytes = new BytePointer(Long.BYTES);
        ByteBuffer keyBuffer = keyBytes.position(0).limit(Long.BYTES).asByteBuffer();
        keyBuffer.order(ByteOrder.BIG_ENDIAN).putLong(key);
        keyBytes.position(0);
        update.key(keyBytes);
        update.key_len(Long.BYTES);
        FloatPointer payload = null;
        if (vector != null) {
            payload = new FloatPointer(dimensions);
            for (int i = 0; i < dimensions; i++) {
                payload.put(i, vector[i]);
            }
            update.payload(payload);
            update.payload_len(dimensions * Float.BYTES);
            update.scalar_kind((byte) DTLV.usearch_scalar_f32_k);
            update.dimensions((short) dimensions);
        } else {
            update.payload(null);
            update.payload_len(0);
            update.scalar_kind((byte) DTLV.usearch_scalar_unknown_k);
            update.dimensions((short) 0);
        }
        DTLV.dtlv_usearch_txn_ctx ctx = new DTLV.dtlv_usearch_txn_ctx();
        expect(DTLV.dtlv_usearch_stage_update(domain, txn, update, ctx) == 0,
                "Failed to stage " + description + " update");
        expect(DTLV.dtlv_usearch_apply_pending(ctx) == 0,
                "Failed to apply pending " + description + " update");
        expect(DTLV.mdb_txn_commit(txn) == 0,
                "Failed to commit " + description + " txn");
        expect(DTLV.dtlv_usearch_publish_log(ctx, 1) == 0,
                "Failed to publish " + description + " log");
        DTLV.dtlv_usearch_txn_ctx_close(ctx);
        if (payload != null) {
            payload.close();
        }
        keyBytes.close();
    }

    static FloatPointer vectorPointer(float[] vector) {
        FloatPointer ptr = new FloatPointer(vector.length);
        for (int i = 0; i < vector.length; i++) {
            ptr.put(i, vector[i]);
        }
        return ptr;
    }

    static void testUsearchMultiDomainIntegration() {
        System.err.println("Testing usearch multi-domain integration ...");
        String root = "usearch-multidomain";
        String envPath = root + "/env";
        String fsPathA = root + "/fsA";
        String fsPathB = root + "/fsB";
        deleteDirectoryFiles(root);
        try {
            Files.createDirectories(Paths.get(envPath));
            Files.createDirectories(Paths.get(fsPathA));
            Files.createDirectories(Paths.get(fsPathB));
        } catch (IOException e) {
            System.err.println("Failed to create directories for multi-domain test: " + e.getMessage());
            return;
        }
        final String domainAName = "vectorsA";
        final String domainBName = "vectorsB";
        final int dimensions = 4;
        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.dtlv_usearch_domain domainA = new DTLV.dtlv_usearch_domain();
        DTLV.dtlv_usearch_domain domainB = new DTLV.dtlv_usearch_domain();
        DTLV.dtlv_usearch_handle handleA = null;
        DTLV.dtlv_usearch_handle handleB = null;
        boolean envCreated = false;
        try {
            expect(DTLV.mdb_env_create(env) == 0, "Failed to create multi-domain env");
            envCreated = true;
            expect(DTLV.mdb_env_set_maxdbs(env, 128) == 0, "Failed to set max DBs for multi-domain env");
            expect(DTLV.mdb_env_open(env, envPath, DTLV.MDB_NOLOCK, 0664) == 0,
                    "Failed to open multi-domain env");
            expect(DTLV.dtlv_usearch_domain_open(env, domainAName, fsPathA, domainA) == 0,
                    "Failed to open domain A");
            expect(DTLV.dtlv_usearch_domain_open(env, domainBName, fsPathB, domainB) == 0,
                    "Failed to open domain B");
            DTLV.usearch_init_options_t opts = createOpts(dimensions);
            DTLV.MDB_txn txn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "Failed to begin domain A init txn");
            expect(DTLV.dtlv_usearch_store_init_options(domainA, txn, opts) == 0,
                    "Failed to store domain A opts");
            expect(DTLV.mdb_txn_commit(txn) == 0, "Failed to commit domain A init txn");
            txn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "Failed to begin domain B init txn");
            expect(DTLV.dtlv_usearch_store_init_options(domainB, txn, opts) == 0,
                    "Failed to store domain B opts");
            expect(DTLV.mdb_txn_commit(txn) == 0, "Failed to commit domain B init txn");
            handleA = new DTLV.dtlv_usearch_handle();
            handleB = new DTLV.dtlv_usearch_handle();
            expect(DTLV.dtlv_usearch_activate(domainA, handleA) == 0, "Failed to activate domain A handle");
            expect(DTLV.dtlv_usearch_activate(domainB, handleB) == 0, "Failed to activate domain B handle");
            float[] vecA = new float[] {0.11f, 0.21f, 0.31f, 0.41f};
            float[] vecB = new float[] {0.52f, 0.62f, 0.72f, 0.82f};
            long keyA = 101L;
            long keyB = 202L;
            stageDomainVector(env, domainA, keyA, vecA, dimensions, "domain A");
            stageDomainVector(env, domainB, keyB, vecB, dimensions, "domain B");
            refreshUsearchHandle(env, domainA, handleA, "domain A refresh txn");
            refreshUsearchHandle(env, domainB, handleB, "domain B refresh txn");
            PointerPointer<BytePointer> error = new PointerPointer<>(1);
            error.put(0, (BytePointer) null);
            boolean containsA = DTLV.dtlv_usearch_handle_contains(handleA, keyA, error);
            expectNoError(error, "domain A contains check failed");
            expect(containsA, "Domain A missing its vector");
            error.put(0, (BytePointer) null);
            boolean containsAOther = DTLV.dtlv_usearch_handle_contains(handleA, keyB, error);
            expectNoError(error, "domain A foreign contains check failed");
            expect(!containsAOther, "Domain A unexpectedly contains domain B vector");
            error.put(0, (BytePointer) null);
            boolean containsB = DTLV.dtlv_usearch_handle_contains(handleB, keyB, error);
            expectNoError(error, "domain B contains check failed");
            expect(containsB, "Domain B missing its vector");
            error.put(0, (BytePointer) null);
            boolean containsBOther = DTLV.dtlv_usearch_handle_contains(handleB, keyA, error);
            expectNoError(error, "domain B foreign contains check failed");
            expect(!containsBOther, "Domain B unexpectedly contains domain A vector");
            DTLV.dtlv_usearch_deactivate(handleA);
            DTLV.dtlv_usearch_deactivate(handleB);
            handleA = null;
            handleB = null;
            DTLV.dtlv_usearch_domain_close(domainA);
            DTLV.dtlv_usearch_domain_close(domainB);
            domainA = new DTLV.dtlv_usearch_domain();
            domainB = new DTLV.dtlv_usearch_domain();
            expect(DTLV.dtlv_usearch_domain_open(env, domainAName, fsPathA, domainA) == 0,
                    "Failed to reopen domain A");
            expect(DTLV.dtlv_usearch_domain_open(env, domainBName, fsPathB, domainB) == 0,
                    "Failed to reopen domain B");
            DTLV.dtlv_usearch_handle reloadA = new DTLV.dtlv_usearch_handle();
            DTLV.dtlv_usearch_handle reloadB = new DTLV.dtlv_usearch_handle();
            expect(DTLV.dtlv_usearch_activate(domainA, reloadA) == 0, "Failed to reactivate domain A");
            expect(DTLV.dtlv_usearch_activate(domainB, reloadB) == 0, "Failed to reactivate domain B");
            refreshUsearchHandle(env, domainA, reloadA, "domain A reload refresh");
            refreshUsearchHandle(env, domainB, reloadB, "domain B reload refresh");
            error.put(0, (BytePointer) null);
            DTLV.usearch_index_t reloadIndexA = DTLV.dtlv_usearch_handle_index(reloadA);
            expect(DTLV.dtlv_usearch_handle_contains(reloadA, keyA, error), "Reloaded domain A missing vector");
            expectNoError(error, "Reloaded domain A contains failed");
            error.put(0, (BytePointer) null);
            expect(!DTLV.dtlv_usearch_handle_contains(reloadA, keyB, error),
                    "Reloaded domain A unexpectedly has domain B vector");
            expectNoError(error, "Reloaded domain A foreign contains failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_contains(reloadB, keyB, error), "Reloaded domain B missing vector");
            expectNoError(error, "Reloaded domain B contains failed");
            error.put(0, (BytePointer) null);
            expect(!DTLV.dtlv_usearch_handle_contains(reloadB, keyA, error),
                    "Reloaded domain B unexpectedly has domain A vector");
            expectNoError(error, "Reloaded domain B foreign contains failed");
            DTLV.dtlv_usearch_deactivate(reloadA);
            DTLV.dtlv_usearch_deactivate(reloadB);
            System.out.println("Passed usearch multi-domain integration test.");
        } finally {
            if (handleA != null) {
                DTLV.dtlv_usearch_deactivate(handleA);
            }
            if (handleB != null) {
                DTLV.dtlv_usearch_deactivate(handleB);
            }
            if (domainA != null && !domainA.isNull()) {
                DTLV.dtlv_usearch_domain_close(domainA);
            }
            if (domainB != null && !domainB.isNull()) {
                DTLV.dtlv_usearch_domain_close(domainB);
            }
            if (envCreated) {
                DTLV.mdb_env_close(env);
            }
            deleteDirectoryFiles(fsPathA);
            deleteDirectoryFiles(fsPathB);
            deleteDirectoryFiles(envPath);
            deleteDirectoryFiles(root);
        }
    }

    static void testUsearchMultiDomainWalRecovery() {
        System.err.println("Testing usearch multi-domain WAL recovery ...");
        String root = "usearch-multidomain-wal";
        String envPath = root + "/env";
        String fsPathA = root + "/fsA";
        String fsPathB = root + "/fsB";
        deleteDirectoryFiles(root);
        try {
            Files.createDirectories(Paths.get(envPath));
            Files.createDirectories(Paths.get(fsPathA));
            Files.createDirectories(Paths.get(fsPathB));
        } catch (IOException e) {
            System.err.println("Failed to create directories for multi-domain WAL test: " + e.getMessage());
            return;
        }
        final String domainAName = "wal-domain-a";
        final String domainBName = "wal-domain-b";
        final int dimensions = 4;
        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.dtlv_usearch_domain domainA = new DTLV.dtlv_usearch_domain();
        DTLV.dtlv_usearch_domain domainB = new DTLV.dtlv_usearch_domain();
        boolean envCreated = false;
        try {
            expect(DTLV.mdb_env_create(env) == 0, "Failed to create WAL recovery env");
            envCreated = true;
            expect(DTLV.mdb_env_set_maxdbs(env, 128) == 0, "Failed to set max DBs for WAL recovery env");
            expect(DTLV.mdb_env_open(env, envPath, DTLV.MDB_NOLOCK, 0664) == 0,
                    "Failed to open WAL recovery env");
            expect(DTLV.dtlv_usearch_domain_open(env, domainAName, fsPathA, domainA) == 0,
                    "Failed to open WAL recovery domain A");
            expect(DTLV.dtlv_usearch_domain_open(env, domainBName, fsPathB, domainB) == 0,
                    "Failed to open WAL recovery domain B");
            DTLV.usearch_init_options_t opts = createOpts(dimensions);
            DTLV.MDB_txn txn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "Failed to begin WAL init txn");
            expect(DTLV.dtlv_usearch_store_init_options(domainA, txn, opts) == 0,
                    "Failed to store WAL domain A opts");
            expect(DTLV.dtlv_usearch_store_init_options(domainB, txn, opts) == 0,
                    "Failed to store WAL domain B opts");
            expect(DTLV.mdb_txn_commit(txn) == 0, "Failed to commit WAL init txn");
            float[] vecA = new float[] {0.15f, 0.25f, 0.35f, 0.45f};
            float[] vecB = new float[] {1.05f, 1.15f, 1.25f, 1.35f};
            float[] pendingA = new float[] {0.55f, 0.65f, 0.75f, 0.85f};
            float[] pendingB = new float[] {1.55f, 1.65f, 1.75f, 1.85f};
            stageDomainVector(env, domainA, 101L, vecA, dimensions, "wal domain A base", true);
            stageDomainVector(env, domainB, 303L, vecB, dimensions, "wal domain B base", true);
            stageDomainVector(env, domainA, 102L, pendingA, dimensions, "wal domain A pending", false);
            stageDomainVector(env, domainB, 304L, pendingB, dimensions, "wal domain B pending", false);
        } finally {
            if (domainA != null && !domainA.isNull()) {
                DTLV.dtlv_usearch_domain_close(domainA);
            }
            if (domainB != null && !domainB.isNull()) {
                DTLV.dtlv_usearch_domain_close(domainB);
            }
            if (envCreated) {
                DTLV.mdb_env_close(env);
            }
        }
        expect(directoryHasSuffix(fsPathA + "/pending/" + domainAName, ".ulog"),
                "Domain A pending WAL not sealed");
        expect(directoryHasSuffix(fsPathB + "/pending/" + domainBName, ".ulog"),
                "Domain B pending WAL not sealed");

        DTLV.MDB_env reloadEnv = new DTLV.MDB_env();
        DTLV.dtlv_usearch_domain reloadA = new DTLV.dtlv_usearch_domain();
        DTLV.dtlv_usearch_domain reloadB = new DTLV.dtlv_usearch_domain();
        DTLV.dtlv_usearch_handle handleA = null;
        DTLV.dtlv_usearch_handle handleB = null;
        boolean reloadCreated = false;
        try {
            expect(DTLV.mdb_env_create(reloadEnv) == 0, "Failed to create WAL reload env");
            reloadCreated = true;
            expect(DTLV.mdb_env_set_maxdbs(reloadEnv, 128) == 0, "Failed to set max DBs for WAL reload env");
            expect(DTLV.mdb_env_open(reloadEnv, envPath, DTLV.MDB_NOLOCK, 0664) == 0,
                    "Failed to open WAL reload env");
            expect(DTLV.dtlv_usearch_domain_open(reloadEnv, domainAName, fsPathA, reloadA) == 0,
                    "Failed to reopen WAL domain A");
            expect(DTLV.dtlv_usearch_domain_open(reloadEnv, domainBName, fsPathB, reloadB) == 0,
                    "Failed to reopen WAL domain B");
            handleA = new DTLV.dtlv_usearch_handle();
            handleB = new DTLV.dtlv_usearch_handle();
            expect(DTLV.dtlv_usearch_activate(reloadA, handleA) == 0,
                    "Failed to activate WAL domain A handle");
            expect(DTLV.dtlv_usearch_activate(reloadB, handleB) == 0,
                    "Failed to activate WAL domain B handle");
            PointerPointer<BytePointer> error = new PointerPointer<>(1);
            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_contains(handleA, 101L, error),
                    "Reloaded domain A missing published vector");
            expectNoError(error, "Reloaded domain A contains failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_contains(handleA, 102L, error),
                    "Reloaded domain A missing pending vector");
            expectNoError(error, "Reloaded domain A pending contains failed");
            error.put(0, (BytePointer) null);
            expect(!DTLV.dtlv_usearch_handle_contains(handleA, 303L, error),
                    "Domain A index leaked domain B vector");
            expectNoError(error, "Domain A cross-domain contains failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_contains(handleB, 303L, error),
                    "Reloaded domain B missing published vector");
            expectNoError(error, "Reloaded domain B contains failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_contains(handleB, 304L, error),
                    "Reloaded domain B missing pending vector");
            expectNoError(error, "Reloaded domain B pending contains failed");
            error.put(0, (BytePointer) null);
            expect(!DTLV.dtlv_usearch_handle_contains(handleB, 101L, error),
                    "Domain B index leaked domain A vector");
            expectNoError(error, "Domain B cross-domain contains failed");
        } finally {
            if (handleA != null) {
                DTLV.dtlv_usearch_deactivate(handleA);
            }
            if (handleB != null) {
                DTLV.dtlv_usearch_deactivate(handleB);
            }
            if (reloadA != null && !reloadA.isNull()) {
                DTLV.dtlv_usearch_domain_close(reloadA);
            }
            if (reloadB != null && !reloadB.isNull()) {
                DTLV.dtlv_usearch_domain_close(reloadB);
            }
            if (reloadCreated) {
                DTLV.mdb_env_close(reloadEnv);
            }
        }
        expect(!directoryHasSuffix(fsPathA + "/pending/" + domainAName, ".ulog"),
                "Domain A pending WAL not cleared after recovery");
        expect(!directoryHasSuffix(fsPathA + "/pending/" + domainAName, ".ulog.sealed"),
                "Domain A sealed WAL not cleared after recovery");
        expect(!directoryHasSuffix(fsPathB + "/pending/" + domainBName, ".ulog"),
                "Domain B pending WAL not cleared after recovery");
        expect(!directoryHasSuffix(fsPathB + "/pending/" + domainBName, ".ulog.sealed"),
                "Domain B sealed WAL not cleared after recovery");
        deleteDirectoryFiles(fsPathA);
        deleteDirectoryFiles(fsPathB);
        deleteDirectoryFiles(envPath);
        deleteDirectoryFiles(root);
        System.out.println("Passed usearch multi-domain WAL recovery test.");
    }

    static void testUsearchJavaMultiProcessIntegration() {
        System.err.println("Testing usearch Java multi-process integration ...");
        String root = "usearch-jvm-mproc";
        String envPath = root + "/env";
        String fsPath = root + "/fs";
        deleteDirectoryFiles(root);
        try {
            Files.createDirectories(Paths.get(envPath));
            Files.createDirectories(Paths.get(fsPath));
        } catch (IOException e) {
            System.err.println("Failed to create directories for JVM multi-process test: " + e.getMessage());
            return;
        }
        final String domainName = "vectors-jvm";
        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.dtlv_usearch_domain domain = new DTLV.dtlv_usearch_domain();
        boolean envCreated = false;
        try {
            expect(DTLV.mdb_env_create(env) == 0, "Failed to create JVM multi-process env");
            envCreated = true;
            expect(DTLV.mdb_env_set_maxdbs(env, 64) == 0, "Failed to set max DBs for JVM multi env");
            expect(DTLV.mdb_env_open(env, envPath, MULTI_PROCESS_ENV_FLAGS, 0664) == 0,
                    "Failed to open JVM multi-process env");
            expect(DTLV.dtlv_usearch_domain_open(env, domainName, fsPath, domain) == 0,
                    "Failed to open JVM multi-process domain");
            DTLV.usearch_init_options_t opts = createOpts(4);
            DTLV.MDB_txn txn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "Failed to begin JVM multi init txn");
            expect(DTLV.dtlv_usearch_store_init_options(domain, txn, opts) == 0,
                    "Failed to store JVM multi init opts");
            expect(DTLV.mdb_txn_commit(txn) == 0, "Failed to commit JVM multi init txn");
        } finally {
            if (domain != null && !domain.isNull()) {
                DTLV.dtlv_usearch_domain_close(domain);
            }
            if (envCreated) {
                DTLV.mdb_env_close(env);
            }
        }
        List<WorkerProcess> concurrent = new ArrayList<>();
        concurrent.add(startJavaWorkerAsync("writer-101", "writer", envPath, fsPath, domainName, "101", 0));
        concurrent.add(startJavaWorkerAsync("writer-303", "writer", envPath, fsPath, domainName, "303", 0));
        waitForWorkerProcesses(concurrent);
        runJavaWorker("reader-101", "reader", envPath, fsPath, domainName, "101", 0);
        runJavaWorker("reader-303", "reader", envPath, fsPath, domainName, "303", 0);
        runJavaWorker("crash-writer", "crash-writer", envPath, fsPath, domainName, "202", 42);
        runJavaWorker("reader-after-crash", "reader", envPath, fsPath, domainName, "202", 0);
        runJavaWorker("checkpoint-crash", "checkpoint-crash", envPath, fsPath, domainName, "0", 17);
        DTLV.MDB_env recoverEnv = new DTLV.MDB_env();
        DTLV.dtlv_usearch_domain recoverDomain = new DTLV.dtlv_usearch_domain();
        try {
            expect(DTLV.mdb_env_create(recoverEnv) == 0, "recover env create failed");
            expect(DTLV.mdb_env_set_maxdbs(recoverEnv, 64) == 0, "recover set maxdbs failed");
            expect(DTLV.mdb_env_open(recoverEnv, envPath, DTLV.MDB_NOLOCK, 0664) == 0,
                    "recover env open failed");
            expect(DTLV.dtlv_usearch_domain_open(recoverEnv, domainName, fsPath, recoverDomain) == 0,
                    "recover domain open failed");
            expect(DTLV.dtlv_usearch_checkpoint_recover(recoverDomain) == 0,
                    "checkpoint recover failed");
        } finally {
            if (recoverDomain != null && !recoverDomain.isNull()) {
                DTLV.dtlv_usearch_domain_close(recoverDomain);
            }
            DTLV.mdb_env_close(recoverEnv);
        }
        runJavaWorker("reader-after-checkpoint-crash", "reader", envPath, fsPath, domainName, "101", 0);
        deleteDirectoryFiles(fsPath);
        deleteDirectoryFiles(envPath);
        deleteDirectoryFiles(root);
        System.out.println("Passed usearch Java multi-process integration test.");
    }

    static void testUsearchFuzz() {
        System.err.println("Testing usearch LMDB fuzz operations ...");
        String root = "usearch-fuzz";
        String envPath = root + "/env";
        String fsPath = root + "/fs";
        final String domainName = "fuzz-domain";
        deleteDirectoryFiles(root);
        try {
            Files.createDirectories(Paths.get(envPath));
            Files.createDirectories(Paths.get(fsPath));
        } catch (IOException e) {
            System.err.println("Failed to create directories for fuzz test: " + e.getMessage());
            return;
        }

        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.dtlv_usearch_domain domain = new DTLV.dtlv_usearch_domain();
        DTLV.dtlv_usearch_handle handle = null;
        boolean envCreated = false;
        try {
            expect(DTLV.mdb_env_create(env) == 0, "Failed to create fuzz env");
            envCreated = true;
            expect(DTLV.mdb_env_set_maxdbs(env, 64) == 0, "Failed to set max DBs for fuzz env");
            expect(DTLV.mdb_env_open(env, envPath, DTLV.MDB_NOLOCK, 0664) == 0, "Failed to open fuzz env");
            expect(DTLV.dtlv_usearch_domain_open(env, domainName, fsPath, domain) == 0,
                    "Failed to open fuzz domain");

            final int dimensions = 4;
            DTLV.usearch_init_options_t opts = createOpts(dimensions);
            DTLV.MDB_txn initTxn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, initTxn) == 0, "Failed to begin fuzz init txn");
            expect(DTLV.dtlv_usearch_store_init_options(domain, initTxn, opts) == 0,
                    "Failed to store fuzz init options");
            expect(DTLV.mdb_txn_commit(initTxn) == 0, "Failed to commit fuzz init txn");

            handle = new DTLV.dtlv_usearch_handle();
            expect(DTLV.dtlv_usearch_activate(domain, handle) == 0, "Failed to activate fuzz handle");

            Map<Long, float[]> expected = new HashMap<>();
            Random rnd = new Random(1234);
            int iterations = 100;
            for (int i = 0; i < iterations; i++) {
                int choice = expected.isEmpty() ? 0 : rnd.nextInt(3);
                if (choice == 0) { // add
                    long key;
                    do {
                        key = Math.abs(rnd.nextLong() % 1_000_000L);
                    } while (expected.containsKey(key));
                    float[] vec = new float[dimensions];
                    for (int d = 0; d < dimensions; d++) {
                        vec[d] = rnd.nextFloat();
                    }
                    stageAndPublishUsearchUpdate(env, domain, (byte) DTLV.DTLV_USEARCH_OP_ADD, key, vec, dimensions,
                            "fuzz add");
                    expected.put(key, vec);
                } else if (choice == 1 && !expected.isEmpty()) { // replace
                    long key = expected.keySet().stream().skip(rnd.nextInt(expected.size())).findFirst().orElse(0L);
                    float[] vec = new float[dimensions];
                    for (int d = 0; d < dimensions; d++) {
                        vec[d] = rnd.nextFloat();
                    }
                    stageAndPublishUsearchUpdate(env, domain, (byte) DTLV.DTLV_USEARCH_OP_REPLACE, key, vec,
                            dimensions, "fuzz replace");
                    expected.put(key, vec);
                } else if (choice == 2 && !expected.isEmpty()) { // delete
                    long key = expected.keySet().stream().skip(rnd.nextInt(expected.size())).findFirst().orElse(0L);
                    stageAndPublishUsearchUpdate(env, domain, (byte) DTLV.DTLV_USEARCH_OP_DELETE, key, null,
                            dimensions, "fuzz delete");
                    expected.remove(key);
                }

                if (i % 10 == 0 || i == iterations - 1) {
                    refreshUsearchHandle(env, domain, handle, "fuzz handle");
                    PointerPointer<BytePointer> err = new PointerPointer<>(1);
                    err.put(0, (BytePointer) null);
                    long size = DTLV.dtlv_usearch_handle_size(handle, err);
                    expectNoError(err, "fuzz size check failed");
                    expect(size == expected.size(), "Fuzz size mismatch: expected " + expected.size() + " got " + size);

                    int checks = Math.min(3, expected.size());
                    Iterator<Map.Entry<Long, float[]>> it = expected.entrySet().iterator();
                    for (int c = 0; c < checks && it.hasNext(); c++) {
                        Map.Entry<Long, float[]> entry = it.next();
                        long key = entry.getKey();
                        float[] vec = entry.getValue();
                        err.put(0, (BytePointer) null);
                        boolean contains = DTLV.dtlv_usearch_handle_contains(handle, key, err);
                        expectNoError(err, "fuzz contains check failed");
                        expect(contains, "Handle missing expected key " + key);
                        // Exact search over expected dataset to get the true nearest key.
                        if (expected.size() > 0) {
                            long[] keyOrder = new long[expected.size()];
                            FloatPointer dataset = new FloatPointer((long) expected.size() * dimensions);
                            int idx = 0;
                            for (Map.Entry<Long, float[]> e : expected.entrySet()) {
                                keyOrder[idx] = e.getKey();
                                float[] v = e.getValue();
                                long base = (long) idx * dimensions;
                                for (int d = 0; d < dimensions; d++) {
                                    dataset.put(base + d, v[d]);
                                }
                                idx++;
                            }
                            FloatPointer query = vectorPointer(vec);
                            LongPointer exactKeys = new LongPointer(1);
                            FloatPointer exactDistances = new FloatPointer(1);
                            err.put(0, (BytePointer) null);
                            long strideBytes = (long) dimensions * Float.BYTES;
                            DTLV.usearch_exact_search(dataset, expected.size(), strideBytes,
                                    query, 1, strideBytes,
                                    DTLV.usearch_scalar_f32_k, dimensions,
                                    DTLV.usearch_metric_ip_k, 1, 1,
                                    exactKeys, Long.BYTES,
                                    exactDistances, Float.BYTES,
                                    err);
                            expectNoError(err, "fuzz exact search failed");
                            long exactIdx = exactKeys.get(0);
                            expect(exactIdx >= 0 && exactIdx < keyOrder.length,
                                    "fuzz exact search returned invalid index");
                            long exactKey = keyOrder[(int) exactIdx];

                            // Approximate search on handle and ensure exact top-1 is present.
                            int approxK = Math.min(Math.max(1, expected.size()), 32);
                            LongPointer keys = new LongPointer(approxK);
                            FloatPointer distances = new FloatPointer(approxK);
                            err.put(0, (BytePointer) null);
                            long found = DTLV.dtlv_usearch_handle_search(handle, query, DTLV.usearch_scalar_f32_k,
                                    approxK, keys, distances, err);
                            expectNoError(err, "fuzz approx search failed");
                            expect(found > 0, "fuzz approx search returned no results for key " + key);
                            boolean matched = false;
                            for (int j = 0; j < found; j++) {
                                if (keys.get(j) == exactKey) {
                                    matched = true;
                                    break;
                                }
                            }
                            expect(matched, "fuzz approx search missing exact top-1 key " + exactKey);

                            dataset.close();
                            query.close();
                            exactKeys.close();
                            exactDistances.close();
                            keys.close();
                            distances.close();
                        }
                    }
                }
            }

            DTLV.dtlv_usearch_deactivate(handle);
            DTLV.dtlv_usearch_domain_close(domain);
        } finally {
            if (envCreated) {
                DTLV.mdb_env_close(env);
            }
            deleteDirectoryFiles(fsPath);
            deleteDirectoryFiles(envPath);
            deleteDirectoryFiles(root);
        }
        System.out.println("Passed usearch fuzz test.");
    }

    public static void main(String[] args) {
        runTest("LMDB suite", Test::testLMDB);
        System.out.println("----");
        testUsearchLMDBIntegration();
        System.out.println("----");
        testUsearchHandleMetadataAndConvenience();
        System.out.println("----");
        testUsearchFuzz();
        System.out.println("----");
        testUsearchMultiDomainIntegration();
        System.out.println("----");
        testUsearchMultiDomainWalRecovery();
        System.out.println("----");
        testUsearchJavaMultiProcessIntegration();
    }

    public static class MultiProcessWorker {

        static class WriterLock implements AutoCloseable {
            private final FileChannel channel;
            private final FileLock lock;

            private WriterLock(FileChannel channel, FileLock lock) {
                this.channel = channel;
                this.lock = lock;
            }

            static WriterLock acquire(String envPath) {
                Path lockFile = Paths.get(envPath, "writer.lock");
                try {
                    FileChannel channel = FileChannel.open(lockFile,
                            StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    FileLock lock = channel.lock();
                    return new WriterLock(channel, lock);
                } catch (IOException e) {
                    expect(false, "Failed to acquire writer lock: " + e.getMessage());
                    return null;
                }
            }

            @Override
            public void close() {
                try {
                    if (lock != null && lock.isValid()) {
                        lock.release();
                    }
                } catch (IOException ignored) {
                }
                try {
                    if (channel != null && channel.isOpen()) {
                        channel.close();
                    }
                } catch (IOException ignored) {
                }
            }
        }
        static float[] buildVector(long key) {
            float base = (float) key / 100.0f;
            return new float[] {base, base + 0.1f, base + 0.2f, base + 0.3f};
        }

        static void runWriter(String envPath,
                              String fsPath,
                              String domainName,
                              long key,
                              boolean crashBeforePublish) {
            try (WriterLock ignored = WriterLock.acquire(envPath)) {
                DTLV.MDB_env env = new DTLV.MDB_env();
                expect(DTLV.mdb_env_create(env) == 0, "worker env create failed");
                expect(DTLV.mdb_env_set_maxdbs(env, 64) == 0, "worker set maxdbs failed");
                expect(DTLV.mdb_env_open(env, envPath, MULTI_PROCESS_ENV_FLAGS, 0664) == 0,
                        "worker env open failed");
                DTLV.dtlv_usearch_domain domain = new DTLV.dtlv_usearch_domain();
                expect(DTLV.dtlv_usearch_domain_open(env, domainName, fsPath, domain) == 0,
                        "worker domain open failed");
                DTLV.MDB_txn txn = new DTLV.MDB_txn();
                expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0, "worker txn begin failed");
                DTLV.dtlv_usearch_update update = new DTLV.dtlv_usearch_update();
                update.op(DTLV.DTLV_USEARCH_OP_ADD);
                BytePointer keyBytes = new BytePointer(Long.BYTES);
                ByteBuffer keyBuffer = keyBytes.position(0).limit(Long.BYTES).asByteBuffer();
                keyBuffer.order(ByteOrder.BIG_ENDIAN).putLong(key);
                keyBytes.position(0);
                update.key(keyBytes);
                update.key_len(Long.BYTES);
                float[] vector = buildVector(key);
                FloatPointer payload = new FloatPointer(vector.length);
                for (int i = 0; i < vector.length; i++) {
                    payload.put(i, vector[i]);
                }
                update.payload(payload);
                update.payload_len(vector.length * Float.BYTES);
                update.scalar_kind((byte) DTLV.usearch_scalar_f32_k);
                update.dimensions((short) vector.length);
                DTLV.dtlv_usearch_txn_ctx ctx = new DTLV.dtlv_usearch_txn_ctx();
                expect(DTLV.dtlv_usearch_stage_update(domain, txn, update, ctx) == 0,
                        "worker stage update failed");
                expect(DTLV.dtlv_usearch_apply_pending(ctx) == 0,
                        "worker apply pending failed");
                expect(DTLV.mdb_txn_commit(txn) == 0, "worker commit failed");
                if (!crashBeforePublish) {
                    expect(DTLV.dtlv_usearch_publish_log(ctx, 1) == 0,
                            "worker publish failed");
                    DTLV.dtlv_usearch_txn_ctx_close(ctx);
                    payload.close();
                    keyBytes.close();
                    DTLV.dtlv_usearch_domain_close(domain);
                    DTLV.mdb_env_close(env);
                    System.exit(0);
                } else {
                    System.exit(42);
                }
            }
        }

        static void runCheckpointCrash(String envPath,
                                       String fsPath,
                                       String domainName) {
            try (WriterLock ignored = WriterLock.acquire(envPath)) {
                DTLV.MDB_env env = new DTLV.MDB_env();
                expect(DTLV.mdb_env_create(env) == 0, "checkpoint env create failed");
                expect(DTLV.mdb_env_set_maxdbs(env, 64) == 0, "checkpoint set maxdbs failed");
                expect(DTLV.mdb_env_open(env, envPath, MULTI_PROCESS_ENV_FLAGS, 0664) == 0,
                        "checkpoint env open failed");
                DTLV.dtlv_usearch_domain domain = new DTLV.dtlv_usearch_domain();
                expect(DTLV.dtlv_usearch_domain_open(env, domainName, fsPath, domain) == 0,
                        "checkpoint domain open failed");
                DTLV.dtlv_usearch_handle handle = new DTLV.dtlv_usearch_handle();
                expect(DTLV.dtlv_usearch_activate(domain, handle) == 0,
                        "checkpoint activate failed");
                DTLV.usearch_index_t index = DTLV.dtlv_usearch_handle_index(handle);
                expect(index != null && !index.isNull(), "checkpoint index missing");
                long currentSnapshot = readDomainMetaU64(env, domainName, "snapshot_seq");
                long targetSeq = currentSnapshot + 1;
                DTLV.dtlv_uuid128 writerUuid = new DTLV.dtlv_uuid128();
                writerUuid.hi(System.nanoTime());
                writerUuid.lo(Thread.currentThread().getId());
                SizeTPointer chunkCount = new SizeTPointer(1);
                expect(DTLV.dtlv_usearch_checkpoint_write_snapshot(domain,
                        index,
                        targetSeq,
                        writerUuid,
                        chunkCount) == 0,
                       "checkpoint write failed");
                System.exit(17);
            }
        }

        static void runReader(String envPath,
                              String fsPath,
                              String domainName,
                              long key) {
            DTLV.MDB_env env = new DTLV.MDB_env();
            expect(DTLV.mdb_env_create(env) == 0, "reader env create failed");
            expect(DTLV.mdb_env_set_maxdbs(env, 64) == 0, "reader set maxdbs failed");
            expect(DTLV.mdb_env_open(env, envPath, MULTI_PROCESS_ENV_FLAGS, 0664) == 0,
                    "reader env open failed");
            DTLV.dtlv_usearch_domain domain = new DTLV.dtlv_usearch_domain();
            expect(DTLV.dtlv_usearch_domain_open(env, domainName, fsPath, domain) == 0,
                    "reader domain open failed");
            DTLV.dtlv_usearch_handle handle = new DTLV.dtlv_usearch_handle();
            expect(DTLV.dtlv_usearch_activate(domain, handle) == 0,
                    "reader activate failed");
            PointerPointer<BytePointer> error = new PointerPointer<>(1);
            error.put(0, (BytePointer) null);
            expect(DTLV.dtlv_usearch_handle_contains(handle, key, error),
                    "reader missing key " + key);
            expectNoError(error, "reader contains error");
            DTLV.dtlv_usearch_deactivate(handle);
            DTLV.dtlv_usearch_domain_close(domain);
            DTLV.mdb_env_close(env);
            System.exit(0);
        }

        public static void main(String[] args) {
            if (args.length < 5) {
                System.err.println("Usage: MultiProcessWorker <role> <envPath> <fsPath> <domain> <key>");
                System.exit(2);
            }
            String role = args[0];
            String envPath = args[1];
            String fsPath = args[2];
            String domainName = args[3];
            long key = Long.parseLong(args[4]);
            switch (role) {
            case "writer":
                runWriter(envPath, fsPath, domainName, key, false);
                break;
            case "crash-writer":
                runWriter(envPath, fsPath, domainName, key, true);
                break;
            case "reader":
                runReader(envPath, fsPath, domainName, key);
                break;
            case "checkpoint-crash":
                runCheckpointCrash(envPath, fsPath, domainName);
                break;
            default:
                System.err.println("Unknown worker role: " + role);
                System.exit(3);
            }
        }
    }
}
