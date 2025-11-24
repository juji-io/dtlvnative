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

        testLMDBCountedPrefix();
        testRankSampleIterator();
        testKeyRankSampleIterator();
        testListValIteratorBounds();

        System.out.println("Passed LMDB tests.");
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

            System.out.println("Passed counted/prefix LMDB test.");
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

            System.out.println("Passed list value iterator bounds test.");
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

            System.out.println("Passed rank-based list sample iterator test.");
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

    static void testKeyRankSampleIterator() {

        System.err.println("Testing key rank sample iterator ...");

        String dir = "db-key-rank-sample";
        List<BytePointer> allocations = new ArrayList<>();

        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.MDB_txn txn = new DTLV.MDB_txn();
        DTLV.MDB_txn rtxn = new DTLV.MDB_txn();
        DTLV.MDB_cursor cursor = new DTLV.MDB_cursor();
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

            System.out.println("Passed key rank sample iterator test.");
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

    static void testUsearchInit(int collSize, int dimensions) {

        DTLV.usearch_init_options_t opts = createOpts(dimensions);

        expect(opts.metric_kind() == DTLV.usearch_metric_ip_k, "fail to get metric_kind");
        expect(opts.quantization() == DTLV.usearch_scalar_f32_k, "fail to get quantization");
        expect(opts.connectivity() == 3, "fail to get connectivity");
        expect(opts.dimensions() == dimensions, "fail to get dimensions");
        expect(opts.expansion_add() == 40, "fail to get expansion_add");
        expect(opts.expansion_search() == 16, "fail to get expansion_search");
        expect(opts.multi() == false, "fail to get multi");

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        error.put(0, (BytePointer) null);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        System.out.println("called init");
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
        index = DTLV.usearch_init(weird_opts, error);
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

        testUsearchFormatIntrospection();
        System.out.println("Passed all usearch tests.");
    }

    static void testUsearchFormatIntrospection() {
        System.err.println("Testing usearch format-introspection helpers ...");

        String snapshotPath = "usearch-format-introspect.usearch";
        new File(snapshotPath).delete();

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        DTLV.usearch_init_options_t snapshotOpts = createOpts(8);
        error.put(0, (BytePointer) null);
        DTLV.usearch_index_t index = DTLV.usearch_init(snapshotOpts, error);
        expectNoError(error, "Failed to init snapshot index");

        error.put(0, (BytePointer) null);
        DTLV.usearch_save(index, snapshotPath, error);
        expectNoError(error, "Failed to save snapshot index");

        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);
        expectNoError(error, "Failed to free snapshot index");

        DTLV.dtlv_usearch_format_info snapshotInfo = new DTLV.dtlv_usearch_format_info();
        expect(DTLV.dtlv_usearch_probe_filesystem(snapshotPath, snapshotInfo) == 0,
               "Failed to probe filesystem snapshot");
        expect(snapshotInfo.metric_kind() == snapshotOpts.metric_kind(),
               "Snapshot metric mismatch");
        expect(snapshotInfo.scalar_kind() == snapshotOpts.quantization(),
               "Snapshot scalar mismatch");
        expect(snapshotInfo.dimensions() == snapshotOpts.dimensions(),
               "Snapshot dimension mismatch");
        expect(!snapshotInfo.multi(), "Snapshot multi flag mismatch");
        // Connectivity is not persisted by usearch_metadata, so it currently reports zero.
        expect(snapshotInfo.connectivity() == 0,
               "Snapshot connectivity should be zero");

        new File(snapshotPath).delete();

        String root = "usearch-format-domain";
        String envPath = root + "/env";
        String fsPath = root + "/fs";
        deleteDirectoryFiles(root);
        try {
            Files.createDirectories(Paths.get(envPath));
            Files.createDirectories(Paths.get(fsPath));
        } catch (IOException e) {
            System.err.println("Failed to create paths for format introspection: " + e.getMessage());
            return;
        }

        final String domainName = "vectors";
        DTLV.MDB_env env = new DTLV.MDB_env();
        DTLV.dtlv_usearch_domain domain = new DTLV.dtlv_usearch_domain();
        boolean envCreated = false;
        try {
            expect(DTLV.mdb_env_create(env) == 0, "Failed to create format env");
            envCreated = true;
            expect(DTLV.mdb_env_set_maxdbs(env, 16) == 0, "Failed to set maxdbs for format env");
            expect(DTLV.mdb_env_open(env, envPath, DTLV.MDB_NOLOCK, 0664) == 0,
                   "Failed to open format env");

            expect(DTLV.dtlv_usearch_domain_open(env, "format-domain", fsPath, domain) == 0,
                   "Failed to open format domain");

            DTLV.usearch_init_options_t domainOpts = createOpts(16);
            DTLV.MDB_txn txn = new DTLV.MDB_txn();
            expect(DTLV.mdb_txn_begin(env, null, 0, txn) == 0,
                   "Failed to begin format init txn");
            expect(DTLV.dtlv_usearch_store_init_options(domain, txn, domainOpts) == 0,
                   "Failed to store format init options");
            expect(DTLV.mdb_txn_commit(txn) == 0, "Failed to commit format init txn");

            DTLV.dtlv_usearch_format_info domainInfo = new DTLV.dtlv_usearch_format_info();
            expect(DTLV.dtlv_usearch_inspect_domain(domain, null, domainInfo) == 0,
                   "Failed to inspect usearch domain");
            expect(domainInfo.metric_kind() == domainOpts.metric_kind(),
                   "Domain metric mismatch");
            expect(domainInfo.scalar_kind() == domainOpts.quantization(),
                   "Domain scalar mismatch");
            expect(domainInfo.dimensions() == domainOpts.dimensions(),
                   "Domain dimension mismatch");
            expect(domainInfo.connectivity() == domainOpts.connectivity(),
                   "Domain connectivity mismatch");
            expect(!domainInfo.multi(), "Domain multi mismatch");

        } finally {
            if (domain != null && !domain.isNull()) {
                DTLV.dtlv_usearch_domain_close(domain);
            }
            if (envCreated) {
                DTLV.mdb_env_close(env);
            }
            deleteDirectoryFiles(fsPath);
            deleteDirectoryFiles(envPath);
            deleteDirectoryFiles(root);
        }

        System.out.println("Passed format-introspection tests.");
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
            DTLV.usearch_index_t index = DTLV.dtlv_usearch_handle_index(handle);
            expect(index != null && !index.isNull(), "Handle did not expose index");

            PointerPointer<BytePointer> error = new PointerPointer<>(1);
            error.put(0, (BytePointer) null);
            long indexSize = DTLV.usearch_size(index, error);
            expectNoError(error, "usearch_size failed");
            expect(indexSize == 1, "Unexpected index size");
            boolean contains = DTLV.usearch_contains(index, vectorKey, error);
            expectNoError(error, "usearch_contains failed");
            FloatPointer query = new FloatPointer(dimensions);
            for (int i = 0; i < dimensions; i++) {
                query.put(i, vector[i]);
            }
            LongPointer keys = new LongPointer(1);
            FloatPointer distances = new FloatPointer(1);
            error.put(0, (BytePointer) null);
            long found = DTLV.usearch_search(index, query, DTLV.usearch_scalar_f32_k,
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
            DTLV.usearch_index_t primaryIndex = DTLV.dtlv_usearch_handle_index(handle);
            DTLV.usearch_index_t secondaryIndex = DTLV.dtlv_usearch_handle_index(secondaryHandle);
            // Publish applies updates to all activated handles eagerly, so both handles
            // already contain the new vector before refresh. Verify the positive case.
            expect(DTLV.usearch_contains(primaryIndex, vectorKeyTwo, error),
                    "Primary handle failed to observe published vector");
            expectNoError(error, "primary contains check failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.usearch_contains(secondaryIndex, vectorKeyTwo, error),
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
            expect(DTLV.usearch_contains(primaryIndex, vectorKeyTwo, error),
                    "Primary handle missing refreshed vector");
            expectNoError(error, "primary post-refresh contains failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.usearch_contains(secondaryIndex, vectorKeyTwo, error),
                    "Secondary handle missing refreshed vector");
            expectNoError(error, "secondary post-refresh contains failed");
            FloatPointer refreshQuery = new FloatPointer(dimensions);
            for (int i = 0; i < dimensions; i++) {
                refreshQuery.put(i, vectorTwo[i]);
            }
            LongPointer refreshKeys = new LongPointer(1);
            FloatPointer refreshDistances = new FloatPointer(1);
            error.put(0, (BytePointer) null);
            long refreshedFound = DTLV.usearch_search(primaryIndex, refreshQuery, DTLV.usearch_scalar_f32_k,
                    1, refreshKeys, refreshDistances, error);
            expectNoError(error, "primary refresh search failed");
            expect(refreshedFound >= 1, "Primary refresh search returned no results");
            expect(refreshKeys.get(0) == vectorKeyTwo, "Primary refresh search mismatch");
            error.put(0, (BytePointer) null);
            long refreshedFoundSecondary = DTLV.usearch_search(secondaryIndex, refreshQuery,
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
            DTLV.usearch_index_t indexA = DTLV.dtlv_usearch_handle_index(handleA);
            DTLV.usearch_index_t indexB = DTLV.dtlv_usearch_handle_index(handleB);
            expect(indexA != null && !indexA.isNull(), "Domain A index unavailable");
            expect(indexB != null && !indexB.isNull(), "Domain B index unavailable");
            boolean containsA = DTLV.usearch_contains(indexA, keyA, error);
            expectNoError(error, "domain A contains check failed");
            expect(containsA, "Domain A missing its vector");
            error.put(0, (BytePointer) null);
            boolean containsAOther = DTLV.usearch_contains(indexA, keyB, error);
            expectNoError(error, "domain A foreign contains check failed");
            expect(!containsAOther, "Domain A unexpectedly contains domain B vector");
            error.put(0, (BytePointer) null);
            boolean containsB = DTLV.usearch_contains(indexB, keyB, error);
            expectNoError(error, "domain B contains check failed");
            expect(containsB, "Domain B missing its vector");
            error.put(0, (BytePointer) null);
            boolean containsBOther = DTLV.usearch_contains(indexB, keyA, error);
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
            DTLV.usearch_index_t reloadIndexB = DTLV.dtlv_usearch_handle_index(reloadB);
            expect(DTLV.usearch_contains(reloadIndexA, keyA, error), "Reloaded domain A missing vector");
            expectNoError(error, "Reloaded domain A contains failed");
            error.put(0, (BytePointer) null);
            expect(!DTLV.usearch_contains(reloadIndexA, keyB, error),
                    "Reloaded domain A unexpectedly has domain B vector");
            expectNoError(error, "Reloaded domain A foreign contains failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.usearch_contains(reloadIndexB, keyB, error), "Reloaded domain B missing vector");
            expectNoError(error, "Reloaded domain B contains failed");
            error.put(0, (BytePointer) null);
            expect(!DTLV.usearch_contains(reloadIndexB, keyA, error),
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
        expect(directoryHasSuffix(fsPathA + "/pending", ".ulog"),
                "Domain A pending WAL not sealed");
        expect(directoryHasSuffix(fsPathB + "/pending", ".ulog"),
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
            DTLV.usearch_index_t indexA = DTLV.dtlv_usearch_handle_index(handleA);
            DTLV.usearch_index_t indexB = DTLV.dtlv_usearch_handle_index(handleB);
            expect(indexA != null && !indexA.isNull(), "Reloaded domain A index unavailable");
            expect(indexB != null && !indexB.isNull(), "Reloaded domain B index unavailable");
            expect(DTLV.usearch_contains(indexA, 101L, error),
                    "Reloaded domain A missing published vector");
            expectNoError(error, "Reloaded domain A contains failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.usearch_contains(indexA, 102L, error),
                    "Reloaded domain A missing pending vector");
            expectNoError(error, "Reloaded domain A pending contains failed");
            error.put(0, (BytePointer) null);
            expect(!DTLV.usearch_contains(indexA, 303L, error),
                    "Domain A index leaked domain B vector");
            expectNoError(error, "Domain A cross-domain contains failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.usearch_contains(indexB, 303L, error),
                    "Reloaded domain B missing published vector");
            expectNoError(error, "Reloaded domain B contains failed");
            error.put(0, (BytePointer) null);
            expect(DTLV.usearch_contains(indexB, 304L, error),
                    "Reloaded domain B missing pending vector");
            expectNoError(error, "Reloaded domain B pending contains failed");
            error.put(0, (BytePointer) null);
            expect(!DTLV.usearch_contains(indexB, 101L, error),
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
        expect(!directoryHasSuffix(fsPathA + "/pending", ".ulog"),
                "Domain A pending WAL not cleared after recovery");
        expect(!directoryHasSuffix(fsPathA + "/pending", ".ulog.sealed"),
                "Domain A sealed WAL not cleared after recovery");
        expect(!directoryHasSuffix(fsPathB + "/pending", ".ulog"),
                "Domain B pending WAL not cleared after recovery");
        expect(!directoryHasSuffix(fsPathB + "/pending", ".ulog.sealed"),
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

    public static void main(String[] args) {
        testLMDB();
        System.out.println("----");
        testUsearch();
        System.out.println("----");
        testUsearchLMDBIntegration();
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
            DTLV.usearch_index_t index = DTLV.dtlv_usearch_handle_index(handle);
            expect(index != null && !index.isNull(), "reader missing index");
            PointerPointer<BytePointer> error = new PointerPointer<>(1);
            error.put(0, (BytePointer) null);
            expect(DTLV.usearch_contains(index, key, error),
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
