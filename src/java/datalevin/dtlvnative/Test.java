package datalevin.dtlvnative;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.function.LongPredicate;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
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

        System.out.println("Passed all usearch tests.");
    }

    public static void main(String[] args) {
        testLMDB();
        System.out.println("----");
        testUsearch();
    }
}
