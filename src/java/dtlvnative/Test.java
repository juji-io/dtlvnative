package dtlvnative;

import java.io.*;
import java.nio.*;
import java.nio.file.*;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

public class Test {
    public static void main(String[] args) {

        LMDB.MDB_env env = new LMDB.MDB_env();
        int result = LMDB.mdb_env_create(env);
        if (result != 0) {
            System.err.println("Failed to create LMDB environment: " + result);
            return;
        }

        result = LMDB.mdb_env_set_maxdbs(env, 5);
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

        result = LMDB.mdb_env_open(env, dir, 0, 0664);
        if (result != 0) {
            System.err.println("Failed to open LMDB environment: " + result);
            return;
        }

        LMDB.MDB_txn txn = new LMDB.MDB_txn();
        result = LMDB.mdb_txn_begin(env, null, 0, txn);
        if (result != 0) {
            System.err.println("Failed to begin a r/w transaction: " + result);
            return;
        }

        IntPointer dbi = new IntPointer(1);
        String dbiName = "a";
        result = LMDB.mdb_dbi_open(txn, dbiName, LMDB.MDB_CREATE, dbi);
        if (result != 0) {
            System.err.println("Failed to open dbi: " + result);
            return;
        }

        String keyword = "Meaning of life?";
        Pointer key = new BytePointer(keyword);
        LMDB.MDB_val kval = new LMDB.MDB_val();
        kval.mv_size(keyword.getBytes().length);
        kval.mv_data(key);

        int[] answer = {42};
        Pointer value = new IntPointer(answer);
        LMDB.MDB_val vval = new LMDB.MDB_val();
        vval.mv_size(value.sizeof());
        vval.mv_data(value);

        result = LMDB.mdb_put(txn, dbi.get(), kval, vval, 0);
        if (result != 0) {
            System.err.println("Failed to put key value: " + result);
            return;
        }

        result = LMDB.mdb_txn_commit(txn);
        if (result != 0) {
            System.err.println("Failed to commit transaction: " + result);
            return;
        }

        result = LMDB.mdb_env_sync(env, 1);
        if (result != 0) {
            System.err.println("Failed to sync: " + result);
            return;
        }

        LMDB.MDB_txn rtxn = new LMDB.MDB_txn();
        result = LMDB.mdb_txn_begin(env, null, LMDB.MDB_RDONLY, rtxn);
        if (result != 0) {
            System.err.println("Failed to begin a read only transaction: " + result);
            return;
        }

        result = LMDB.mdb_get(rtxn, dbi.get(), kval, vval);
        if (result != 0) {
            System.err.println("Failed to get key value: " + result);
            return;
        }

        LMDB.mdb_txn_abort(rtxn);

        Pointer.free(dbi);
        Pointer.free(key);
        Pointer.free(value);

        LMDB.mdb_env_close(env);

        System.out.println("JavaCPP binding for LMDB is working.");
    }
}
