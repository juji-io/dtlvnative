package dtlvnative;

import java.io.*;
import java.nio.*;
import java.nio.file.*;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

public class Test {
    public static void main(String[] args) {

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
            System.err.println("Failed to set comparator for r/w transaction: " + result);
            return;
        }

        String keyword = "Meaning of life?";
        Pointer key = new BytePointer(keyword);
        DTLV.MDB_val kval = new DTLV.MDB_val();
        kval.mv_size(keyword.getBytes().length);
        kval.mv_data(key);

        int[] answer = {42};
        Pointer value = new IntPointer(answer);
        DTLV.MDB_val vval = new DTLV.MDB_val();
        vval.mv_size(value.sizeof());
        vval.mv_data(value);

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

        DTLV.MDB_txn rtxn = new DTLV.MDB_txn();
        result = DTLV.mdb_txn_begin(env, null, DTLV.MDB_RDONLY, rtxn);
        if (result != 0) {
            System.err.println("Failed to begin a read only transaction: " + result);
            return;
        }

        result = DTLV.dtlv_set_comparator(rtxn, dbi.get());
        if (result != 0) {
            System.err.println("Failed to set comparator for read transaction: " + result);
            return;
        }

        result = DTLV.mdb_get(rtxn, dbi.get(), kval, vval);
        if (result != 0) {
            System.err.println("Failed to get key value: " + result);
            return;
        }

        DTLV.mdb_txn_abort(rtxn);

        Pointer.free(dbi);
        Pointer.free(key);
        Pointer.free(value);

        DTLV.mdb_env_close(env);

        System.out.println("JavaCPP binding for DTLV is working.");
    }
}
