package org.apache.rocketmq.streams.state;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class RocksDBStore<K, V> extends AbstractStore<K, V> {
    private static final String ROCKSDB_PATH = "/tmp/rocksdb";
    private RocksDB rocksDB;
    private volatile boolean created = false;
    private WriteOptions writeOptions;


    private void createRocksDB() {
        try (final Options options = new Options().setCreateIfMissing(true)) {

            try {
                String localAddress = RemotingUtil.getLocalAddress();
                int pid = UtilAll.getPid();

                String rocksdbFilePath = String.format("%s/%s/%s", ROCKSDB_PATH, localAddress, pid);


                File dir = new File(rocksdbFilePath);
                if (dir.exists() && !dir.delete()) {
                    throw new RuntimeException("before create rocksdb, delete exist path " + rocksdbFilePath + " error");
                }

                if (!dir.mkdirs()) {
                    throw new RuntimeException("before create rocksdb,mkdir path " + rocksdbFilePath + " error");
                }

                this.rocksDB = TtlDB.open(options, rocksdbFilePath, 10800, false);

                writeOptions = new WriteOptions();
                writeOptions.setSync(false);
                writeOptions.setDisableWAL(true);
            } catch (RocksDBException e) {
                throw new RuntimeException("create rocksdb error " + e.getMessage());
            }
        }
    }

    @Override
    public synchronized void init() {
        if (!created) {
            createRocksDB();
            created = true;
        }
    }

    @Override
    public void recover() {

    }

    @Override
    public V get(K key) {
        if (key == null) {
            return null;
        }

        try {
            byte[] bytes = ((String) key).getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = rocksDB.get(bytes);

            if (valueBytes == null || valueBytes.length == 0) {
                return null;
            }

            return (V) new String(valueBytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Override
    public void put(K key, V value) {
        try {
            byte[] keyBytes = ((String) key).getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
            rocksDB.put(writeOptions, keyBytes, valueBytes);
        } catch (Exception e) {
            throw new RuntimeException("putWindowInstance to rocksdb error", e);
        }
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws Exception {

    }
}
