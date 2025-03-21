package org.apache.flink.playgrounds.spendreport;

import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.Op;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;

public class RocksdbTest {

    public static void main(String[] args) throws Exception {
        //可成功加载，flink在windows上运行时使用rocksdbjni替代frocksdbjni
        RocksDB.loadLibrary();

        Options options=new Options();
        options.setCreateIfMissing(true);
        RocksDB rocksDB=RocksDB.open(options,"data/rocksdb");
        byte[] key="flink".getBytes(StandardCharsets.UTF_8);
        byte[] value="rocksdb".getBytes(StandardCharsets.UTF_8);
        //写数据
        rocksDB.put(key,value);
        //读数据
        System.out.println(new String(rocksDB.get(key)));
        //删除数据
        rocksDB.delete(key);
        
        //测试内核出错部分代码
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        columnFamilyOptions.setMinWriteBufferNumberToMerge(1);

        System.out.println("结束");

    }

}
