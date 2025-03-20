/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playgrounds.spendreport;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class BaseTests {

    public static Table report(Table transactions) {
        throw new UnimplementedException();
    }

    /**
     * 测试建表语句
     * create table test_table (
     * account_id BIGINT,
     * log_ts BIGINT,
     * amount BIGINT,
     * primary key (account_id,log_ts) NOT ENFORCED)
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //stream mode，不支持update和delete，
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //batch mode
/*        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);*/


        //创建mysql源，这里实际不会创建表，而是和mysql已有表关联
        tEnv.executeSql("CREATE TABLE test_table_source (\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     BIGINT,\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "  'connector'  = 'jdbc',\n" +
                "  'url'        = 'jdbc:mysql://10.82.12.23:3306/vhr?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnectForPools=true&failOverReadOnly=false&allowMultiQueries=true',\n" +
                "  'table-name' = 'test_table',\n" +
                "  'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "  'username'   = 'root',\n" +
                "  'password'   = 'Rzx_1218'\n" +
                ")");

        //删数据
        //tEnv.executeSql("delete from test_table_source where account_id=1");
        //写数据，主键冲突不会报错，写入会失败
        tEnv.executeSql("insert into test_table_source(account_id,log_ts,amount) values (1,2,3)");
        //更新数据
        //tEnv.executeSql("update test_table_source set amount=5  where account_id=1");


        //读数据,会有op列，但是读取的是最终数据；
        TableResult execute = tEnv.sqlQuery("select * from test_table_source").execute();
        execute.print();
    }

    public void jdbcSinkTest(){
        //JdbcSink  https://zhuanlan.zhihu.com/p/665189359
    }
}
