package com.miao.logmobile.parser;

import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.modle.dim.StatsBaseDimension;
import com.miao.logmobile.parser.modle.dim.value.StatsBaseOutputDimension;
import com.miao.logmobile.service.DimensionInfoImpl;
import com.miao.logmobile.service.JDBCService;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.conf.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MysqlOutputFormat extends OutputFormat<StatsBaseDimension,StatsBaseOutputDimension> {


    @Override
    public RecordWriter<StatsBaseDimension, StatsBaseOutputDimension> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        Configuration conf = taskAttemptContext.getConfiguration();
        Connection connection = JDBCService.getConnection();

        return new MysqlRecordWriter(connection,conf);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        String name = taskAttemptContext.getConfiguration().get("mapred.output.dir");

        Path path = name == null ? null : new Path(name);

        return new FileOutputCommitter(path, taskAttemptContext);
    }

    public static class MysqlRecordWriter extends RecordWriter<StatsBaseDimension,StatsBaseOutputDimension>{


        private Connection connection;

        private Configuration conf;

        private Map<KpiTypeEnum, PreparedStatement> cache = new HashMap<>();

        private Map<KpiTypeEnum, Integer> batch = new HashMap<>();


        public MysqlRecordWriter(Connection connection, Configuration conf) {
            this.connection = connection;
            try {
                this.connection.setAutoCommit(false);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            this.conf = conf;
        }

        @Override
        public void write(StatsBaseDimension key, StatsBaseOutputDimension value) throws IOException, InterruptedException {

            PreparedStatement ps = null;

            if(key==null||value==null){
                throw new RuntimeException("传入到writer的参数异常");
            }

            KpiTypeEnum kpiTypeEnum = value.getKpiTypeName();

            try {
                //使用反射的方式
                //在配置文件中写实现类的  name ->类名的映射
                Class<?> reduceOutputFormatImpl = Class.forName(conf.get("mysqlwritterClass"));

                IReduceOutputFormat iReduceOutputFormat =(IReduceOutputFormat)reduceOutputFormatImpl.newInstance();

                if(this.cache.containsKey(kpiTypeEnum)&&this.batch.containsKey(kpiTypeEnum)){
                    ps = this.cache.get(kpiTypeEnum);

                    int oldcount = this.batch.get(kpiTypeEnum);

                    this.batch.put(kpiTypeEnum, oldcount + 1);

                }else {

                    String sql = iReduceOutputFormat.buildInsertSql(kpiTypeEnum, conf);

                    ps = this.connection.prepareStatement(sql);

                    this.cache.put(kpiTypeEnum, ps);

                    this.batch.put(kpiTypeEnum, 1);
                }

                iReduceOutputFormat.buildInsertPs(new DimensionInfoImpl(),key,value,ps);


            } catch (Exception e) {
                e.printStackTrace();
            }


            //批量提交  当该kpi指标的ps个数达到30执行
            if(this.batch.get(kpiTypeEnum)>=30){

                try {
                    this.cache.get(kpiTypeEnum).executeBatch();

                    connection.commit();

                    this.cache.remove(kpiTypeEnum);

                    this.batch.remove(kpiTypeEnum);

                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {


            for(Map.Entry<KpiTypeEnum,PreparedStatement> cacheEntry:cache.entrySet()){

                try {
                    cacheEntry.getValue().executeBatch();

                    connection.commit();

                    cacheEntry.getValue().close();

                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
            if (connection!=null)
            JDBCService.colseAll(connection,null,null);

        }
    }
}
