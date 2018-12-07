package com.miao.logmobile.parser.custom_outputformat;

import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;

import com.miao.logmobile.service.JDBCService;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class NewUserOutputFormat extends OutputFormat<StatsUserDimension,ReduceOutputWritable> {

    @Override
    public RecordWriter<StatsUserDimension, ReduceOutputWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

//        Connection connection = JDBCService.getConnection();
//
//        try {
//            return new MysqlRecordWriter(connection);
//        } catch (SQLException e) {
//            throw new RuntimeException("初始化connect出错 " + e);
//        }
            return null;
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

    public class MysqlRecordWriter extends RecordWriter<StatsUserDimension,ReduceOutputWritable> {

        private Connection connection;
        private PreparedStatement ps;

        private ResultSet rs;


        public MysqlRecordWriter(Connection connection) throws SQLException {
            this.connection = connection;

        }
        @Override
        public void write(StatsUserDimension key, ReduceOutputWritable value) throws IOException, InterruptedException {
            System.out.println(value.getKpiTypeName().getKpiType()+"*******************writer");

            IntWritable reduceOutputMapValue;
            Text reduceOutputMapKey;
            Map<String,Integer> map = new HashMap<>();
            for(Map.Entry<Writable,Writable> entry:value.getValue().entrySet()){

                reduceOutputMapKey = (Text) entry.getKey();
                reduceOutputMapValue = (IntWritable) entry.getValue();
                map.put(reduceOutputMapKey.toString(), reduceOutputMapValue.get());

            }

            try {
                //构建sql
                String[] sqls = ResultFields.createSqls(value.getKpiTypeName());

                //先查询，查看当前维度 除了时间之外  上一天的总用户字段的值
                synchronized (this){
                    ps = connection.prepareStatement(sqls[0]);

                    ResultFields.buildSelectSqlForYesterday(ps, value.getKpiTypeName(), map);
                    rs = ps.executeQuery();
                    System.out.println(sqls[0]+"***************");

                if(rs.next()){
                    //得到之前的总用户量 然后加上今天新增的  放入当天这条数据的总用户字段
                    int old_all_user = rs.getInt(1);

                    int new_all_user = old_all_user + map.get("new_install_users");

                    map.put("total_install_users",new_all_user);
                }else {
                    //没查到之前有数据 直接把当天的新增用户放到总用户字段
                    map.put("total_install_users",map.get("new_install_users"));
                }

                ps = connection.prepareStatement(sqls[1]);

                ResultFields.buildInsertSql(ps,value.getKpiTypeName(),map);

                ps.executeUpdate();
                }

            } catch (SQLException e) {
//                throw new RuntimeException("向 "+value.getKpiTypeName().getKpiType()+" 结果表中插入数据失败"+"  "+e);
                e.printStackTrace();
            }

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            JDBCService.colseAll(connection,ps,null);
        }
    }

}
