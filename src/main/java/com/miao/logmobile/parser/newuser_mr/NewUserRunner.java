package com.miao.logmobile.parser.newuser_mr;

import com.miao.logmobile.common.DateTypeEnum;
import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.common.LogFields;
import com.miao.logmobile.etl.etlMapReduce.EtlRunner;
import com.miao.logmobile.etl.util.InitFileSystem;
import com.miao.logmobile.parser.MysqlOutputFormat;
import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.miao.logmobile.service.DimensionInfoImpl;
import com.miao.logmobile.service.IDimensionInfo;
import com.miao.logmobile.service.JDBCService;
import com.miao.logmobile.timeTransform.TimeTransform;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class NewUserRunner implements Tool {

    private static final Logger logger = Logger.getLogger(NewUserRunner.class);

    private Configuration conf = null;


    public static void main(String[] args) {

        try {
            int status = ToolRunner.run(new NewUserRunner(), args);

            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();

        setArgs(args,configuration);

        Job job = Job.getInstance(conf, "new user");

        job.setJarByClass(getClass());

        job.setNumReduceTasks(4);

        job.setMapperClass(NewUserValueMapper.class);

        job.setMapOutputKeyClass(StatsUserDimension.class);

        job.setMapOutputValueClass(MapOutPutWritable.class);

        FileInputFormat.addInputPath(job,setInputAndOutput(job));

        job.setReducerClass(NewUserValueReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(ReduceOutputWritable.class);
        job.setOutputFormatClass(MysqlOutputFormat.class);


        int status = job.waitForCompletion(true) ? 0 : 1;

        //表示插入成功了前面的， 这里实现总用户的思想是先统计出今天的所有维度(由于kpi不同，所以还要进行分配)
        boolean result = false;

        if(status==0){
           result= computer(job,KpiTypeEnum.NEW_ALL_USER);
           result= computer(job,KpiTypeEnum.BROWSE_NEW_ALL_USER);

        }

        return result?0:1;

    }

    private boolean computer(Job job, KpiTypeEnum kpiTypeEnum) {

        IDimensionInfo iDimensionInfo = new DimensionInfoImpl();

        String today = job.getConfiguration().get(LogFields.TASKRUNTIME);

        long todayTime = TimeTransform.String2long(today, "yyyy-MM-dd");

        int todayDateId = iDimensionInfo.getDimensionIdByDim(DateDimension.buildDate(todayTime,DateTypeEnum.DAY));

        long yesterdayTime = todayTime - LogFields.ONE_DAY_TIME;

        DateDimension yesterdayDateDimension = DateDimension.buildDate(yesterdayTime, DateTypeEnum.DAY);

        int yesterdayId = iDimensionInfo.getDimensionIdByDim(yesterdayDateDimension);

        Map<String, Integer> newAllUser = new ConcurrentHashMap<>();

        Connection connection = JDBCService.getConnection();

        PreparedStatement ps = null;


            try {
                if(KpiTypeEnum.BROWSE_NEW_ALL_USER.equals(kpiTypeEnum)) {
                    //查找昨天的各个维度的数据
                    ps = connection.prepareStatement(job.getConfiguration().get("select_plat_bro_totalUser"));

                    ps.setInt(1, yesterdayId);
                    //rs中就是昨天的 stats_device_users中的所有数据 包括维度 platformdimension 和 total_install_users

                    ResultSet rs = ps.executeQuery();

                    //先查出历史数据然后,放入map
                    while (rs.next()) {

                        newAllUser.put(rs.getInt(1) + ","+rs.getInt(2), rs.getInt(3));

                    }

                    //查找出今天的新增用户
                    ps = connection.prepareStatement(job.getConfiguration().get("select_plat_bro_newUser"));
                    ps.setInt(1, todayDateId);

                    rs = ps.executeQuery();

                    ps = connection.prepareStatement(job.getConfiguration().get("bro_new_all_user"));


                    connection.setAutoCommit(false);

                    while (rs.next()) {

                        int platformId = rs.getInt(1);
                        int browserId = rs.getInt(2);
                        int todayNewUser = rs.getInt(3);

                        //如果昨天的数据中查到了对应维度的昨天的总用户，就拿出来类加上今天的
                        if (newAllUser.containsKey(platformId + ","+browserId)) {
                            todayNewUser += newAllUser.get(platformId+","+browserId);
                        }
                        //将今天的这个维度的信息放入map
                        newAllUser.put(platformId + "," + browserId, todayNewUser);

                    }

                    for(Map.Entry<String,Integer> entry:newAllUser.entrySet()){

                        String[] pltAndBro = entry.getKey().split(",");

                        int value = entry.getValue();
                        int i = 0;
                        ps.setInt(++i,todayDateId);
                        ps.setInt(++i,Integer.valueOf(pltAndBro[0]));
                        ps.setInt(++i,Integer.valueOf(pltAndBro[1]));
                        ps.setInt(++i,value);
                        ps.setDate(++i,new Date(new java.util.Date().getTime()));
                        ps.setInt(++i,value);
                        ps.addBatch();

                    }
                    ps.executeBatch();
                    connection.commit();
                }else {
                    ps = connection.prepareStatement(job.getConfiguration().get("select_plat_totalUser"));

                    ps.setInt(1, yesterdayId);
                    //rs中就是昨天的 stats_users中的所有数据 包括维度 platformdimension 和 total_install_users

                    ResultSet rs = ps.executeQuery();

                    //先查出历史数据然后
                    while (rs.next()) {

                        newAllUser.put(rs.getInt(1) + "", rs.getInt(2));

                    }

                    ps = connection.prepareStatement(job.getConfiguration().get("select_plat_newUser"));
                    ps.setInt(1, todayDateId);

                    rs = ps.executeQuery();

                    ps = connection.prepareStatement(job.getConfiguration().get("new_all_user"));

                    //已经查出来想要的昨天的新增用户了，现在把autoCommit关了
                    connection.setAutoCommit(false);

                    while (rs.next()) {

                        int todayNewUser = rs.getInt(2);
                        int platformId = rs.getInt(1);
                        //如果昨天的数据中查到了对应维度的昨天的总用户，就拿出来类加上今天的
                        if (newAllUser.containsKey(platformId +"")) {
                            todayNewUser += newAllUser.get(platformId+"");
                        }
                        newAllUser.put(platformId + "", todayNewUser);
//
//                        int i = 0;
//                        ps.setInt(++i, todayDateId);
//                        ps.setInt(++i, platformId);
//                        ps.setInt(++i, todayNewUser);//注意这里是总用户
//                        ps.setDate(++i, new Date(TimeTransform.String2long(LogFields.TASKRUNTIME,"yyyy-MM-dd")));
//                        ps.setInt(++i, todayNewUser);
//                        ps.addBatch();
                    }
                    for(Map.Entry<String,Integer> entry:newAllUser.entrySet()){


                        int value = entry.getValue();
                        int i = 0;
                        ps.setInt(++i,todayDateId);
                        ps.setInt(++i,Integer.valueOf(entry.getKey()));
                        ps.setInt(++i,value);
                        ps.setDate(++i,new Date(new java.util.Date().getTime()));
                        ps.setInt(++i,value);
                        ps.addBatch();

                    }

                    ps.executeBatch();
                    connection.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return false;

            }
        return true;


    }

    @Override
    public void setConf(Configuration configuration) {

        this.conf = configuration;
        conf.addResource("mysqlwritterClass.xml");
        conf.addResource("mysqlwritterSql.xml");

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * 设置输入输出路径
     * @param job
     * @return
     */
    private Path setInputAndOutput(Job job){



        String[] date = job.getConfiguration().get(LogFields.TASKRUNTIME).split("-");

//        String inputPathStr = "/mobile_logs/input" +"/month="+ date[1] + "/day=" + date[2];

        String inputPathStr = "/mobile_logs/output"+"/month="+ date[1] + "/day=" + date[2];


        return new Path(inputPathStr);
    }

    /**
     * 从参数列表中提取时间参数 添加到conf中
     * @param args
     * @param configuration
     */
    private void setArgs(String[] args,Configuration configuration){


        for(int i=0;i<args.length;i++){
            if(args[i].equals("-d")){

                if(i+1<args.length){
                    configuration.set(LogFields.TASKRUNTIME,args[i+1]);
                    break;
                }

            }
        }
        if(StringUtils.isEmpty(configuration.get(LogFields.TASKRUNTIME))){

            String yesterday = TimeTransform.getYestarday();

            configuration.set(LogFields.TASKRUNTIME,yesterday);

        }

    }

}
