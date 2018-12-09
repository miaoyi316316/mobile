package com.miao.logmobile.parser.active_mr;

import com.miao.logmobile.etl.etlMapReduce.EtlRunner;
import com.miao.logmobile.etl.util.InitFileSystem;
import com.miao.logmobile.parser.MysqlOutputFormat;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
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

public class ActiveUserRunner implements Tool {


    private static final Logger logger = Logger.getLogger(ActiveUserRunner.class);
    private final String TASKRUNTIME = "runtime";
    private Configuration conf = null;

    public static void main(String[] args) {

        try {
            int status = ToolRunner.run(new ActiveUserRunner(), args);

            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();

        setArgs(args,configuration);

        Job job = Job.getInstance(conf, "action user");

        job.setJarByClass(getClass());

        job.setNumReduceTasks(4);

        job.setMapperClass(ActiveUserMapper.class);

        job.setMapOutputKeyClass(StatsUserDimension.class);

        job.setMapOutputValueClass(MapOutPutWritable.class);


        FileSystem fs = InitFileSystem.getFileSystem();



        FileInputFormat.addInputPath(job,setInputAndOutput(job));

        job.setReducerClass(ActiveUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(ReduceOutputWritable.class);

        job.setOutputFormatClass(MysqlOutputFormat.class);

//        FileOutputFormat.setOutputPath(job,inputAndOutputPath[1]);

        return job.waitForCompletion(true)?0:1;
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


        String[] date = job.getConfiguration().get(TASKRUNTIME).split("-");

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
                    configuration.set(TASKRUNTIME,args[i+1]);
                    break;
                }

            }
        }
        if(StringUtils.isEmpty(configuration.get(TASKRUNTIME))){

            String yesterday = TimeTransform.getYestarday();
            configuration.set(TASKRUNTIME,yesterday);

        }

    }
}
