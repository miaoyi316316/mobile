package com.miao.logmobile.etl.etlMapReduce;


import com.miao.logmobile.etl.etlDimension.LogDimension;
import com.miao.logmobile.etl.util.InitFileSystem;
import com.miao.logmobile.timeTransform.TimeTransform;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;


public class EtlRunner implements Tool {

    private static final Logger logger = Logger.getLogger(EtlRunner.class);
    private final String TASKRUNTIME = "runtime";
    private Configuration conf = null;

    public static void main(String[] args) {

        try {
            int status = ToolRunner.run(new EtlRunner(), args);

            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();

        setArgs(args,configuration);

        Job job = Job.getInstance(conf, "logToHdfs");

        job.setJarByClass(getClass());

        job.setNumReduceTasks(0);

        job.setMapperClass(EtlMapper.class);

        job.setMapOutputKeyClass(LogDimension.class);

        job.setMapOutputValueClass(NullWritable.class);


        FileSystem fs = InitFileSystem.getFileSystem();

        Path[] inputAndOutputPath = setInputAndOutput(job, fs);

        FileInputFormat.addInputPath(job,inputAndOutputPath[0]);

        FileOutputFormat.setOutputPath(job,inputAndOutputPath[1]);

        return job.waitForCompletion(true)?0:1;
    }

    @Override
    public void setConf(Configuration configuration) {

        this.conf = configuration;

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
    private Path[] setInputAndOutput(Job job,FileSystem fs){

        Path[] paths = new Path[2];

        String[] date = job.getConfiguration().get(TASKRUNTIME).split("-");
        String inputPathStr = "/mobile_logs/input" +"/month="+ date[1] + "/day=" + date[2];

        String outputPatStr = "/mobile_logs/output"+"/month="+ date[1] + "/day=" + date[2];

        paths[0] = new Path(inputPathStr);

        paths[1] = new Path(outputPatStr);

        try {
            if(fs.exists(paths[1])){
                fs.delete(paths[1], true);
            }

        } catch (IOException e) {
            logger.warn("mapreduce任务的输入输出路径异常",e);
        }
        return paths;
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
