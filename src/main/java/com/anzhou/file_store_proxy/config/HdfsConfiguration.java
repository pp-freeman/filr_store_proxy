package com.anzhou.file_store_proxy.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *@author pengpan
 *@description HDFS文件系统配置类
 *@date 13:02 2022/09/09
 */
@Configuration
@RefreshScope
@Slf4j
public class HdfsConfiguration {

    @Value("${hdfs_uri}")
    private String defaultDfs;
    @Value("${hadoop_user_name}")
    private String hadoopUserName;

    @Bean
    public HdfsOperations getHdfsOperations() {
        System.setProperty("HADOOP_USER_NAME",hadoopUserName);
        log.info("HADOOP_USER_NAME:{}",hadoopUserName);
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.defaultFS", defaultDfs);
        return new HdfsOperations(conf, defaultDfs);
    }
}
