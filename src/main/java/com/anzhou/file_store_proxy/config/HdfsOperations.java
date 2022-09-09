package com.anzhou.file_store_proxy.config;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *@author pengpan
 *@description HDFS文件系统操作类
 *@date 13:01 2022/09/09
 */
public class HdfsOperations {

    private Logger logger = LoggerFactory.getLogger(HdfsOperations.class);
    private Configuration conf = null;

    private String defaultHdfsUri;

    public HdfsOperations(Configuration conf, String defaultHdfsUri) {
        this.conf = conf;
        this.defaultHdfsUri = defaultHdfsUri;
    }

    /**
     ** 获取HDFS文件系统
     **
     * @return org.apache.hadoop.fs.FileSystem
     */
    private FileSystem getFileSystem() throws IOException {
        return FileSystem.newInstance(conf);
    }

    /**
     * 创建HDFS目录
     *
     * @author anzhou
     * @since 1.0.0
     * @param path HDFS的相对目录路径，比如：/testDir
     * @return boolean 是否创建成功
     */
    public boolean mkdir(String path) {
        // 如果目录已经存在，则直接返回
        if (checkExists(path)) {
            return true;
        } else {
            FileSystem fileSystem = null;
            try {
                fileSystem = getFileSystem();
                // 最终的HDFS文件目录
                String hdfsPath = generateHdfsPath(path);
                // 创建目录
                return fileSystem.mkdirs(new Path(hdfsPath));
            } catch (IOException e) {
                logger.error(MessageFormat.format("创建HDFS目录失败，path:{0}", path), e);
                return false;
            } finally {
                close(fileSystem);
            }
        }
    }

    /**
     * 上传文件至HDFS
     *
     * @author anzhou
     * @since 1.0.0
     * @param srcFile 本地文件路径，比如：D:/test.txt
     * @param dstPath HDFS的相对目录路径，比如：/testDir
     */
    public void uploadFileToHdfs(String srcFile, String dstPath) {
        this.uploadFileToHdfs(false, true, srcFile, dstPath);
    }

    /**
     * 上传文件至HDFS
     *
     * @author anzhou
     * @since 1.0.0
     * @param delSrc    是否删除本地文件
     * @param overwrite 是否覆盖HDFS上面的文件
     * @param srcFile   本地文件路径，比如：D:/test.txt
     * @param dstPath   HDFS的相对目录路径，比如：/testDir
     */
    public void uploadFileToHdfs(boolean delSrc, boolean overwrite, String srcFile, String dstPath) {
        FileSystem fileSystem = null;
        try {
            // 源文件路径
            Path localSrcPath = new Path(srcFile);
            String fileName = localSrcPath.getName();
            // 目标文件路径
            Path hdfsDstPath = new Path(generateHdfsPath(dstPath));
            fileSystem = getFileSystem();
            fileSystem.copyFromLocalFile(delSrc, overwrite, localSrcPath, hdfsDstPath);
        } catch (IOException e) {
            logger.error(MessageFormat.format("上传文件至HDFS失败，srcFile:{0},dstPath:{1}", srcFile, dstPath), e);
        } finally {
            close(fileSystem);
        }
    }

    /**
     * 通过文件流上传文件至HDFS
     * @param overwrite 是否覆盖
     * @param in 输入流
     * @param dstPath 目标目录
     * @param filename 目标文件名 e.g.(1.txt)
     */
    public void uploadFileToHdfsByFlow(boolean overwrite, InputStream in, String dstPath, String filename) {
        FileSystem fileSystem = null;
        FSDataOutputStream out = null;
        try {
            // 目标文件路径
            Path hdfsDstPath = new Path(generateHdfsPath(dstPath + "/" + filename));
            fileSystem = getFileSystem();
            out = fileSystem.create(hdfsDstPath, overwrite);
            byte[] buffer = new byte[1024 * 8];
            int read = 0;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
                out.flush();
            }
        } catch (IOException e) {
            logger.error(MessageFormat.format("上传文件至HDFS失败,dstPath:{1}", dstPath), e);
        } finally {
            try {
                close(fileSystem);
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 判断文件或者目录是否在HDFS上面存在
     *
     * @author anzhou
     * @since 1.0.0
     * @param path HDFS的相对目录路径，比如：/testDir、/testDir/a.txt
     * @return boolean
     */
    public boolean checkExists(String path) {
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            // 最终的HDFS文件目录
            String hdfsPath = generateHdfsPath(path);
            // 创建目录
            return fileSystem.exists(new Path(hdfsPath));
        } catch (IOException e) {
            logger.error(MessageFormat.format("'判断文件或者目录是否在HDFS上面存在'失败，path:{0}", path), e);
            return false;
        } finally {
            close(fileSystem);
        }
    }

    /**
     * @列出basePath下的一级子目录或者子文件
     * @param parentPath   父目录
     * @param pathFilter 过滤器，没有则置为null
     * @param listDir      选择器，选择列出文件还是目录,<code>true</code>则为目录，<code>false</code>则为文件
     * @return basePath下的一级子目录或者文件
     */
    public List<String> listChilds(String parentPath, PathFilter pathFilter, boolean listDir) {
        List<String> fileRes = new ArrayList<String>();
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            String path = generateHdfsPath(parentPath);
            FileStatus[] statues = null;
            if (pathFilter == null) {
                statues = fileSystem.listStatus(new Path(path));
            } else {
                statues = fileSystem.listStatus(new Path(path), pathFilter);
            }
            if (statues != null && statues.length > 0) {
                for (FileStatus st : statues) {
                    if (st.isDirectory() && listDir) {
                        fileRes.add(st.getPath().toString());
                    } else if (st.isFile() && !listDir) {
                        fileRes.add(st.getPath().toString());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(fileSystem);
        }
        return fileRes;
    }

    /**
     * 获取HDFS上面的某个路径下面的所有文件或目录（不包含子目录）信息
     *
     * @author anzhou
     * @since 1.0.0
     * @param path HDFS的相对目录路径，比如：/testDir
     * @return java.util.List<java.util.Map<java.lang.String,java.lang.Object>>
     */
    public List<Map<String, Object>> listFiles(String path, PathFilter pathFilter) {
        // 返回数据
        List<Map<String, Object>> result = new ArrayList<>();

        // 如果目录已经存在，则继续操作
        if (checkExists(path)) {
            FileSystem fileSystem = null;
            try {
                fileSystem = getFileSystem();
                // 最终的HDFS文件目录
                String hdfsPath = generateHdfsPath(path);
                FileStatus[] statuses;
                // 根据Path过滤器查询
                if (pathFilter != null) {
                    statuses = fileSystem.listStatus(new Path(hdfsPath), pathFilter);
                } else {
                    statuses = fileSystem.listStatus(new Path(hdfsPath));
                }
                if (statuses != null) {
                    for (FileStatus status : statuses) {
                        // 每个文件的属性
                        Map<String, Object> fileMap = new HashMap<>(2);
                        fileMap.put("path", status.getPath().toString());
                        fileMap.put("isDir", status.isDirectory());
                        result.add(fileMap);
                    }
                }
            } catch (IOException e) {
                logger.error(MessageFormat.format("获取HDFS上面的某个路径下面的所有文件失败，path:{0}", path), e);
            } finally {
                close(fileSystem);
            }
        }

        return result;
    }

    /**
     * 从HDFS下载文件至本地
     *
     * @author anzhou
     * @since 1.0.0
     * @param srcFile HDFS的相对目录路径，比如：/testDir/a.txt
     * @param dstFile 下载之后本地文件路径（如果本地文件目录不存在，则会自动创建），比如：D:/test.txt
     */
    public void downloadFileFromHdfs(String srcFile, String dstFile) {
        // HDFS文件路径
        Path hdfsSrcPath = new Path(generateHdfsPath(srcFile));
        // 下载之后本地文件路径
        Path localDstPath = new Path(dstFile);
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            this.delete(localDstPath.toString());
            fileSystem.copyToLocalFile(false, hdfsSrcPath, localDstPath, true);
        } catch (IOException e) {
            logger.error(MessageFormat.format("从HDFS下载文件至本地失败，srcFile:{0},dstFile:{1}", srcFile, dstFile), e);
        } finally {
            close(fileSystem);
        }
    }

    /**
     * 打开HDFS上面的文件并返回 InputStream
     *
     * @author anzhou
     * @since 1.0.0
     * @param path HDFS的相对目录路径，比如：/testDir/c.txt
     * @return FSDataInputStream
     */
    public FSDataInputStream open(String path) {
        // HDFS文件路径
        Path hdfsPath = new Path(generateHdfsPath(path));
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            return fileSystem.open(hdfsPath);
        } catch (IOException e) {
            logger.error(MessageFormat.format("打开HDFS上面的文件失败，path:{0}", path), e);
        }

        return null;
    }

    /**
     * 打开HDFS上面的文件并返回byte数组，方便Web端下载文件
     * <p>
     * new ResponseEntity<byte[]>(byte数组, headers, HttpStatus.CREATED);
     * </p>
     * <p>
     * 或者：new ResponseEntity<byte[]>(FileUtils.readFileToByteArray(templateFile),
     * headers, HttpStatus.CREATED);
     * </p>
     *
     * @author anzhou
     * @since 1.0.0
     * @param path HDFS的相对目录路径，比如：/testDir/b.txt
     * @return FSDataInputStream
     */
    public byte[] openWithBytes(String path) {
        // HDFS文件路径
        Path hdfsPath = new Path(generateHdfsPath(path));
        FileSystem fileSystem = null;
        FSDataInputStream inputStream = null;
        try {
            fileSystem = getFileSystem();
            inputStream = fileSystem.open(hdfsPath);
            return IOUtils.toByteArray(inputStream);
        } catch (IOException e) {
            logger.error(MessageFormat.format("打开HDFS上面的文件失败，path:{0}", path), e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }

        return null;
    }

    /**
     * 打开HDFS上面的文件并返回String字符串
     *
     * @author anzhou
     * @since 1.0.0
     * @param path HDFS的相对目录路径，比如：/testDir/b.txt
     * @return FSDataInputStream
     */
    public String openWithString(String path) {
        // HDFS文件路径
        Path hdfsPath = new Path(generateHdfsPath(path));
        FileSystem fileSystem = null;
        FSDataInputStream inputStream = null;
        try {
            fileSystem = getFileSystem();
            inputStream = fileSystem.open(hdfsPath);
            return IOUtils.toString(inputStream, Charset.forName("UTF-8"));
        } catch (IOException e) {
            logger.error(MessageFormat.format("打开HDFS上面的文件失败，path:{0}", path), e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }

        return null;
    }

    /**
     * 打开HDFS上面的文件并转换为Java对象（需要HDFS上门的文件内容为JSON字符串）
     *
     * @author anzhou
     * @since 1.0.0
     * @param path HDFS的相对目录路径，比如：/testDir/c.txt
     * @return FSDataInputStream
     */
    public <T extends Object> T openWithObject(String path, Class<T> clanzhouz) {
        // 1、获得文件的json字符串
        String jsonStr = this.openWithString(path);

        // 2、使用com.alibaba.fastjson.JSON将json字符串转化为Java对象并返回
        return JSON.parseObject(jsonStr, clanzhouz);
    }

    /**
     * 重命名
     *
     * @author anzhou
     * @since 1.0.0
     * @param srcFile 重命名之前的HDFS的相对目录路径，比如：/testDir/b.txt
     * @param dstFile 重命名之后的HDFS的相对目录路径，比如：/testDir/b_new.txt
     */
    public boolean rename(String srcFile, String dstFile) {
        // HDFS文件路径
        Path srcFilePath = new Path(generateHdfsPath(srcFile));
        // 下载之后本地文件路径
        Path dstFilePath = new Path(dstFile);

        FileSystem fileSystem = null;
        try {

            fileSystem = getFileSystem();

            return fileSystem.rename(srcFilePath, dstFilePath);
        } catch (IOException e) {
            logger.error(MessageFormat.format("重命名失败，srcFile:{0},dstFile:{1}", srcFile, dstFile), e);
        } finally {
            close(fileSystem);
        }

        return false;
    }

    /**
     * 删除HDFS文件或目录
     *
     * @author anzhou
     * @since 1.0.0
     * @param path HDFS的相对目录路径，比如：/testDir/c.txt
     * @return boolean
     */
    public boolean delete(String path) {
        // HDFS文件路径
        Path hdfsPath = new Path(generateHdfsPath(path));

        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();

            return fileSystem.delete(hdfsPath, true);
        } catch (IOException e) {
            logger.error(MessageFormat.format("删除HDFS文件或目录失败，path:{0}", path), e);
        } finally {
            close(fileSystem);
        }

        return false;
    }


    /**
     * 创建HDFS上的文件目录
     *
     * @author anzhou
     * @since 1.0.0
     * @param path HDFS的相对目录路径，比如：/testDir/c.txt
     * @return boolean
     */
    public void createPath(String path) {
        // HDFS文件路径
        Path hdfsPath = new Path(generateHdfsPath(path));

        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            if(fileSystem.exists(hdfsPath)){
                logger.debug("path:{} is exists",path);
                return;
            }
            fileSystem.mkdirs(hdfsPath);
        } catch (IOException e) {
            logger.error(MessageFormat.format("创建HDFS文件或目录失败，path:{0}", path), e);
        } finally {
            close(fileSystem);
        }
    }

    /**
     * 获取某个文件在HDFS集群的位置
     *
     * @author anzhou
     * @since 1.0.0
     * @param path HDFS的相对目录路径，比如：/testDir/a.txt
     * @return org.apache.hadoop.fs.BlockLocation[]
     */
    public BlockLocation[] getFileBlockLocations(String path) {
        // HDFS文件路径
        Path hdfsPath = new Path(generateHdfsPath(path));

        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            FileStatus fileStatus = fileSystem.getFileStatus(hdfsPath);

            return fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        } catch (IOException e) {
            logger.error(MessageFormat.format("获取某个文件在HDFS集群的位置失败，path:{0}", path), e);
        } finally {
            close(fileSystem);
        }

        return null;
    }

    /**
     * 将相对路径转化为HDFS文件路径
     *
     * @author anzhou
     * @since 1.0.0
     * @param dstPath 相对路径，比如：/data
     * @return java.lang.String
     */
    private String generateHdfsPath(String dstPath) {
        String hdfsPath = defaultHdfsUri;
        if (dstPath.startsWith("hdfs")) {
            return dstPath;
        }
        if (dstPath.startsWith("/")) {
            hdfsPath += dstPath;
        } else {
            hdfsPath = hdfsPath + "/" + dstPath;
        }

        return hdfsPath;
    }

    /**
     * @简单测试系统是否联通
     * @return
     */
    public  boolean checkFileSystem() {
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            fs.getStatus();
            return true;
        } catch (Exception e) {
            logger.error("无法连接HDFS");
            return false;
        } finally {
            close(fs);
        }
    }

    /**
     * close方法
     */
    private void close(FileSystem fileSystem) {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }



    public static void main(String[] args){
        String defaultDfs = "hdfs://192.168.111.134:9020";
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.defaultFS", defaultDfs);
        HdfsOperations operations = new HdfsOperations(conf,defaultDfs);
        operations.mkdir("/test");
        //operations.delete("/data/hap//0/");
        //operations.delete("/data/hap/task/");
        //operations.uploadFileToHdfs("E://test/analysis.json","/data/hap/task/");
    }
}
