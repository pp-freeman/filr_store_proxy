package com.anzhou.file_store_proxy.controller;

import com.anzhou.file_store_proxy.util.RSAUtils;
import com.anzhou.file_store_proxy.config.HdfsOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

@RestController
@RequestMapping("/proxy")
public class FileProxyController {

    private Logger logger = LoggerFactory.getLogger(FileProxyController.class);
    public static Map<String, String> keyMap = RSAUtils.createKeys(1024);
    @Autowired
    HdfsOperations hdfsOperations;
    @Value("${hdfsPath}")
    private String hdfsPath;

    @GetMapping("/publicKey")
    public String getPublicKey() {
        return keyMap.get("publicKey");
    }

    @PostMapping("/upload")
    public String upload(MultipartFile file, HttpServletRequest req) throws IOException {
        InputStream in = null;
        try {
            in = new ByteArrayInputStream(RSAUtils.privateDecrypt(file.getBytes(), RSAUtils.getPrivateKey(keyMap.get("privateKey"))));
            String filename = file.getOriginalFilename();
            int unixSep = filename.lastIndexOf('/');
            int winSep = filename.lastIndexOf('\\');
            int pos = (winSep > unixSep ? winSep : unixSep);
            if (pos != -1)  {
                filename = filename.substring(pos + 1);
            }
            //文件上传至hdfs
            String hdfPath = "";
            if (hdfsPath.endsWith("/")) {
                hdfPath = hdfsPath + "2109/" + dateToStr();
            } else {
                hdfPath = hdfsPath + "/2109/" + dateToStr();
            }
            if (!hdfsOperations.checkExists(hdfPath)) {
                hdfsOperations.mkdir(hdfPath);
            }
            hdfsOperations.uploadFileToHdfsByFlow(true, in, hdfPath, filename);
            return "success";
        } catch (Exception e) {
            logger.error("文件上传至hdfs失败,error: ", e.getMessage());
            e.printStackTrace();
        } finally {
            in.close();
        }
        return "false";
    }

    /****
     * 日期转换字符串
     * @return
     */
    private String dateToStr() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String dateTime = dateFormat.format(new Date());
        return dateTime;
    }
}
