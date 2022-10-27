package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.entity.clean.Cleaner;
import com.eurlanda.datashire.engine.enumeration.PostProcess;
import com.eurlanda.datashire.engine.enumeration.RemoteType;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.ServerRpcUtil;
import com.eurlanda.datashire.enumeration.EncodingType;
import jcifs.smb.SmbFile;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.CustomJavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Juntao.Zhang on 14-6-4.
 * 将指定的共享文件抽取转化生成RDD
 */
public class TSharedFileSquid extends TSquid implements IExtractSquid {
    // 服务器ip
    private String ip;
    // 指定的文件
    private String[] files;
    // 用户名
    private String username;
    // 密码
    private String password;
    // 文件类型
    private String fileType;
    // 数据抽取行 第一条数据记录
    private int firstDataRowNo;
    // 编码
    private EncodingType encoding;
    //文件行分割信息
    private FileRecordSeparator fileLineSeparator;//xml web log
    // connection squid id
    private Integer connectionSquidId;

    public Integer getConnectionSquidId() {
        return connectionSquidId;
    }

    public void setConnectionSquidId(Integer connectionSquidId) {
        this.connectionSquidId = connectionSquidId;
    }

    public TSharedFileSquid() {
        this.setType(TSquidType.SHARED_FILE_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException {
        // 发送connectionSquid运行成功
        ServerRpcUtil.sendConnectionSquidSuccess(isDebug(), this, connectionSquidId);

        logBeforeStart();
        // 共享文件夹 不需要指定服务器编码
        this.outRDD = ((CustomJavaSparkContext) jsc).ftpSharedFolderFile(ip, -1,
                null,
                files, username,
                password, fileType, RemoteType.SHARED_FILE,
                null, encoding, fileLineSeparator);
        return null;
    }

    private void logBeforeStart() {
        fileLineSeparator.setFileType(fileType);
        if (fileType.equalsIgnoreCase("LOG")) {
            switch (fileLineSeparator.getLogFormatType()) {
                case EXTENDED_LOG_FORMAT:
                    fileLineSeparator.setFirstDataRowNo(3);
                    break;
                default:
                    break;
            }
        }
        firstDataRowNo = fileLineSeparator.getFirstDataRowNo();
    }

    @Override
    protected void clean() {
        super.clean();
        this.getCurrentFlow().addCleaner(new Cleaner() {
            public void doSuccess() {
                deleteFile();
            }
        });
    }

    private void deleteFile() {
        if (fileLineSeparator != null && fileLineSeparator.getPostProcess() == PostProcess.DELETE) {
            try {
                for (String p : files) {
                    String uri;
                    if (StringUtils.isBlank(username) && StringUtils.isBlank(password)) {
                        uri = "smb://" + ip + "/" + p;
                    } else {
                        uri = "smb://" + username + ":" + password + "@" + ip + "/" + p;
                    }
                    new SmbFile(uri).delete();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String[] getFiles() {
        return files;
    }

    public void setFiles(String[] files) {
        this.files = files;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public int getFirstDataRowNo() {
        return firstDataRowNo;
    }

    public void setFirstDataRowNo(int firstDataRowNo) {
        this.firstDataRowNo = firstDataRowNo;
    }

    public EncodingType getEncoding() {
        return encoding;
    }

    public void setEncoding(EncodingType encoding) {
        this.encoding = encoding;
    }

    public FileRecordSeparator getFileLineSeparator() {
        return fileLineSeparator;
    }

    public void setFileLineSeparator(FileRecordSeparator fileLineSeparator) {
        this.fileLineSeparator = fileLineSeparator;
    }
}


