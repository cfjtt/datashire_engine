package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.entity.clean.Cleaner;
import com.eurlanda.datashire.engine.enumeration.PostProcess;
import com.eurlanda.datashire.engine.enumeration.RemoteType;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.EngineLogFactory;
import com.eurlanda.datashire.engine.util.FtpUtil;
import com.eurlanda.datashire.engine.util.ServerRpcUtil;
import com.eurlanda.datashire.enumeration.EncodingType;
import org.apache.spark.CustomJavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhudebin on 14-5-14.
 * 将指定的FTP文件抽取转化生成RDD
 */
public class TFtpFileSquid extends TSquid implements IExtractSquid {

    private static Logger log = LoggerFactory.getLogger(TFtpFileSquid.class);

    // FTP 服务器ip
    private String ip;
    // FTP 服务器端口
    private int port;
    // FTP 指定的文件
    private String[] files;
    // 用户名
    private String username;
    // 密码
    private String password;
    // 文件类型
    private String fileType;
    // 数据抽取行 第一条数据记录
    private int firstDataRowNo;
    // 服务器编码
    private EncodingType serverEncoding;
    // 文件编码
    private EncodingType encoding;
    //文件行分割信息
    private FileRecordSeparator fileLineSeparator;//xml web log
    // connection squid id
    private Integer connectionSquidId;
    // 传输协议 1:SFTP 0:FTP
    private Integer protocol;
    // 是否匿名登录 true:匿名登录
    private boolean isAnonymous;
    // 主动,被动
    Integer transfermode;

    public Integer getConnectionSquidId() {
        return connectionSquidId;
    }

    public void setConnectionSquidId(Integer connectionSquidId) {
        this.connectionSquidId = connectionSquidId;
    }

    public TFtpFileSquid() {
        this.setType(TSquidType.FTP_FILE_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException  {
        if (isFinished()) return null;
        // 发送connectionSquid运行成功
        ServerRpcUtil.sendConnectionSquidSuccess(isDebug(), this, connectionSquidId);

        logBeforeStart();
        this.outRDD = ((CustomJavaSparkContext) jsc).ftpSharedFolderFile(ip, port,
                new FtpUtil.FtpConnectionInfo(ip, port, username,
                        password, serverEncoding.toFtpEncoding(),
                        0,isAnonymous, 0, protocol),
                files, username,
                password, fileType,
                RemoteType.FTP, serverEncoding, encoding, fileLineSeparator);
        return this.outRDD;
    }

    private void logBeforeStart() {
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
            FtpUtil.FtpConnectionInfo info = new FtpUtil.FtpConnectionInfo(ip, port, username,
                    password, serverEncoding.toFtpEncoding(),
                    0,isAnonymous, transfermode, protocol);
            try {
                for (String p : files) {
                    boolean flag = false;
                    flag = FtpUtil.deleteFile(info, p);
                    log.info("删除ftp文件[{}],{}", p, (flag?"成功" : "失败"));
                    if(!flag) {
                        EngineLogFactory.logError(this, "删除ftp文件[" + p + "]异常");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("删除ftp文件异常", e);
                EngineLogFactory.logError(this, "删除ftp文件异常", e);
            }
        }
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

    public EncodingType getServerEncoding() {
        return serverEncoding;
    }

    public void setServerEncoding(EncodingType serverEncoding) {
        this.serverEncoding = serverEncoding;
    }

    public Integer getProtocol() {
        return protocol;
    }

    public void setProtocol(Integer protocol) {
        this.protocol = protocol;
    }

    public boolean isAnonymous() {
        return isAnonymous;
    }

    public void setAnonymous(boolean anonymous) {
        isAnonymous = anonymous;
    }

    public Integer getTransfermode() {
        return transfermode;
    }

    public void setTransfermode(Integer transfermode) {
        this.transfermode = transfermode;
    }
}


