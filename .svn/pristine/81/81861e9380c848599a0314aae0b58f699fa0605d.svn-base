package com.eurlanda.datashire.engine.util;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Properties;

/**
 * Created by zhudebin on 2017/2/22.
 */
public class FtpUtil {

    private static Logger logger = LoggerFactory.getLogger(FtpUtil.class);

    public static class FtpConnectionInfo implements Serializable {
        String host;
        Integer port;
        String username;
        String password;
        // ftp 编码
        String encoding;
        // 加密方式 0:不加密
        Integer encryption;
        // true 匿名
        boolean isAnonymous;
        // 主动,被动
        Integer transfermode;
        // 协议 0:ftp, 1:sftp
        Integer protocol;

        public FtpConnectionInfo(String host, Integer port,
                String username, String password,
                String encoding, Integer encryption,
                boolean isAnonymous, Integer transfermode, Integer protocol) {
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.encoding = encoding;
            this.encryption = encryption;
            this.isAnonymous = isAnonymous;
            this.transfermode = transfermode;
            this.protocol = protocol;
        }

        public String getHost() {
            return host;
        }

        public FtpConnectionInfo setHost(String host) {
            this.host = host;
            return this;
        }

        public Integer getPort() {
            return port;
        }

        public FtpConnectionInfo setPort(Integer port) {
            this.port = port;
            return this;
        }

        public String getUsername() {
            return username;
        }

        public FtpConnectionInfo setUsername(String username) {
            this.username = username;
            return this;
        }

        public String getPassword() {
            return password;
        }

        public FtpConnectionInfo setPassword(String password) {
            this.password = password;
            return this;
        }

        public String getEncoding() {
            return encoding;
        }

        public FtpConnectionInfo setEncoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public Integer getEncryption() {
            return encryption;
        }

        public FtpConnectionInfo setEncryption(Integer encryption) {
            this.encryption = encryption;
            return this;
        }

        public boolean isAnonymous() {
            return isAnonymous;
        }

        public FtpConnectionInfo setAnonymous(boolean anonymous) {
            isAnonymous = anonymous;
            return this;
        }

        public Integer getTransfermode() {
            return transfermode;
        }

        public FtpConnectionInfo setTransfermode(Integer transfermode) {
            this.transfermode = transfermode;
            return this;
        }

        public Integer getProtocol() {
            return protocol;
        }

        public FtpConnectionInfo setProtocol(Integer protocol) {
            this.protocol = protocol;
            return this;
        }
    }

    public static void downLoadFile(FtpConnectionInfo info,
            String srcPath, String targetPath) throws Exception {
        if(info.protocol == 0) {
            FTPClient ftpClient = loginFTP(info);
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(targetPath);
                Boolean status = ftpClient.retrieveFile(new String(srcPath.getBytes(info.encoding)
                        , "iso-8859-1"
                ),
                        fos);
                if(!status) {
                    logger.error("下载失败");
                }
            } catch (IOException e) {
                throw new Exception("FTP客户端下载出错");
            } finally {
                try {
                    //断开连接
                    try {
                        ftpClient.logout();
                        ftpClient.disconnect();
                    } catch (Exception ee) {
                        ee.printStackTrace();
                    }
                } catch (Exception e) {
                    logger.debug("关闭异常");
                }
            }
        } else if(info.protocol == 1) {
            ChannelSftp sftp = loginSFTP(info);
            sftp.get(new String(srcPath.getBytes("UTF-8"), info.encoding), targetPath, null, ChannelSftp.OVERWRITE);
        }
    }

    public static boolean deleteFile(FtpConnectionInfo info, String filePath) throws Exception {
        if(info.protocol == 0) {
            FTPClient ftpClient = loginFTP(info);
            try {
                return ftpClient.deleteFile(new String(filePath.getBytes(info.getEncoding()),
                        "iso-8859-1"));
            } catch (Exception e) {
                throw new Exception("删除文件异常");
            } finally {
                try {
                    //断开连接
                    try {
                        ftpClient.logout();
                        ftpClient.disconnect();
                    } catch (Exception ee) {
                        ee.printStackTrace();
                    }
                } catch (Exception e) {
                    logger.debug("关闭异常");
                }
            }
        } else if(info.protocol == 1) {
            ChannelSftp sftp = loginSFTP(info);
            try {
                sftp.rm(new String(filePath.getBytes("UTF-8"), info.encoding));
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("sftp 删除文件异常");
            } finally {
                sftp.disconnect();
            }
        } else {
            throw new RuntimeException("ftp 不支持该协议:" + info.protocol);
        }
    }

    public static FTPClient loginFTP(FtpConnectionInfo info) throws Exception {
        FTPClient ftpClient = new FTPClient();
        try {
            //设置连接的超时时间,架包需要时2.0以上的
            //ftpClient.setConnectTimeout(10000);
            // 连接
            ftpClient.connect(info.host, info.port);
            //设置编码格式
            ftpClient.setControlEncoding(info.encoding);
            //设置文件类型，二进制
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            //设置缓存区大小
            ftpClient.setBufferSize(3072);
            // 返回状态码
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                //不合法时断开连接
                try {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }catch (Exception e) {
                    e.printStackTrace();
                }
                throw new Exception("未连接到FTP，请检查连接信息");
            } else {
                // 登录
                if(info.isAnonymous) {
                    info.username = "anonymous";
                    info.password = System.getProperty("user.name")+"@"+ InetAddress.getLocalHost().getHostName();
                }
                ftpClient.login(info.username, info.password);
                int reply2 = ftpClient.getReplyCode();
                if (230 != reply2) {
                    throw new Exception("账号或者密码错误");
                }
                logger.debug("==登陆成功====");
                //设置连接模式
                //被动模式ftpClient.enterLocalPassiveMode();
                //主动模式ftpClient.enterLocalActiveMode();
            }
        } catch (SocketException e) {
            //断开连接
            try {
                ftpClient.logout();
                ftpClient.disconnect();
            }catch (Exception ee) {
                ee.printStackTrace();
            }
            throw new Exception("FTP的IP地址可能错误，请正确配置");
        } catch (IOException e) {
            //断开连接
            try {
                ftpClient.logout();
                ftpClient.disconnect();
            }catch (Exception ee) {
                ee.printStackTrace();
            }
            throw new Exception("FTP的登录异常,请正确配置 " + e.getMessage());
        }
        return ftpClient;
    }

    public static ChannelSftp loginSFTP(FtpConnectionInfo info) {
        ChannelSftp sftp = null;
        Session session = null;
        try {

            //com.jcraft.jsch.Logger sftpLogger = new SettleLogger();//日志
            //JSch.setLogger(sftpLogger);
            JSch jsch = new JSch();
            if (info.isAnonymous) {
                info.username = "anonymous";
            }
            session = jsch.getSession(info.username, info.host, info.port);// 根据用户名，主机ip，端口获取一个Session对象
            logger.debug("Session created.");
            if (!info.isAnonymous) {
                session.setPassword(info.password);// 设置密码
            }
            Properties sshConfig = new Properties();
            sshConfig.put("StrictHostKeyChecking", "no");
            session.setConfig(sshConfig);// 为Session对象设置properties
            session.setTimeout(10000);// 设置timeout时间
            session.connect();// 通过Session建立链接
            logger.debug("Session connected.");
            logger.debug("Opening Channel.");
            sftp = (ChannelSftp)session.openChannel("sftp");// 打开SFTP通道
            sftp.connect();// 建立SFTP通道的连接
            sftp.setFilenameEncoding(info.encoding);
            logger.debug("Connected to " + info.host + ".");
//            sftp.get(info.srcFile, targetFile, null, ChannelSftp.OVERWRITE);
        } catch (Exception e) {
            logger.error("连接sftp异常", e);
            throw new RuntimeException("sftp连接异常");
        }
        return sftp;
    }
}
