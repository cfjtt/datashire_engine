package com.eurlanda.common;

import com.eurlanda.datashire.engine.util.FtpUtil;
import org.apache.commons.net.ftp.FTPClient;
import org.junit.Test;

/**
 * Created by zhudebin on 2017/2/22.
 */
public class TestFtpUtil {

    @Test
    public void testDownLoadSFTP() throws Exception {
        String srcPath = "/root/data/ftp_data/类型不同.txt";
        String targetPath = "/var/folders/lt/2ctz1m9d5_l6m0xchgxqm0g00000gn/T//36066f6e-448d-4247-b9c6-cca1579ad3ca";
        FtpUtil.FtpConnectionInfo info = new FtpUtil.FtpConnectionInfo("192.168.137.128",
                22, "root", "111111", "GBK", 0, false, 0, 1);
        FtpUtil.downLoadFile(info, srcPath, targetPath);

        /**
        ChannelSftp sftp = FtpUtil.loginSFTP(info);
//        sftp.get(new String(srcPath.getBytes(info.getEncoding()), info.getEncoding()), targetPath, null, ChannelSftp.OVERWRITE);
        sftp.get(new String(srcPath.getBytes("UTF-8"), "GBK"), targetPath, null, ChannelSftp.OVERWRITE);
         */
    }

    @Test
    public void testDownLoadFtp() throws Exception {
        String srcPath = "/TestData/测试数据/抽取之后/标题与起始数据行号 - 副本.txt";
        String targetPath = "/var/folders/lt/2ctz1m9d5_l6m0xchgxqm0g00000gn/T//36066f6e-448d-4247-b9c6-cca1579ad3ca";
        FtpUtil.FtpConnectionInfo info = new FtpUtil.FtpConnectionInfo("192.168.137.1",
                21, "datashire", "eurlanda1", "GBK", 0, false, 0, 0);
        FtpUtil.downLoadFile(info, srcPath, targetPath);

        /**
        FTPClient client = FtpUtil.loginFTP(info);
        FTPFile[] files = client.listFiles("/TestData");
        for(FTPFile f : files) {
            System.out.println(f.getName());
        }
        String status = client.getStatus(new String(srcPath.getBytes("GBK"), "UTF-8"));

        System.out.println(status);
         */
    }

    @Test
    public void testDeleteFtp() throws Exception {
        String srcPath = "/TestData/测试数据/抽取之后/标题与起始数据行号 - 副本.txt";
        String targetPath = "/var/folders/lt/2ctz1m9d5_l6m0xchgxqm0g00000gn/T//36066f6e-448d-4247-b9c6-cca1579ad3ca";
        FtpUtil.FtpConnectionInfo info = new FtpUtil.FtpConnectionInfo("192.168.137.1",
                21, "datashire", "eurlanda1", "GBK", 0, false, 0, 0);
        boolean flag = FtpUtil.deleteFile(info, srcPath);
//        System.out.println(flag);

        FTPClient ftpClient = FtpUtil.loginFTP(info);
        try {
            boolean f = ftpClient.deleteFile(
                    new String(srcPath.getBytes(info.getEncoding()),
                    "iso-8859-1"));
            System.out.println(f);
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
                e.printStackTrace();
            }
        }
    }
}
