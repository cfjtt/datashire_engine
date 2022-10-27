package com.eurlanda.datashire.engine.util;

import org.apache.commons.io.FileUtils;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.io.File;
import java.io.IOException;

/**
 * Created by zhudebin on 14-10-30.
 */
public class EmailSenderUtil {

    private static JavaMailSender mailSender = ConstantUtil.getJavaMailSender();

    /**
     * 同步发送
     * @throws IOException
     * @see
     */
    public static void sendMailBySynchronizationMode(String from, String[] to, String bcc, String[] cc,
                                              String replyTo, String subject, String text,
                                              boolean isHtml, File[] attachmentFiles)
            throws MessagingException, IOException {
        sendMailBySynchronizationMode(new Mail(from, to, bcc, cc, replyTo, subject, text, isHtml, attachmentFiles));
    }

    /**
     * 同步发送
     * @throws IOException
     * @see
     */
    public static void sendMailBySynchronizationMode(Mail mail)
            throws MessagingException, IOException {
        MimeMessage mime = mailSender.createMimeMessage();
        MimeMessageHelper helper = new MimeMessageHelper(mime, true, "utf-8");
        if(org.apache.commons.lang.StringUtils.isNotEmpty(mail.getFrom())) {
            helper.setFrom(mail.getFrom());//发件人
        } else {
            helper.setFrom(ConfigurationUtil.getProperty("MAIL_USERNAME"));
        }
        helper.setTo(mail.getTo());//收件人
        if(org.apache.commons.lang.StringUtils.isNotEmpty(mail.getBcc())) {
            helper.setBcc(mail.getBcc());//暗送
        }
        if(mail.getCc() != null) {
            helper.setCc(mail.getCc());//抄送
        }
        if(org.apache.commons.lang.StringUtils.isNotEmpty(mail.getReplyTo())) {
            helper.setReplyTo(mail.getReplyTo());//回复到
        }
        helper.setSubject(mail.getSubject());//邮件主题
        helper.setText(mail.getText(), mail.isHtml());//true表示设定html格式

        //内嵌资源，这种功能很少用，因为大部分资源都在网上，只需在邮件正文中给个URL就足够了.
        //helper.addInline("logo", new ClassPathResource("logo.gif"));

        //处理附件
        if(mail.getAttachmentFiles() != null && mail.getAttachmentFiles().length>0) {
            for(File file : mail.getAttachmentFiles()) {
                helper.addAttachment(file.getName(), new ByteArrayResource(FileUtils.readFileToByteArray(file)));
            }
        }

//        helper.addAttachment(fileName, new ByteArrayResource(file.getBytes()));
        mailSender.send(mime);
    }

    public static class Mail {
        // 发件人
        private String from;
        // 收件人
        private String[] to;
        // 暗送
        private String bcc;
        // 抄送
        private String[] cc;
        // 回复地址
        private String replyTo;
        // 邮件主题
        private String subject;
        // 邮件内容
        private String text;
        // true 表示设定为html格式
        private boolean isHtml;
        private File[] attachmentFiles;

        public Mail() {
        }

        public Mail(String[] to, String[] cc, String subject, String text, boolean isHtml) {
            this.to = to;
            this.cc = cc;
            this.subject = subject;
            this.text = text;
            this.isHtml = isHtml;
        }

        public Mail(String from, String[] to, String bcc, String[] cc, String replyTo, String subject, String text, boolean isHtml, File[] attachmentFiles) {
            this.from = from;
            this.to = to;
            this.bcc = bcc;
            this.cc = cc;
            this.replyTo = replyTo;
            this.subject = subject;
            this.text = text;
            this.isHtml = isHtml;
            this.attachmentFiles = attachmentFiles;
        }

        public Mail(String from, String[] to, String bcc, String[] cc, String replyTo, String subject, String text, boolean isHtml) {
            this.from = from;
            this.to = to;
            this.bcc = bcc;
            this.cc = cc;
            this.replyTo = replyTo;
            this.subject = subject;
            this.text = text;
            this.isHtml = isHtml;
        }

        public String getFrom() {
            return from;
        }

        public void setFrom(String from) {
            this.from = from;
        }

        public String[] getTo() {
            return to;
        }

        public void setTo(String[] to) {
            this.to = to;
        }

        public String getBcc() {
            return bcc;
        }

        public void setBcc(String bcc) {
            this.bcc = bcc;
        }

        public String[] getCc() {
            return cc;
        }

        public void setCc(String[] cc) {
            this.cc = cc;
        }

        public String getReplyTo() {
            return replyTo;
        }

        public void setReplyTo(String replyTo) {
            this.replyTo = replyTo;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public boolean isHtml() {
            return isHtml;
        }

        public void setHtml(boolean isHtml) {
            this.isHtml = isHtml;
        }

        public File[] getAttachmentFiles() {
            return attachmentFiles;
        }

        public void setAttachmentFiles(File[] attachmentFiles) {
            this.attachmentFiles = attachmentFiles;
        }
    }
}
