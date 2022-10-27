package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.squidFlow.AbstractSquidTest;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Juntao.Zhang on 2014/5/23.
 */
public class ApacheLogFormatTest extends AbstractSquidTest {
    @Test
    public void testApacheLogFormat() {
//        System.out.println(
//                ApacheLogFormat.logInfoHandler(
//                        "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" " +
//                                "200  2326  \"http://www.example.com/start.html\" " +
//                                "\"Mozilla/4.08 [en] (Win98; I ;Nav)\""
//                        , null
//                )
//        );
//        System.out.println(ApacheLogFormat.logInfoHandler("127.0.0.1 " +
//                        "[10/Oct/2000:13:55:36 -0700] " +
//                        "\"GET /apache_pb.gif HTTP/1.0\" 200  -  " +
//                        "\"http://www.example.com/start.html\" " +
//                        "\"Mozilla/4.08 [en] (Win98; I ;Nav)\"  - frank",
//                "%h %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\" %l %u"
//        ));
//        System.out.println(ApacheLogFormat.logInfoHandler(
//                "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200  2326", "%h %l %u %t \"%r\" %>s %B"));
    }

    @Test
    public void test() throws ParseException {
//        t("(\\w+|-)?", " - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" \" + \"200  2326 ");
//        t("(\\w+|-)?", " frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" \" + \"200  2326 ");
        t("[a-zA-Z0-9][-a-zA-Z0-9]{0,62}(.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+.?", "65-37-13-251.nrp2.roc.ny.frontiernet.net - - [09/Mar/2004:17:42:41 -0800] \"GET /twiki/bin/view/Main/WebHome HTTP/1.1\" 200 10419");
        t("[a-zA-Z0-9][-a-zA-Z0-9]{0,62}(.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+.?", "203.147.138.233 - - [09/Mar/2004:15:25:14 -0800] \"GET /dccstats/stats-spam.1day.png HTTP/1.1\" 200 3041");
    }

    private void t(String reg, String str) {
        Pattern pattern = Pattern.compile(reg);
        str=str.trim();
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            System.out.println("result:" + matcher.group());
            System.out.println("new:" + str.substring(matcher.end()));
        }
    }

    /**
     * G 年代标志符
     * y 年
     * M 月
     * d 日
     * h 时 在上午或下午 (1~12)
     * H 时 在一天中 (0~23)
     * m 分
     * s 秒
     * S 毫秒
     * E 星期
     * D 一年中的第几天
     * F 一月中第几个星期几
     * w 一年中第几个星期
     * W 一月中第几个星期
     * a 上午 / 下午 标记符
     * k 时 在一天中 (1~24)
     * K 时 在上午或下午 (0~11)
     * z 时区
     *
     * @throws ParseException
     */
    @Test
    public void testDate() throws ParseException {
        String apache_time = "[13/Aug/2008:00:50:49 -0700]";
        SimpleDateFormat sdf = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]", Locale.US);
        System.out.println(sdf.parse(apache_time));
//        System.out.println(DateUtils.parseDate(apache_time, new String[]{"[dd/MMM/yyyy:HH:mm:ss z]"}));
    }
}
