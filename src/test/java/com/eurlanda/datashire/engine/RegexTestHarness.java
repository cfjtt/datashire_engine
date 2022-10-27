package com.eurlanda.datashire.engine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexTestHarness {

    private static Log log = LogFactory.getLog(RegexTestHarness.class);

    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("[a-yA-Y]");
        Matcher matcher = pattern.matcher("AB");
        boolean found = false;
        while (matcher.find()) {
            System.out.printf("I found the text \"%s\" starting at index %d " +
                            "and ending at index %d.%n",
                    matcher.group(), matcher.start(), matcher.end()
            );
            found = true;
        }
        System.out.println("zd dsf -Zd".matches("[b-zB-Z\\-\" \"]*"));
        System.out.println("ad dsf z".matches("[b-zB-Z\\-\" \"]*"));
        if (!found) {
            System.out.printf("No match found.%n");
        }
    }

    @Test
    public void test1() {
        StringReader sr = new StringReader("ssss1\r\nssssss2\r\nssss3");
        BufferedReader br = new BufferedReader(sr);
        try {
            String str = null;
            while((str=br.readLine()) != null) {
                System.out.println(str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test2() {
        String path = System.getProperty("java.io.tmpdir");
        System.out.println(path);

        File file = new File(path + "hello.txt");
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println(file.getAbsoluteFile());
        file.deleteOnExit();

    }

    @Test
    public void test3() {
//        String rep = "([^@a-zA-Z0-9]+)?(@[a-zA-Z]+[a-zA-Z0-9]*)([^a-zA-Z0-9]+)?";
        String rep = "([^@'a-zA-Z0-9]+)(@[a-zA-Z]+[a-zA-Z0-9]*)";

//        String str = "";
        String str = "a>'@@c' and c<@b or @c<10 or 'dd@d22dd'+1<2 or 'dd@d22dd+1<2 or @d22dd asfa '+1<2";
//        String str = "a>'@@c' and c<@b or @c<10 or 'dd@d22dd'+1<2 or 'dd@d22dd+1<2 or @d22dd asfa '+1<2  and ' a''@d''bc'>e";
        Matcher matcher = Pattern.compile(rep).matcher(str);
        while (matcher.find()) {
            System.out.printf("I found the text \"%s\" starting at index %d " +
                            "and ending at index %d.%n",
                    matcher.group(), matcher.start(), matcher.end()
            );
            log.info(matcher.group(1));
            log.info(matcher.group(2));
//            log.info(matcher.group(3));
        }
    }
}
