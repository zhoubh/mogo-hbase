package cn.creativearts.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * 静态方法类
 *
 * Created by xuchl on 2018/9/29.
 */
public class Java_Utils implements Serializable {

    private static final Logger log = Logger.getLogger(Java_Utils.class);

    /**
     * 初始化Kerberos环境
     */
    public static void initKerberosENV(Configuration conf,String confPath,String principal,String keytabPath) {
        System.setProperty("java.security.krb5.conf", confPath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "true");
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
            System.out.println(UserGroupInformation.getCurrentUser());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 默认参数
     * @param conf
     * @throws IOException
     */
    public static void  initKerberosENV(Configuration conf) throws IOException{
        System.setProperty("java.security.krb5.conf", "D:\\krb5\\krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "true");

        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("xucl@CREATIVEARTS.CN", "D:\\krb5\\xucl.keytab");
            System.out.println(UserGroupInformation.getCurrentUser());
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 去除中间空格、前部+86
     * @param tel
     * @return
     */
    public static String trims(String tel) {
        return  tel.replace(" ", "")
                //特殊ACII码
                //.replace("\\xC2\\xA0", "")
                .replace(" ", "")
                .replace("－", "").replace("-","")
                .replace("＋86","").replace("+86","");
    }

    /**
     * 判断为null时，返回""
     * @param row
     * @param num
     * @return
     */
    public static String getValue(Row row,int num) {
        if(null == row.get(num)){
            return "";
        }else{
            return row.get(num).toString();
        }
    }

    /**
     * 判断为null时，返回""
     * @param row
     * @param name
     * @return
     */
    public static String getValue(Row row,String name) {
        if(null == row.getAs(name)){
            return "";
        }else{
            return row.getAs(name).toString();
        }
    }

    /**
     * 判断产品类型
     * 1：12位userid：nyd
     * 2：14位userid：ymt
     * @param id
     * @return
     * @throws ParseException
     */
    public static String proType(String id) throws ParseException{
        if(id.length() == 12){
            return "1";
        }else if(id.length() == 14){
            return "2";
        }else {
            return "0";
        }
    }

    /**
     * 日期格式由yyyy-MM-dd HH:mm:ss（2017-12-20 16:42:10）转为yyyyMMddHHmmss（20171220164210）
     * 去除前两位20
     * @param datetime
     * @return
     */
    public static String dateTrans(String datetime) throws ParseException{
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
        String str = sdf2.format(sdf1.parse(datetime));

        return str;
        //return str.substring(2,str.length());
    }

    /**
     * 返回主被叫编码（主叫DIAL-0，被叫DIALED-1，其他2）
     * @param dial_type
     * @return
     * @throws ParseException
     */
    public static String dialType(String dial_type) throws ParseException{
        if("DIAL".equals(dial_type)){
            return "0";
        }else if("DIALED".equals(dial_type)){
            return "1";
        }else {
            return "2";
        }
    }

    /**
     * 截取peer_number后4位
     * 如果长度不够，右边加0补齐
     * @param peer_number
     * @return
     * @throws ParseException
     */
    public static String peerTail(String peer_number) throws ParseException{
        if(peer_number.length() >= 4){
            return peer_number.substring(peer_number.length()-4,peer_number.length());
        }else {
            for(int i = peer_number.length();i<4;i++){
                peer_number += "0";
            }
            return peer_number;
        }
    }


    /**
     * 判断被/主叫号码是否合规
     * @param str
     * @return
     */
    public static boolean isNumeric(String str) {
        String reg = "^\\d+$";
        if (str.matches(reg)) {
            return true;
        } else {
            return false;
        }
    }


    /**
     * 日期转Long，返回10位
     * @param strDate
     * @return
     */
    public static Long getLongTime(String strDate) {
        String dateFormat = "yyyy-MM-dd HH:mm:ss.SSS";
        SimpleDateFormat sf = new SimpleDateFormat( dateFormat);
/*        Date date = sf.parse(strDate);
        Long result = date.getTime()/1000;*/

        Date date = null;
        Long result = 0L;

        try {
            date = sf.parse(strDate.substring(0,23));
            result = date.getTime()/1000;
        }
        catch(ParseException e) {
            log.error(date);
            e.printStackTrace();
        }

        return result;
    }


}
