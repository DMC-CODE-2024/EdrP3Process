package com.glocks.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.glocks.EdrP3Process.conn;

public class Util {

    public static String defaultDate(boolean isOracle) {
        if (isOracle) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String val = sdf.format(new Date());
            String date = "TO_DATE('" + val + "','YYYY-MM-DD HH24:MI:SS')";
            return date;
        } else {
            return "now()";
        }
    }


    public static String defaultDateNow(boolean isOracle) {
        if (isOracle) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String val = sdf.format(new Date());
            String date = " TO_DATE('" + val + "','YYYY-MM-DD HH24:MI:SS') ";  //commented by sharad
            return date;
        } else {
            return " now() ";
        }
    }

    public static String defaultStringtoDate(String stringDate) {
        if (conn.toString().contains("oracle")) {
            return "TO_DATE('" + stringDate + "','YYYY-MM-DD HH24:MI:SS')";
        } else {
            return " '" + stringDate + "' ";
        }
    }


}




