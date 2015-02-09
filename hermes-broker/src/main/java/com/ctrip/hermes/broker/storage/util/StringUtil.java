package com.ctrip.hermes.broker.storage.util;

public class StringUtil {

    public static int safeToInt(String s, int defaultValue) {
        int result = defaultValue;

        if (s != null) {
            try {
                result = Integer.parseInt(s);
            } catch (Exception e) {
                // ignore
            }
        }

        return result;
    }

}
