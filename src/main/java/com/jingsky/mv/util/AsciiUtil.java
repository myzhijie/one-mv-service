package com.jingsky.mv.util;

/**
 * Ascii工具类
 */
public class AsciiUtil {

    /**
     * 字符串中字符Ascii数字和
     * @param str 字符串
     * @return int
     */
    public static int sumStrAscii(String str){
        byte[] byteStr = str.getBytes();
        int sum = 0;
        for(int i=0;i<byteStr.length;i++){
            sum += byteStr[i];
        }
        return sum;
    }
}
