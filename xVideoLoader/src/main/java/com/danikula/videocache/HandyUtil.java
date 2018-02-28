package com.danikula.videocache;


import android.util.Log;
/**
 * @author zhangfeng
 * @license: Apache License 2.0
 * */
public class HandyUtil {
    public static void handle(String msg, Exception e) {
//        checkNotNull(e);
        Log.w("HandyUtil", "msg:" + msg);
        if (e != null)
            e.printStackTrace();
    }
}
