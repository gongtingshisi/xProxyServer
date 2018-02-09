package com.danikula.videocache;


import android.util.Log;

public class HandyUtil {
    public static void handle(String msg, Exception e) {
//        checkNotNull(e);
        Log.w("HandyUtil", "msg:" + msg);
        if (e != null)
            e.printStackTrace();
    }
}
