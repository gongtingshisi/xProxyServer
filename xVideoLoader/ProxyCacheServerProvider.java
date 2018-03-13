package com.cootek.smallvideo.main;


import android.content.Context;

import com.danikula.videocache.HttpProxyCacheServer;
import com.danikula.videocache.IContentTypeProvider;

public class ProxyCacheServerProvider {
    private static ProxyCacheServerProvider instance;
    private static HttpProxyCacheServer mProxy;

    private ProxyCacheServerProvider() {
    }

    private static ProxyCacheServerProvider getInstance(Context context) {
        if (instance == null) {
            synchronized (ProxyCacheServerProvider.class) {
                if (instance == null) {
                    instance = new ProxyCacheServerProvider();
                    mProxy = new HttpProxyCacheServer.Builder(context).mimeProvider(new IContentTypeProvider() {
                        @Override
                        public String getMIME() {
                            return HttpProxyCacheServer.CONTENT_TYPE_VIDEO_MP4;
                        }
                    }).build();
                }
            }
        }
        return instance;
    }

    public static HttpProxyCacheServer getProxy(Context context) {
        getInstance(context);
        return mProxy;
    }

    /**
     * terminate preload service.
     */
    public static void close() {
        if (mProxy != null)
            mProxy.shutdown();
        instance = null;
        mProxy = null;
    }
}
