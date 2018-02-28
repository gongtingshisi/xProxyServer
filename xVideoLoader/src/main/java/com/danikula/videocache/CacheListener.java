package com.danikula.videocache;

import java.io.File;

/**
 * Listener for cache availability.
 *
 * @author Egor Makovsky (yahor.makouski@gmail.com)
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
public interface CacheListener {

    /**
     * title can be null
     */
    void onCacheAvailable(String title, File cacheFile, String url, int percentsAvailable);
}
