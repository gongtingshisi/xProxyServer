package com.danikula.videocache.file;

import java.io.File;
import java.io.IOException;

/**
 * Unlimited version of {@link DiskUsage}.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
public class UnlimitedDiskUsage implements DiskUsage {

    @Override
    public void touch(File file) throws IOException {
        // do nothing
    }
}
