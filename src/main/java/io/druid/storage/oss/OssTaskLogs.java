package io.druid.storage.oss;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import io.druid.tasklogs.TaskLogs;

/**
 * Copyright 2015, Easemob.
 * All rights reserved.
 * Author: zhangjin@easemob.com
 */
public class OssTaskLogs implements TaskLogs {
    @Override
    public void pushTaskLog(String taskId, File logFile) throws IOException {

    }

    @Override
    public Optional<ByteSource> streamTaskLog(String taskId, long offset) throws IOException {
        return null;
    }
}
