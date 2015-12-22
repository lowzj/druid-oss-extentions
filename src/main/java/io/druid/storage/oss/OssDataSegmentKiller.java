package io.druid.storage.oss;

import java.util.Map;

import com.aliyun.oss.OSSClient;
import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

/**
 * Copyright 2015, Easemob.
 * All rights reserved.
 * Author: zhangjin@easemob.com
 */
public class OssDataSegmentKiller implements DataSegmentKiller {
    private static final Logger log = new Logger(OssDataSegmentKiller.class);

    private final OSSClient ossClient;

    @Inject
    public OssDataSegmentKiller(OSSClient ossClient) {
        this.ossClient = ossClient;
    }

    @Override
    public void kill(DataSegment segment) throws SegmentLoadingException {
        try {
            Map<String, Object> loadSpec = segment.getLoadSpec();
            String ossBucket = MapUtils.getString(loadSpec, "bucket");
            String ossPath = MapUtils.getString(loadSpec, "key");
            String ossDescriptorPath = OssUtils.descriptorPathForSegmentPath(ossPath);

            if (ossClient.doesObjectExist(ossBucket, ossPath)) {
                log.info("Removing index file[oss://%s/%s] from oss!", ossBucket, ossPath);
                ossClient.deleteObject(ossBucket, ossPath);
            }
            if (ossClient.doesObjectExist(ossBucket, ossDescriptorPath)) {
                log.info("Removing descriptor file[oss://%s/%s] from oss!", ossBucket, ossDescriptorPath);
                ossClient.deleteObject(ossBucket, ossDescriptorPath);
            }
        } catch (Exception e) {
            throw new SegmentLoadingException(e, "Couldn't kill segment[%s]: [%s]", segment.getIdentifier(), e);
        }
    }
}
