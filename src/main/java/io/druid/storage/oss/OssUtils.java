package io.druid.storage.oss;

import java.net.URI;

import com.google.common.base.Joiner;
import com.metamx.common.IAE;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;

/**
 * Copyright 2015, Easemob.
 * All rights reserved.
 * Author: zhangjin@easemob.com
 */
public class OssUtils {
    private static final Joiner JOINER = Joiner.on("/").skipNulls();

    public static URI checkURI(URI uri) {
        String scheme = OssStorageDruidModule.SCHEME;
        if (scheme.equalsIgnoreCase(uri.getScheme())) {
            uri = URI.create(scheme + uri.toString().substring(scheme.length()));
        } else if (!scheme.equalsIgnoreCase(uri.getScheme())) {
            throw new IAE("Don't know how to load scheme for URI [%s]", uri.toString());
        }
        return uri;
    }

    public static URI genURI(OssDataSegmentPuller.OssCoords ossCoords) {
        return URI.create(String.format("%s://%s/%s",
                OssStorageDruidModule.SCHEME,
                ossCoords.bucket,
                ossCoords.path));
    }

    public static String genSegmentPath(String baseKey, DataSegment dataSegment) {
        return JOINER.join(
                baseKey.isEmpty() ? null : baseKey,
                DataSegmentPusherUtil.getStorageDir(dataSegment)) + "/index.zip";
    }

    public static String descriptorPathForSegmentPath(String ossPath) {
        return ossPath.substring(0, ossPath.lastIndexOf("/")) + "/descriptor.json";
    }
}
