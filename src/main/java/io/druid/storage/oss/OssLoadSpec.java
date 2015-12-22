package io.druid.storage.oss;

import java.io.File;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.segment.loading.LoadSpec;
import io.druid.segment.loading.SegmentLoadingException;

/**
 *
 */
@JsonTypeName(OssStorageDruidModule.SCHEME)
public class OssLoadSpec implements LoadSpec
{
    @JsonProperty(OssDataSegmentPuller.BUCKET)
    private final String bucket;

    @JsonProperty(OssDataSegmentPuller.KEY)
    private final String key;

    final OssDataSegmentPuller puller;

    @JsonCreator
    public OssLoadSpec(
            @JacksonInject OssDataSegmentPuller puller,
            @JsonProperty(OssDataSegmentPuller.BUCKET) String bucket,
            @JsonProperty(OssDataSegmentPuller.KEY) String key){
        Preconditions.checkNotNull(bucket);
        this.bucket = bucket;
        this.key = key;
        this.puller = puller;
    }

    @Override
    public LoadSpecResult loadSegment(File outDir) throws SegmentLoadingException {
        return new LoadSpecResult(puller.getSegmentFiles(
                new OssDataSegmentPuller.OssCoords(bucket, key), outDir).size());
    }
}