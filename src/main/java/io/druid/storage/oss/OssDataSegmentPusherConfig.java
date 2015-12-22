package io.druid.storage.oss;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OssDataSegmentPusherConfig {
    @JsonProperty
    public String bucket = "";

    @JsonProperty
    public String baseKey = "";

    @JsonProperty
    public boolean disableAcl = false;

    public String getBucket() {
        return bucket;
    }

    public String getBaseKey() {
        return baseKey;
    }

    public boolean getDisableAcl() {
        return disableAcl;
    }
}
