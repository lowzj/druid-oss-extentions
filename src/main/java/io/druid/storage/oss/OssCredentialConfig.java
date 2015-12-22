package io.druid.storage.oss;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Copyright 2015, Easemob.
 * All rights reserved.
 * Author: zhangjin@easemob.com
 */
public class OssCredentialConfig {
    @JsonProperty
    private String endpoint = "";

    @JsonProperty
    private String accessKeyId = "";

    @JsonProperty
    private String accessKeySecret = "";

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }
}
