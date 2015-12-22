# druid-oss-extentions
based on druid 0.8.3-SNAPSHOT

## Build into druid

* pull the druid source code

```sh
git clone https://github.com/druid-io/druid.git
cd druid
git checkout 0.8.2
```

* copy to druid project: druid/extentions/
* add `druid-oss-extensions` to `druid/pom.xml`

```
<modules>
  ...
  <module>extensions/druid-oss-extensions</module>
  ...
<modules>
```

* add `aliyun-sdk-oss` into `druid/pom.xml`

```
<dependencyManagement>
  <dependencies>
  <dependency>
    <groupId>com.aliyun.oss</groupId>
    <artifactId>aliyun-sdk-oss</artifactId>
    <version>2.0.6</version>
  </dependency>
  </dependencies>
</dependencyManagement>
```

* add `druid-oss-extensions` to `druid/extensions-distribution/pom.xml`

```
<dependency>
  <groupId>io.druid.extensions</groupId>
  <artifactId>druid-oss-extensions</artifactId>
  <version>${project.parent.version}</version>
  <optional>true</optional>
</dependency>
```

> NOTE: in druid 0.9.0, add the following code into `druid/distribution/pom.xml`

> ```
> <argument>-c</argument>
> <argument>io.druid.extensions:druid-oss-extensions</argument>
> ```

## Configuration

```sh
druid.storage.type=oss                                      # always 'oss'
druid.storage.bucket=zjtesteasemob                          # bucket name
druid.storage.baseKey=test                                  # directory or prefix
druid.oss.endpoint=https://oss-cn-beijing.aliyuncs.com      # Aliyun OSS endpoint
druid.oss.accessKeyId=''                                    # your Aliyun OSS accessKeyId
druid.oss.accessKeySecret=''                                # your Aliyun OSS accessKeySecret
```

