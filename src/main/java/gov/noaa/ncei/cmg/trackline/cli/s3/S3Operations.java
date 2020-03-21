package gov.noaa.ncei.cmg.trackline.cli.s3;

import java.nio.file.Path;
import java.util.function.Consumer;

public interface S3Operations {

  void upload(Path source, String targetBucket, String targetKey);

  void download(String sourceBucket, String sourceKey, Path target);

  void copy(String sourceBucket, String sourceKey, String targetBucket, String targetKey);

  void forEachKey(String bucket, String prefix, Consumer<String> transfer);

  void deleteObject(String bucket, String key);
}
