package gov.noaa.ncei.cmg.trackline.cli.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import gov.noaa.ncei.cmg.trackline.cli.AwsCommands;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;


@Command(
    name = "cp",
    description = "S3 copy operations",
    mixinStandardHelpOptions = true)
public class S3CpCommands implements Runnable {

  @Parameters(index = "0", description = "A file path or S3 URL to copy from ex. s3://mybucket/test.txt or mydir/test2.txt")
  private String source;

  @Parameters(index = "1", description = "A file path or S3 URL to copy to ex. s3://mybucket/test.txt or mydir/test2.txt")
  private String target;

  @Option(names = {"-r", "--recursive"}, description = "Command is  performed  on all files or objects under the specified directory or prefix.")
  private boolean recursive = false;

  @Option(names = {"-i", "--include"}, description = "When recursive, don't exclude files or objects in the command that match the specified pattern. See https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob")
  private String include;

  @Option(names = {"-e", "--exclude"}, description = "When recursive, exclude all files or objects from the command that matches the specified pattern. See https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob")
  private String exclude;

  @Override
  public void run() {
    new S3CpCommandsHandler(new S3OperationsImpl(AwsCommands.getS3()),
        source,
        target,
        recursive,
        include,
        exclude,
        path -> {
          try {
            return Files.walk(path).filter(f -> !Files.isDirectory(f));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    ).run();
  }

  static class S3CpCommandsHandler {

    private final S3Operations s3;
    private final String source;
    private final String target;
    private final boolean recursive;
    private final String include;
    private final String exclude;
    private final Function<Path, Stream<Path>> listPaths;

    S3CpCommandsHandler(S3Operations s3, String source, String target, boolean recursive, String include, String exclude, Function<Path, Stream<Path>> listPaths) {
      this.s3 = s3;
      this.source = source;
      this.target = target;
      this.recursive = recursive;
      this.include = include;
      this.exclude = exclude;
      this.listPaths = listPaths;
    }

    public void run() {
      if (StringUtils.isBlank(source)) {
        throw new RuntimeException("source is required");
      }

      if (StringUtils.isBlank(target)) {
        throw new RuntimeException("target is required");
      }

      String s = source.trim();
      String t = target.trim();

      if (!isS3Uri(s) && isS3Uri(t)) {
        upload(s, t);
      } else if (isS3Uri(s) && !isS3Uri(t)) {
        download(s, t);
      } else if (isS3Uri(s) && isS3Uri(t)) {
        copy(s, t);
      } else {
        throw new RuntimeException("source or target must be a S3 URI");
      }

    }


    private boolean incExc(Path path) {
      return S3Utils.incExc(path, include, exclude);
    }

    private void copy(String s, String t) {
      AmazonS3URI sUri = new AmazonS3URI(s);
      AmazonS3URI tUri = new AmazonS3URI(t);

      String sourceBucket = sUri.getBucket();
      String sourceKey = S3Utils.normalize(sUri.getKey());

      String targetBucket = tUri.getBucket();
      String targetKey = S3Utils.normalize(tUri.getKey());

      if (recursive) {
        String sourcePrefix = sourceKey.isEmpty() ? sourceKey : sourceKey + "/";
        String targetPrefix = targetKey.isEmpty() ? targetKey : targetKey + "/";
        s3.forEachKey(sourceBucket, sourcePrefix, key -> {
          String relativePath = sourcePrefix.isEmpty() ? key : S3Utils.normalize(key).replaceAll("^" + sourcePrefix, "");
          String targetPath = targetPrefix.isEmpty() ? relativePath : targetPrefix + relativePath;
          if(incExc(Paths.get(relativePath))) {
            s3.copy(sourceBucket, key, targetBucket, targetPath);
          }

        });
      } else {
        s3.copy(sourceBucket, sourceKey, targetBucket, targetKey);
      }

    }

    private void download(String s, String t) {
      AmazonS3URI s3Uri = new AmazonS3URI(s);
      String sourceBucket = s3Uri.getBucket();
      String sourceKey = S3Utils.normalize(s3Uri.getKey());
      Path dest = Paths.get(t);

      if (recursive) {
        String prefix = sourceKey.isEmpty() ? sourceKey : sourceKey + "/";
        s3.forEachKey(sourceBucket, prefix, key -> {
          String resolvedPath = prefix.isEmpty() ? key : S3Utils.normalize(key).replaceAll("^" + prefix, "");
          if(incExc(Paths.get(resolvedPath))) {
            Path destFile = dest.resolve(resolvedPath);
            s3.download(sourceBucket, key, destFile);
          }

        });
      } else {
        s3.download(sourceBucket, sourceKey, dest);
      }


    }

    private void upload(String s, String t) {
      AmazonS3URI s3Uri = new AmazonS3URI(t);
      String targetBucket = s3Uri.getBucket();
      String targetKey = S3Utils.normalize(s3Uri.getKey());
      Path source = Paths.get(s);

      if (recursive) {
        try (Stream<Path> paths = listPaths.apply(source)) {
          paths.forEach(path -> {

            Path tail = source.relativize(path);

            if(incExc(tail)) {
              String tk = targetKey.isEmpty() ? tail.toString() : targetKey + "/" + tail.toString();
              s3.upload(path, targetBucket, tk);
            }

          });
        }
      } else {
        s3.upload(source, targetBucket, targetKey);
      }
    }


    private boolean isS3Uri(String value) {
      return value.startsWith("s3://");
    }

  }
}
