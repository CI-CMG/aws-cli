package gov.noaa.ncei.cmg.trackline.cli.s3;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import gov.noaa.ncei.cmg.trackline.cli.AwsCommands;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;


@Command(
    name = "rm",
    description = "S3 delete operations",
    mixinStandardHelpOptions = true)
public class S3RmCommands implements Runnable {

  @Parameters(index = "0", description = "A file path or S3 URL to delete. ex. s3://mybucket/test.txt")
  private String path;

  @Option(names = {"-r", "--recursive"}, description = "Command is  performed  on all files or objects under the specified prefix.")
  private boolean recursive = false;

  @Option(names = {"-i", "--include"}, description = "When recursive, don't exclude files or objects in the command that match the specified pattern. See https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob")
  private String include;

  @Option(names = {"-e", "--exclude"}, description = "When recursive, exclude all files or objects from the command that matches the specified pattern. IMPORTANT: Pay attention to the difference between '**' and '*'. Using '*' could delete more than expected.  a See https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob")
  private String exclude;

  @Override
  public void run() {
    new S3RmCommandsHandler(new S3OperationsImpl(AwsCommands.getS3()),
        path,
        recursive,
        include,
        exclude
    ).run();
  }

  static class S3RmCommandsHandler {

    private final S3Operations s3;
    private final String path;
    private final boolean recursive;
    private final String include;
    private final String exclude;


    S3RmCommandsHandler(S3Operations s3, String path, boolean recursive, String include, String exclude) {
      this.s3 = s3;
      this.path = path;
      this.recursive = recursive;
      this.include = include;
      this.exclude = exclude;
    }

    public void run() {
      if (StringUtils.isBlank(path)) {
        throw new RuntimeException("path is required");
      }

      AmazonS3URI uri = new AmazonS3URI(path.trim());

      String bucket = uri.getBucket();
      String victim = S3Utils.normalize(uri.getKey());

      if (recursive) {
        String prefix = victim.isEmpty() ? victim : victim + "/";
        s3.forEachKey(bucket, prefix, key -> {
          String resolvedPath = prefix.isEmpty() ? key : S3Utils.normalize(key).replaceAll("^" + prefix, "");
          if(incExc(Paths.get(resolvedPath))) {
            s3.deleteObject(bucket, key);
          }

        });
      } else {
        s3.deleteObject(bucket, victim);
      }
    }

    private boolean incExc(Path path) {
      return S3Utils.incExc(path, include, exclude);
    }

  }
}
