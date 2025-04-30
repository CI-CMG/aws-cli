package edu.colorado.cires.mgg.aws.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Testcontainers(disabledWithoutDocker = true)
public class AwsCommandsTest {


  @Container
  private static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.5.0")).withServices(S3);


  private static S3Client s3;

  @BeforeAll
  public static void setupAll() {
    AwsCommands.setS3(AmazonS3ClientBuilder
        .standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(
                localstack.getEndpoint().toString(),
                localstack.getRegion()
            )
        )
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey())
            )
        )
        .build());

    s3 = S3Client
        .builder()
        .endpointOverride(localstack.getEndpoint())
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
            )
        )
        .region(Region.of(localstack.getRegion()))
        .build();
  }


  @Test
  public void testRecursiveDownloadS3ToLocal() throws Exception {

    String bucket = "test-recursive-download-s3-to-local";
    Path localPath = Paths.get("target").resolve(bucket);
    FileUtils.deleteQuietly(localPath.toFile());
    Files.createDirectories(localPath);

    try {
      s3.createBucket(b -> b.bucket(bucket));

      for (int i = 0; i < 10; i++) {
        final int level = i;
        String base = "root/" + level + "/";
        s3.putObject(b -> b.bucket(bucket).key(base + level + ".json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));
        for (int j = 0; j < 10; j++) {
          int level2 = j;
          s3.putObject(b -> b.bucket(bucket).key(base + level2 + "/" + level2 + ".json"),
              RequestBody.fromString("{}", StandardCharsets.UTF_8));
        }
      }

      s3.putObject(b -> b.bucket(bucket).key("root.json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));
      s3.putObject(b -> b.bucket(bucket).key("root/root.json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));
      s3.putObject(b -> b.bucket(bucket).key("root/3/"), RequestBody.fromString("{}", StandardCharsets.UTF_8));

      assertEquals(0, AwsCommands.runWithoutExit(new String[]{
          "s3",
          "cp",
          "-r",
          "s3://" + bucket + "/root/3/",
          localPath.resolve("3").toString()
      }));

      Set<String> paths;
      try (Stream<Path> stream = Files.walk(localPath)) {
        paths = new TreeSet<>(stream.filter(Files::isRegularFile).map(Path::toString).collect(Collectors.toSet()));
      }

      Set<String> expected = new TreeSet<>();
      for (int j = 0; j < 10; j++) {
        expected.add(localPath.resolve("3").resolve("" + j).resolve(j + ".json").toString());
      }
      expected.add(localPath.resolve("3").resolve("3.json").toString());

      assertEquals(expected, paths);
    } finally {
      FileUtils.deleteQuietly(localPath.toFile());
    }
  }

  @Test
  public void testRecursiveUploadLocalToS3() throws Exception {

    String bucket = "test-recursive-upload-local-to-s3";
    Path localPath = Paths.get("src/test/resources/testlocal/3");
    Files.createDirectories(localPath);

    s3.createBucket(b -> b.bucket(bucket));

    s3.putObject(b -> b.bucket(bucket).key("root.json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));
    s3.putObject(b -> b.bucket(bucket).key("root/root.json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));
    s3.putObject(b -> b.bucket(bucket).key("root/0/0.json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));

    assertEquals(0, AwsCommands.runWithoutExit(new String[]{
        "s3",
        "cp",
        "-r",
        localPath.toString(),
        "s3://" + bucket + "/root/3/",
    }));

    Set<String> expected = new TreeSet<>();
    expected.add("root.json");
    expected.add("root/root.json");
    expected.add("root/0/0.json");
    expected.add("root/3/0/0.json");
    expected.add("root/3/1/1.json");
    expected.add("root/3/2/2.json");
    expected.add("root/3/3/3.json");
    expected.add("root/3/4/4.json");
    expected.add("root/3/5/5.json");
    expected.add("root/3/6/6.json");
    expected.add("root/3/7/7.json");
    expected.add("root/3/8/8.json");
    expected.add("root/3/9/9.json");
    expected.add("root/3/3.json");

    Set<String> actual = new TreeSet<>();
    s3.listObjectsV2(b -> b.bucket(bucket)).contents().forEach(obj -> {
      actual.add(obj.key());
    });

    assertEquals(expected, actual);

  }

  @Test
  public void testRecursiveCopyS3ToS3() throws Exception {

    String sourceBucket = "test-recursive-copy-source";
    String destBucket = "test-recursive-copy-dest";

    s3.createBucket(b -> b.bucket(destBucket));
    s3.createBucket(b -> b.bucket(sourceBucket));

    s3.putObject(b -> b.bucket(destBucket).key("root2.json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));
    s3.putObject(b -> b.bucket(destBucket).key("root2/root2.json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));
    s3.putObject(b -> b.bucket(destBucket).key("root2/10/10.json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));

    for (int i = 0; i < 10; i++) {
      final int level = i;
      String base = "root/" + level + "/";
      s3.putObject(b -> b.bucket(sourceBucket).key(base + level + ".json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));
      for (int j = 0; j < 10; j++) {
        int level2 = j;
        s3.putObject(b -> b.bucket(sourceBucket).key(base + level2 + "/" + level2 + ".json"),
            RequestBody.fromString("{}", StandardCharsets.UTF_8));
      }
    }

    s3.putObject(b -> b.bucket(sourceBucket).key("root.json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));
    s3.putObject(b -> b.bucket(sourceBucket).key("root/root.json"), RequestBody.fromString("{}", StandardCharsets.UTF_8));

    assertEquals(0, AwsCommands.runWithoutExit(new String[]{
        "s3",
        "cp",
        "-r",
        "s3://" + sourceBucket + "/root/3/",
        "s3://" + destBucket + "/root2/3/",
    }));

    Set<String> expected = new TreeSet<>();
    expected.add("root2.json");
    expected.add("root2/root2.json");
    expected.add("root2/10/10.json");
    expected.add("root2/3/0/0.json");
    expected.add("root2/3/1/1.json");
    expected.add("root2/3/2/2.json");
    expected.add("root2/3/3/3.json");
    expected.add("root2/3/4/4.json");
    expected.add("root2/3/5/5.json");
    expected.add("root2/3/6/6.json");
    expected.add("root2/3/7/7.json");
    expected.add("root2/3/8/8.json");
    expected.add("root2/3/9/9.json");
    expected.add("root2/3/3.json");

    Set<String> actual = new TreeSet<>();
    s3.listObjectsV2(b -> b.bucket(destBucket)).contents().forEach(obj -> {
      actual.add(obj.key());
    });

    assertEquals(expected, actual);
  }

}