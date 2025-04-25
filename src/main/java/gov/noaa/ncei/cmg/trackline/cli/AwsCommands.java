package gov.noaa.ncei.cmg.trackline.cli;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import gov.noaa.ncei.cmg.trackline.cli.s3.S3Commands;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(
    name = "aws-cli",
    description = "Executes commands using the AWS API",
    mixinStandardHelpOptions = true,
    subcommands = {
        S3Commands.class
    })
public class AwsCommands implements Runnable {

  private static AmazonS3 s3 = null;

  @Spec
  private CommandSpec spec;

  public static void setS3(AmazonS3 override) {
    s3 = override;
  }

  public static AmazonS3 getS3() {
    return s3 == null ? AmazonS3ClientBuilder.defaultClient() : s3;
  }

  static int runWithoutExit(String[] args) {
    return new CommandLine(new AwsCommands()).execute(args);
  }

  public static void main(String[] args) {
    System.exit(runWithoutExit(args));
  }

  public void run() {
    spec.commandLine().usage(System.out);
  }
}
