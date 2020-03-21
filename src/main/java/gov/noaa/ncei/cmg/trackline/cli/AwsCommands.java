package gov.noaa.ncei.cmg.trackline.cli;

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

  @Spec
  private CommandSpec spec;


  public static void main(String[] args) {
    System.exit(new CommandLine(new AwsCommands()).execute(args));
  }

  public void run() {
    spec.commandLine().usage(System.out);
  }
}
