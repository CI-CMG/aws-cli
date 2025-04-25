package edu.colorado.cires.mgg.aws.cli.s3;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(
    name = "s3",
    description = "S3 operations",
    mixinStandardHelpOptions = true,
    subcommands = {
        S3CpCommands.class,
        S3RmCommands.class
    })
public class S3Commands implements Runnable {

  @Spec
  private CommandSpec spec;

  @Override
  public void run() {
    spec.commandLine().usage(System.out);
  }
}
