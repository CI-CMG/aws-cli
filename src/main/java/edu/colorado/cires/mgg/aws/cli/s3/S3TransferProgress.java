package edu.colorado.cires.mgg.aws.cli.s3;

public interface S3TransferProgress {
  long getTotalBytesToTransfer();
  long getBytesTransferred();
}
