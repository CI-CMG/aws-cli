package gov.noaa.ncei.cmg.trackline.cli.s3;

public interface S3TransferProgress {
  long getTotalBytesToTransfer();
  long getBytesTransferred();
}
