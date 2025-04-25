package edu.colorado.cires.mgg.aws.cli.s3;

import com.amazonaws.services.s3.transfer.TransferProgress;

public class S3TransferProgressWrapper implements S3TransferProgress {

  private final TransferProgress progress;

  public S3TransferProgressWrapper(TransferProgress progress) {
    this.progress = progress;
  }

  @Override
  public long getTotalBytesToTransfer() {
    return progress.getTotalBytesToTransfer();
  }

  @Override
  public long getBytesTransferred() {
    return progress.getBytesTransferred();
  }
}
