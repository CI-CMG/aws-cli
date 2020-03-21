package gov.noaa.ncei.cmg.trackline.cli.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class S3OperationsImpl implements S3Operations {

  private final AmazonS3 s3;

  public S3OperationsImpl(AmazonS3 s3) {
    this.s3 = s3;
  }

  @Override
  public void upload(Path source, String targetBucket, String targetKey) {
    System.out.println("Uploading " + source + " to " + "s3://" + targetBucket + "/" + targetKey);
    TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();
    long startTime = System.currentTimeMillis();
    Upload upload = transferManager.upload(targetBucket, targetKey, source.toFile());
    transfer(startTime, upload);
    transferManager.shutdownNow(false);
  }


  private void createParent(Path target) {
    Path parent = target.getParent();
    if (parent != null && !Files.exists(parent)) {
      try {
        Files.createDirectories(parent);
      } catch (IOException e) {
        throw new RuntimeException("Unable to create directory: " + parent.toAbsolutePath().toString());
      }
    }
  }

  @Override
  public void download(String sourceBucket, String sourceKey, Path target) {
    System.out.println("Downloading " + "s3://" + sourceBucket + "/" + sourceKey + " to " + target);
    createParent(target);
    TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();
    long startTime = System.currentTimeMillis();
    Download download = transferManager.download(sourceBucket, sourceKey, target.toFile());
    transfer(startTime, download);
    transferManager.shutdownNow(false);
    System.out.println();
  }


  @Override
  public void copy(
      String sourceBucket,
      String sourceKey,
      String targetBucket,
      String targetKey) {

    System.out.println("Copying " + "s3://" + sourceBucket + "/" + sourceKey + " to " + "s3://" + targetBucket + "/" + targetKey);
    TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();
    long startTime = System.currentTimeMillis();
    Copy copy = transferManager.copy(sourceBucket, sourceKey, targetBucket, targetKey);
    transfer(startTime, copy);
    transferManager.shutdownNow(false);
    System.out.println();
  }

  private static void printProgress(long startTime, long total, long current) {
    long eta = current == 0 ? 0 :
        (total - current) * (System.currentTimeMillis() - startTime) / current;

    String etaHms = current == 0 ? "N/A" :
        String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(eta),
            TimeUnit.MILLISECONDS.toMinutes(eta) % TimeUnit.HOURS.toMinutes(1),
            TimeUnit.MILLISECONDS.toSeconds(eta) % TimeUnit.MINUTES.toSeconds(1));

    StringBuilder string = new StringBuilder(140);
    int percent = (int) (current * 100 / total);
    string
        .append('\r')
//          .append(String.join("", Collections.nCopies(percent == 0 ? 2 : 2 - (int) (Math.log10(percent)), " ")))
        .append(String.format(" %d%% [", percent))
        .append(String.join("", Collections.nCopies(percent, "=")))
        .append('>')
        .append(String.join("", Collections.nCopies(100 - percent, " ")))
        .append(']')
//          .append(String.join("", Collections.nCopies((int) (Math.log10(total)) - (int) (Math.log10(current)), " ")))
        .append(String.format(" %d/%d, ETA: %s", current, total, etaHms));

    System.out.print(string);
  }


  private void transfer(long startTime, Transfer transfer) {
    do {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      TransferProgress progress = transfer.getProgress();
      printProgress(startTime, progress.getTotalBytesToTransfer(), progress.getBytesTransferred());
    } while (transfer.isDone() == false);
    TransferState state = transfer.getState();
    System.out.println(": " + state);
    if (state == TransferState.Failed || state == TransferState.Canceled) {
      throw new RuntimeException("Transfer Failed");
    }
  }

  @Override
  public void forEachKey(String bucket, String prefix, Consumer<String> transfer) {
    ObjectListing objectListing = s3.listObjects(bucket, prefix);
    while (true) {
      Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
      while (objIter.hasNext()) {
        transfer.accept(objIter.next().getKey());
      }
      if (objectListing.isTruncated()) {
        objectListing = s3.listNextBatchOfObjects(objectListing);
      } else {
        break;
      }
    }
  }

  @Override
  public void deleteObject(String bucket, String key) {
    System.out.println("Deleting " + "s3://" + bucket + "/" + key);
    s3.deleteObject(bucket, key);
  }
}
