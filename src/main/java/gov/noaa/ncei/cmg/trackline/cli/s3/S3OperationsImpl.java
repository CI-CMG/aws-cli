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
import java.util.Iterator;
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

  // prints a simple text progressbar: [#####     ]
  public static void printProgressBar(double pct) {
    // if bar_size changes, then change erase_bar (in eraseProgressBar) to
    // match.
    final int bar_size = 40;
    final String empty_bar = "                                        ";
    final String filled_bar = "########################################";
    int amt_full = (int) (bar_size * (pct / 100.0));
    System.out.format("  [%s%s]", filled_bar.substring(0, amt_full),
        empty_bar.substring(0, bar_size - amt_full));
  }

  // erases the progress bar.
  public static void eraseProgressBar() {
    // erase_bar is bar_size (from printProgressBar) + 4 chars.
    final String erase_bar = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";
    System.out.format(erase_bar);
  }

  private void transfer(long startTime, Transfer transfer) {
    printProgressBar(0.0);
    do {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      TransferProgress progress = transfer.getProgress();
      double pct = progress.getPercentTransferred();
      eraseProgressBar();
      printProgressBar(pct);
    } while (transfer.isDone() == false);
    TransferState state = transfer.getState();
    System.out.println(": " + state);
    if (state == TransferState.Failed || state == TransferState.Canceled) {
      try {
        transfer.waitForCompletion();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Transfer Failed", e);
      }
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
