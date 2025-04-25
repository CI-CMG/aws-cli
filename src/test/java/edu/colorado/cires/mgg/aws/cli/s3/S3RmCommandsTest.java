package edu.colorado.cires.mgg.aws.cli.s3;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import edu.colorado.cires.mgg.aws.cli.s3.S3RmCommands.S3RmCommandsHandler;
import java.util.Arrays;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class S3RmCommandsTest {

  @Test
  public void testDelete() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String path = "s3://foo-bucket/bucket-file.txt";
    boolean recursive = false;
    String include = null;
    String exclude = null;
    S3RmCommandsHandler handler = new S3RmCommandsHandler(s3, path, recursive, include, exclude);
    handler.run();
    verify(s3).deleteObject(eq("foo-bucket"), eq("bucket-file.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  public void testDeleteRecursive() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String path = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = null;
    String exclude = null;
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "cats/dir/file1.txt",
          "cats/dir/file2.txt",
          "cats/file3.txt"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3RmCommandsHandler handler = new S3RmCommandsHandler(s3, path, recursive, include, exclude);
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/dir/file1.txt"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/dir/file2.txt"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/file3.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  public void testDeleteRecursive2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String path = "s3://foo-bucket/cats/";
    boolean recursive = true;
    String include = null;
    String exclude = null;
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "cats/dir/file1.txt",
          "cats/dir/file2.txt",
          "cats/file3.txt"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3RmCommandsHandler handler = new S3RmCommandsHandler(s3, path, recursive, include, exclude);
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/dir/file1.txt"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/dir/file2.txt"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/file3.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  public void testDeleteRecursiveRoot() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String path = "s3://foo-bucket";
    boolean recursive = true;
    String include = null;
    String exclude = null;
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq(""), any(Consumer.class));

    S3RmCommandsHandler handler = new S3RmCommandsHandler(s3, path, recursive, include, exclude);
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq(""), any(Consumer.class));
    verify(s3).deleteObject(eq("foo-bucket"), eq("dir/file1.txt"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("dir/file2.txt"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("file3.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  public void testDeleteRecursiveRoot2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String path = "s3://foo-bucket/";
    boolean recursive = true;
    String include = null;
    String exclude = null;
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq(""), any(Consumer.class));

    S3RmCommandsHandler handler = new S3RmCommandsHandler(s3, path, recursive, include, exclude);
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq(""), any(Consumer.class));
    verify(s3).deleteObject(eq("foo-bucket"), eq("dir/file1.txt"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("dir/file2.txt"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("file3.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  public void testDeleteInclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String path = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = "*.zip";
    String exclude = null;
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "cats/dir/file1.txt",
          "cats/dir/file2.txt",
          "cats/file3.txt",
          "cats/dir/file4.zip",
          "cats/file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3RmCommandsHandler handler = new S3RmCommandsHandler(s3, path, recursive, include, exclude);
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  public void testDeleteInclude2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String path = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = "**.zip";
    String exclude = null;
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "cats/dir/file1.txt",
          "cats/dir/file2.txt",
          "cats/file3.txt",
          "cats/dir/file4.zip",
          "cats/file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3RmCommandsHandler handler = new S3RmCommandsHandler(s3, path, recursive, include, exclude);
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/dir/file4.zip"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  public void testDeleteExclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String path = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = null;
    String exclude = "*.txt";
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "cats/dir/file1.txt",
          "cats/dir/file2.txt",
          "cats/file3.txt",
          "cats/dir/file4.zip",
          "cats/file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3RmCommandsHandler handler = new S3RmCommandsHandler(s3, path, recursive, include, exclude);
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/dir/file1.txt"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/dir/file2.txt"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/dir/file4.zip"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  public void testDeleteExclude2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String path = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = null;
    String exclude = "**.txt";
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "cats/dir/file1.txt",
          "cats/dir/file2.txt",
          "cats/file3.txt",
          "cats/dir/file4.zip",
          "cats/file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3RmCommandsHandler handler = new S3RmCommandsHandler(s3, path, recursive, include, exclude);
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/dir/file4.zip"));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  public void testDeleteIncludeExclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String path = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = "*.zip";
    String exclude = "*cats*";
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "cats/dir/file1.txt",
          "cats/dir/file2.txt",
          "cats/file3.txt",
          "cats/dir/file4.zip",
          "cats/file5.zip",
          "cats/cats.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3RmCommandsHandler handler = new S3RmCommandsHandler(s3, path, recursive, include, exclude);
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).deleteObject(eq("foo-bucket"), eq("cats/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

}