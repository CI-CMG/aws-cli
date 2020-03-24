package gov.noaa.ncei.cmg.trackline.cli.s3;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import gov.noaa.ncei.cmg.trackline.cli.s3.S3CpCommands.S3CpCommandsHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class S3CpCommandsTest {


  @Test
  void testUpload() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String source = "foo/bar/file.txt";
    String target = "s3://foo-bucket/bucket-file.txt";
    boolean recursive = false;
    String include = null;
    String exclude = null;
    S3CpCommandsHandler handler = new S3CpCommandsHandler(
        s3,
        source,
        target,
        recursive,
        include,
        exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).upload(eq(Paths.get(source)), eq("foo-bucket"), eq("bucket-file.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testUploadRecursive() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String source = "foo/bar";
    String target = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = null;
    String exclude = null;
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude, p -> Arrays.asList(
        p.resolve("dir/file1.txt"),
        p.resolve("dir/file2.txt"),
        p.resolve("file3.txt")
    ).stream());
    handler.run();
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file1.txt")), eq("foo-bucket"), eq("cats/dir/file1.txt"));
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file2.txt")), eq("foo-bucket"), eq("cats/dir/file2.txt"));
    verify(s3).upload(eq(Paths.get(source).resolve("file3.txt")), eq("foo-bucket"), eq("cats/file3.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testUploadRecursiveRoot() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String source = "/";
    String target = "s3://foo-bucket";
    boolean recursive = true;
    String include = null;
    String exclude = null;
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude, p -> Arrays.asList(
        p.resolve("dir/file1.txt"),
        p.resolve("dir/file2.txt"),
        p.resolve("file3.txt")
    ).stream());
    handler.run();
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file1.txt")), eq("foo-bucket"), eq("dir/file1.txt"));
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file2.txt")), eq("foo-bucket"), eq("dir/file2.txt"));
    verify(s3).upload(eq(Paths.get(source).resolve("file3.txt")), eq("foo-bucket"), eq("file3.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testUploadRecursiveRoot2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String source = "foo/bar/";
    String target = "s3://foo-bucket/";
    boolean recursive = true;
    String include = null;
    String exclude = null;
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude, p -> Arrays.asList(
        p.resolve("dir/file1.txt"),
        p.resolve("dir/file2.txt"),
        p.resolve("file3.txt")
    ).stream());
    handler.run();
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file1.txt")), eq("foo-bucket"), eq("dir/file1.txt"));
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file2.txt")), eq("foo-bucket"), eq("dir/file2.txt"));
    verify(s3).upload(eq(Paths.get(source).resolve("file3.txt")), eq("foo-bucket"), eq("file3.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testUploadInclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String source = "foo/bar";
    String target = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = "*.zip";
    String exclude = null;
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude, p -> Arrays.asList(
        p.resolve("dir/file1.txt"),
        p.resolve("dir/file2.txt"),
        p.resolve("file3.txt"),
        p.resolve("dir/file4.zip"),
        p.resolve("file5.zip")
    ).stream());
    handler.run();
    verify(s3).upload(eq(Paths.get(source).resolve("file5.zip")), eq("foo-bucket"), eq("cats/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testUploadInclude2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String source = "foo/bar";
    String target = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = "**.zip";
    String exclude = null;
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude, p -> Arrays.asList(
        p.resolve("dir/file1.txt"),
        p.resolve("dir/file2.txt"),
        p.resolve("file3.txt"),
        p.resolve("dir/file4.zip"),
        p.resolve("file5.zip")
    ).stream());
    handler.run();
    verify(s3).upload(eq(Paths.get(source).resolve("file5.zip")), eq("foo-bucket"), eq("cats/file5.zip"));
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file4.zip")), eq("foo-bucket"), eq("cats/dir/file4.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testUploadExclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String source = "foo/bar";
    String target = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = null;
    String exclude = "*.txt";
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude, p -> Arrays.asList(
        p.resolve("dir/file1.txt"),
        p.resolve("dir/file2.txt"),
        p.resolve("file3.txt"),
        p.resolve("dir/file4.zip"),
        p.resolve("file5.zip")
    ).stream());
    handler.run();
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file1.txt")), eq("foo-bucket"), eq("cats/dir/file1.txt"));
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file2.txt")), eq("foo-bucket"), eq("cats/dir/file2.txt"));
    verify(s3).upload(eq(Paths.get(source).resolve("file5.zip")), eq("foo-bucket"), eq("cats/file5.zip"));
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file4.zip")), eq("foo-bucket"), eq("cats/dir/file4.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testUploadExclude2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String source = "foo/bar";
    String target = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = null;
    String exclude = "**.txt";
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude, p -> Arrays.asList(
        p.resolve("dir/file1.txt"),
        p.resolve("dir/file2.txt"),
        p.resolve("file3.txt"),
        p.resolve("dir/file4.zip"),
        p.resolve("file5.zip")
    ).stream());
    handler.run();
    verify(s3).upload(eq(Paths.get(source).resolve("file5.zip")), eq("foo-bucket"), eq("cats/file5.zip"));
    verify(s3).upload(eq(Paths.get(source).resolve("dir/file4.zip")), eq("foo-bucket"), eq("cats/dir/file4.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testUploadIncludeExclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String source = "foo/bar";
    String target = "s3://foo-bucket/cats";
    boolean recursive = true;
    String include = "*.zip";
    String exclude = "*cats*";
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude, p -> Arrays.asList(
        p.resolve("dir/file1.txt"),
        p.resolve("dir/file2.txt"),
        p.resolve("file3.txt"),
        p.resolve("dir/file4.zip"),
        p.resolve("file5.zip"),
        p.resolve("cats.zip")
    ).stream());
    handler.run();
    verify(s3).upload(eq(Paths.get(source).resolve("file5.zip")), eq("foo-bucket"), eq("cats/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testDownload() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "foo/bar/file.txt";
    String source = "s3://foo-bucket/bucket-file.txt";
    boolean recursive = false;
    String include = null;
    String exclude = null;
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).download(eq("foo-bucket"), eq("bucket-file.txt"), eq(Paths.get(target)));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testDownloadRecursive() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "foo/bar/";
    String source = "s3://foo-bucket/cats/";
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
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).download(eq("foo-bucket"), eq("cats/dir/file1.txt"), eq(Paths.get(target).resolve("dir/file1.txt")));
    verify(s3).download(eq("foo-bucket"), eq("cats/dir/file2.txt"), eq(Paths.get(target).resolve("dir/file2.txt")));
    verify(s3).download(eq("foo-bucket"), eq("cats/file3.txt"), eq(Paths.get(target).resolve("file3.txt")));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testDownloadRecursiveRoot() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = ".";
    String source = "s3://foo-bucket";
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

    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq(""), any(Consumer.class));
    verify(s3).download(eq("foo-bucket"), eq("dir/file1.txt"), eq(Paths.get(target).resolve("dir/file1.txt")));
    verify(s3).download(eq("foo-bucket"), eq("dir/file2.txt"), eq(Paths.get(target).resolve("dir/file2.txt")));
    verify(s3).download(eq("foo-bucket"), eq("file3.txt"), eq(Paths.get(target).resolve("file3.txt")));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testDownloadRecursiveRoot2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "/";
    String source = "s3://foo-bucket/";
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

    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq(""), any(Consumer.class));
    verify(s3).download(eq("foo-bucket"), eq("dir/file1.txt"), eq(Paths.get(target).resolve("dir/file1.txt")));
    verify(s3).download(eq("foo-bucket"), eq("dir/file2.txt"), eq(Paths.get(target).resolve("dir/file2.txt")));
    verify(s3).download(eq("foo-bucket"), eq("file3.txt"), eq(Paths.get(target).resolve("file3.txt")));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testDownloadInclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "foo/bar/";
    String source = "s3://foo-bucket/cats/";
    boolean recursive = true;
    String include = "*.zip";
    String exclude = null;
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt",
          "dir/file4.zip",
          "file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).download(eq("foo-bucket"), eq("cats/file5.zip"), eq(Paths.get(target).resolve("file5.zip")));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testDownloadInclude2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "foo/bar/";
    String source = "s3://foo-bucket/cats/";
    boolean recursive = true;
    String include = "**.zip";
    String exclude = null;
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt",
          "dir/file4.zip",
          "file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).download(eq("foo-bucket"), eq("cats/dir/file4.zip"), eq(Paths.get(target).resolve("dir/file4.zip")));
    verify(s3).download(eq("foo-bucket"), eq("cats/file5.zip"), eq(Paths.get(target).resolve("file5.zip")));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testDownloadExclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "foo/bar/";
    String source = "s3://foo-bucket/cats/";
    boolean recursive = true;
    String include = null;
    String exclude = "*.txt";
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt",
          "dir/file4.zip",
          "file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).download(eq("foo-bucket"), eq("cats/dir/file1.txt"), eq(Paths.get(target).resolve("dir/file1.txt")));
    verify(s3).download(eq("foo-bucket"), eq("cats/dir/file2.txt"), eq(Paths.get(target).resolve("dir/file2.txt")));
    verify(s3).download(eq("foo-bucket"), eq("cats/dir/file4.zip"), eq(Paths.get(target).resolve("dir/file4.zip")));
    verify(s3).download(eq("foo-bucket"), eq("cats/file5.zip"), eq(Paths.get(target).resolve("file5.zip")));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testDownloadExclude2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "foo/bar/";
    String source = "s3://foo-bucket/cats/";
    boolean recursive = true;
    String include = null;
    String exclude = "**.txt";
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt",
          "dir/file4.zip",
          "file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).download(eq("foo-bucket"), eq("cats/dir/file4.zip"), eq(Paths.get(target).resolve("dir/file4.zip")));
    verify(s3).download(eq("foo-bucket"), eq("cats/file5.zip"), eq(Paths.get(target).resolve("file5.zip")));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testDownloadIncludeExclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "foo/bar/";
    String source = "s3://foo-bucket/cats/";
    boolean recursive = true;
    String include = "*.zip";
    String exclude = "*cats*";
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt",
          "dir/file4.zip",
          "file5.zip",
          "cats.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));

    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("foo-bucket"), eq("cats/"), any(Consumer.class));
    verify(s3).download(eq("foo-bucket"), eq("cats/file5.zip"), eq(Paths.get(target).resolve("file5.zip")));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testCopy() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "s3://target-bucket/target-file.txt";
    String source = "s3://source-bucket/source-file.txt";
    boolean recursive = false;
    String include = null;
    String exclude = null;
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).copy(eq("source-bucket"), eq("source-file.txt"), eq("target-bucket"), eq("target-file.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testCopyRecursive() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "s3://target-bucket/target-dir";
    String source = "s3://source-bucket/source-dir";
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
    }).when(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/dir/file1.txt"), eq("target-bucket"), eq("target-dir/dir/file1.txt"));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/dir/file2.txt"), eq("target-bucket"), eq("target-dir/dir/file2.txt"));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/file3.txt"), eq("target-bucket"), eq("target-dir/file3.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testCopyRecursiveRoot() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "s3://target-bucket";
    String source = "s3://source-bucket/";
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
    }).when(s3).forEachKey(eq("source-bucket"), eq(""), any(Consumer.class));
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("source-bucket"), eq(""), any(Consumer.class));
    verify(s3).copy(eq("source-bucket"), eq("dir/file1.txt"), eq("target-bucket"), eq("dir/file1.txt"));
    verify(s3).copy(eq("source-bucket"), eq("dir/file2.txt"), eq("target-bucket"), eq("dir/file2.txt"));
    verify(s3).copy(eq("source-bucket"), eq("file3.txt"), eq("target-bucket"), eq("file3.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testCopyRecursiveRoot2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "s3://target-bucket/";
    String source = "s3://source-bucket";
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
    }).when(s3).forEachKey(eq("source-bucket"), eq(""), any(Consumer.class));
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("source-bucket"), eq(""), any(Consumer.class));
    verify(s3).copy(eq("source-bucket"), eq("dir/file1.txt"), eq("target-bucket"), eq("dir/file1.txt"));
    verify(s3).copy(eq("source-bucket"), eq("dir/file2.txt"), eq("target-bucket"), eq("dir/file2.txt"));
    verify(s3).copy(eq("source-bucket"), eq("file3.txt"), eq("target-bucket"), eq("file3.txt"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testCopyInclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "s3://target-bucket/target-dir";
    String source = "s3://source-bucket/source-dir";
    boolean recursive = true;
    String include = "*.zip";
    String exclude = null;
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt",
          "dir/file4.zip",
          "file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/file5.zip"), eq("target-bucket"), eq("target-dir/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testCopyInclude2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "s3://target-bucket/target-dir";
    String source = "s3://source-bucket/source-dir";
    boolean recursive = true;
    String include = "**.zip";
    String exclude = null;
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt",
          "dir/file4.zip",
          "file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/dir/file4.zip"), eq("target-bucket"), eq("target-dir/dir/file4.zip"));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/file5.zip"), eq("target-bucket"), eq("target-dir/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testCopyExclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "s3://target-bucket/target-dir";
    String source = "s3://source-bucket/source-dir";
    boolean recursive = true;
    String include = null;
    String exclude = "*.txt";
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt",
          "dir/file4.zip",
          "file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/dir/file1.txt"), eq("target-bucket"), eq("target-dir/dir/file1.txt"));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/dir/file2.txt"), eq("target-bucket"), eq("target-dir/dir/file2.txt"));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/dir/file4.zip"), eq("target-bucket"), eq("target-dir/dir/file4.zip"));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/file5.zip"), eq("target-bucket"), eq("target-dir/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testCopyExclude2() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "s3://target-bucket/target-dir";
    String source = "s3://source-bucket/source-dir";
    boolean recursive = true;
    String include = null;
    String exclude = "**.txt";
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt",
          "dir/file4.zip",
          "file5.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/dir/file4.zip"), eq("target-bucket"), eq("target-dir/dir/file4.zip"));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/file5.zip"), eq("target-bucket"), eq("target-dir/file5.zip"));
    verifyNoMoreInteractions(s3);
  }

  @Test
  void testCopyIncludeExclude() {
    S3Operations s3 = Mockito.mock(S3Operations.class);
    String target = "s3://target-bucket/target-dir";
    String source = "s3://source-bucket/source-dir";
    boolean recursive = true;
    String include = "*.zip";
    String exclude = "*cats*";
    doAnswer(invocation -> {
      Consumer<String> consumer = invocation.getArgument(2, Consumer.class);
      Arrays.asList(
          "dir/file1.txt",
          "dir/file2.txt",
          "file3.txt",
          "dir/file4.zip",
          "file5.zip",
          "cats.zip"
      ).forEach(consumer::accept);
      return null;
    }).when(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    S3CpCommandsHandler handler = new S3CpCommandsHandler(s3, source, target, recursive, include, exclude,
        p -> Collections.<Path>emptyList().stream());
    handler.run();
    verify(s3).forEachKey(eq("source-bucket"), eq("source-dir/"), any(Consumer.class));
    verify(s3).copy(eq("source-bucket"), eq("source-dir/file5.zip"), eq("target-bucket"), eq("target-dir/file5.zip"));
    verifyNoMoreInteractions(s3);
  }


}