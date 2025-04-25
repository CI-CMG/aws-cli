package edu.colorado.cires.mgg.aws.cli.s3;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import org.apache.commons.lang3.StringUtils;

public final class S3Utils {

  public static String normalize(String key) {
    if (StringUtils.isBlank(key) || key.equals("/")) {
      return "";
    }
    return key.replaceAll("/+$", "");
  }

  public static boolean incExc(Path path, String include, String exclude) {
    boolean go = true;
    if(StringUtils.isNotBlank(include)) {
      PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + include.trim());
      if(!matcher.matches(path)) {
        go = false;
      }
    }

    if(go && StringUtils.isNotBlank(exclude)) {
      PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + exclude.trim());
      if(matcher.matches(path)) {
        go = false;
      }
    }

    return go;
  }

  private S3Utils() {

  }

}
