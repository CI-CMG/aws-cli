package gov.noaa.ncei.cmg.trackline.cli.s3;

import org.apache.commons.lang3.StringUtils;

public final class S3Utils {

  public static String normalize(String key) {
    if (StringUtils.isBlank(key) || key.equals("/")) {
      return "";
    }
    return key.replaceAll("/+$", "");
  }

  private S3Utils() {

  }

}
