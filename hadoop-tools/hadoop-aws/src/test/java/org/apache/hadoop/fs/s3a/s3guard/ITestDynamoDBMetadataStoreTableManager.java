package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class ITestDynamoDBMetadataStoreTableManager {
  private static DynamoDBMetadataStoreTableManager tableManager;

  @Before
  public void setUp() throws Exception {
    Configuration conf = S3GuardTestUtils.prepareTestConfigurationForS3Guard();
  }

  @Test
  public void testInitWithSseEnabled() throws Exception {

  }

  @Test
  public void testInitWithCMK() throws Exception {

  }

}
