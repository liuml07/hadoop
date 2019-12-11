package org.apache.hadoop.fs.s3a.s3guard;

import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_NAME_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_TAG;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.prepareTestConfiguration;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.junit.Assume;

public class S3GuardTestUtils {
  static String testDynamoDBTableName;

  /**
   * Validate and patch configuration for initializing the test.
   */
  static Configuration prepareTestConfigurationForS3Guard() {
    Configuration conf = prepareTestConfiguration(new Configuration());
    assumeThatDynamoMetadataStoreImpl(conf);
    // S3GUARD_DDB_TEST_TABLE_NAME_KEY and S3GUARD_DDB_TABLE_NAME_KEY should
    // be configured to use this test.
    testDynamoDBTableName = conf.get(
        S3ATestConstants.S3GUARD_DDB_TEST_TABLE_NAME_KEY);
    String dynamoDbTableName = conf.getTrimmed(S3GUARD_DDB_TABLE_NAME_KEY);
    Assume.assumeTrue("No DynamoDB table name configured in "
                          + S3GUARD_DDB_TABLE_NAME_KEY,
        !StringUtils.isEmpty(dynamoDbTableName));

    // We should assert that the table name is configured, so the test should
    // fail if it's not configured.
    assertNotNull("Test DynamoDB table name '"
        + S3ATestConstants.S3GUARD_DDB_TEST_TABLE_NAME_KEY + "'"
        + " should be set to run integration tests.",
        testDynamoDBTableName);

    // We should assert that the test table is not the same as the production
    // table, as the test table could be modified and destroyed multiple
    // times during the test.
    assertNotEquals("Test DynamoDB table name: "
        + "'" + S3ATestConstants.S3GUARD_DDB_TEST_TABLE_NAME_KEY + "'"
        + " and production table name: "
        + "'" + S3GUARD_DDB_TABLE_NAME_KEY + "' can not be the same.",
        testDynamoDBTableName, conf.get(S3GUARD_DDB_TABLE_NAME_KEY));

    // We can use that table in the test if these assertions are valid
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, testDynamoDBTableName);

    // remove some prune delays
    conf.setInt(S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY, 0);

    // clear all table tagging config before this test
    conf.getPropsWithPrefix(S3GUARD_DDB_TABLE_TAG).keySet().forEach(
        propKey -> conf.unset(S3GUARD_DDB_TABLE_TAG + propKey)
    );

    tagConfiguration(conf);

    return conf;
  }

  static void assumeThatDynamoMetadataStoreImpl(Configuration conf){
    Assume.assumeTrue("Test only applies when DynamoDB is used for S3Guard",
        conf.get(Constants.S3_METADATA_STORE_IMPL).equals(
            Constants.S3GUARD_METASTORE_DYNAMO));
  }

  static Map<String, String> createTagMap() {
    Map<String, String> tagMap = new HashMap<>();
    tagMap.put("hello", "dynamo");
    tagMap.put("tag", "youre it");
    return tagMap;
  }

  /**
   * Set the tags on the table so that it can be tested later.
   */
  static void tagConfiguration(Configuration conf) {
    Map<String, String> tagMap = createTagMap();
    for (Map.Entry<String, String> tagEntry : tagMap.entrySet()) {
      conf.set(S3GUARD_DDB_TABLE_TAG + tagEntry.getKey(), tagEntry.getValue());
    }
  }

}
