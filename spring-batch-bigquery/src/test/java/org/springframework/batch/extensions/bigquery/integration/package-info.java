/**
 * In order to launch these tests you should provide a way how to authorize to Google BigQuery.
 * A simple way is to create service account, store credentials as JSON file and provide environment variable.
 * Example: GOOGLE_APPLICATION_CREDENTIALS=/home/dgray/Downloads/bq-key.json
 * @see <a href="https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries#before-you-begin">Authentication</a>
 *
 * Test names should follow this pattern: test1, test2, testN.
 * So later in BigQuery you will see generated table name: csv_test1, csv_test2, csv_testN.
 * This way it will be easier to trace errors in BigQuery.
 */
package org.springframework.batch.extensions.bigquery.integration;