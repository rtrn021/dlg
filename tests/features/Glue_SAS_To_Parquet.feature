Feature: Test Glue Job SAS to Parquet

  Scenario Outline: Validate tables from connnection
    Given I get list folders from client with these details "<client>" "<bucket_name>" "<prefix>"
    Then I validate list folder has these "table_1" is exist in the data
    Examples:
      | client | bucket_name | prefix |
      | s3     | bucket_name | /      |

  Scenario: Validate data exist from filepath
    Given I read "filepath"
    Then I validate data has records

  Scenario: Validate audit columns
    Given I read "filepath"
    Then I add audit cols
    Then I validate data has "operation" column
    Then I validate data has "processeddate" column
    Then I validate data has "changedate" column
    Then I validate data has "changedate_year" column
    Then I validate data has "changedate_month" column
    Then I validate data has "changedate_day" column

  Scenario: Validate after writing data to target path
    Given I read "filepath"
    Then I add audit cols
    Then I write to parquet write mode as "overwrite" partition_cols as "processeddate" target_path as "target_path"
    Then I read "target_path"
    Then I validate data has "operation" column
    Then I validate data has "processeddate" column
    Then I validate data has "changedate" column
    Then I validate data has "changedate_year" column
    Then I validate data has "changedate_month" column
    Then I validate data has "changedate_day" column
    Then I validate I have records


