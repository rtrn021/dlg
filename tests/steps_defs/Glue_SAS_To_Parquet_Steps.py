import datetime
from pytest_bdd import scenario, given, when, then,parsers
import pytest
from functools import partial
from apps.Test_Glue_Job_SAS_To_Parquet import create_glue_context, list_folders, read_sas_table, add_audit_cols,write_to_parquet
from datas import Spark_Data

@scenario('../features/Glue_SAS_To_Parquet.feature', 'Validate tables from connnection')
def test_start():
    pass

@scenario('../features/Glue_SAS_To_Parquet.feature', 'Validate data exist from filepath')
def test_start():
    pass

@scenario('../features/Glue_SAS_To_Parquet.feature', 'Validate audit columns')
def test_start():
    pass

@scenario('../features/Glue_SAS_To_Parquet.feature', 'Validate after writing data to target path')
def test_start():
    pass

EXTRA_TYPES = {

}
parse=partial(parsers.cfparse,extra_types=EXTRA_TYPES)

@pytest.fixture
def get_spark_df():
    return Spark_Data.get_df()

@pytest.fixture
def get_list_data():
    return Spark_Data.get_list_data()

@pytest.fixture
@given(parse('I create glue context'))
def glue_context():
    return create_glue_context()

@then(parse('I get list folders from client with these details "<client>" "<bucket_name>" "<prefix>"'))
def get_list_folders(client,bucket_name,prefix):
    Spark_Data.set_list_data(list_folders(client,bucket_name,prefix))
    return Spark_Data.get_list_data()

@then(parse('I validate list folder has these "{table}" is exist in the data'))
def check_table(get_list_data,table):
    assert table in get_list_data

@given(parse('I read "{filepath}"'))
def read_file(glue_context,filepath):
    Spark_Data.set_df(read_sas_table(glue_context[0],filepath))
    return Spark_Data.get_df()

@then(parse('I validate data has records'))
def validate_record_exist(get_spark_df):
    assert get_spark_df.count()>0

@then(parse('I add audit cols'))
def validate_record_exist(get_spark_df):
    Spark_Data.set_df(add_audit_cols(get_spark_df,datetime.now()))
    return Spark_Data.get_df()


@then(parse('I validate data has "{column}" column'))
def validate_record_exist(get_spark_df,column):
    assert column in get_spark_df.columns

@then(parse('I write to parquet write mode as "{writemode}" partition_cols as "{partition_cols}" target_path as "{target_path}"'))
def write_to_parquet_as(get_spark_df,writemode,partition_cols,target_path):
    write_to_parquet(get_spark_df,writemode,partition_cols,target_path)


