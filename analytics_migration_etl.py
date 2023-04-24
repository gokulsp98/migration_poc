import os, sys

from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import ArrayType
from pyspark.sql.utils import AnalysisException

from migration_poc.data import dataframe_schema, document_data, document_schema

SF_PARAMS = {"sfURL": "",
             "sfUser": "",
             "sfPassword": "",
             "sfRole": "",
             "sfWarehouse": "",
             "TIMESTAMP_NTZ_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF",
             "TIMESTAMP_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF",
             "sfDatabase": '"pyspark_snowflake"',
             "sfSchema": "PUBLIC"}


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class Transformation:

    def has_column(self, df, col):
        try:
            df[col]
            if df.select(col).dtypes[0][1] == "null":
                return False
            return True
        except AnalysisException:
            return False

    def get_transformed_fields(self, fields, df):
        """
         Transformation is being applied for dataframe
        """
        fields_data = []
        for field_id, field_def in fields.items():
            if field_def["Type"] in ["Currency", "User", "DateTime"]:
                transformed_field = getattr(self, 'get_%s_data' % field_def["Type"].lower())(df, field_def["Id"])
                if transformed_field: fields_data.extend(transformed_field)
            elif self.has_column(df, field_def["Id"]):
                fields_data.append(df[field_def["Id"]])
        return tuple(fields_data)

    def get_currency_data(self, df, field_id):
        """
         Below mentioned transformation is applied to a dataframe for currency field
         input ==> "_currency_field": {"unit": "USD", "dv": "500 USD", "v": 500}
         output ==> "_currency_field.unit": "USD", "_currency_field.dv": "500 USD"," _currency_field.v":500
        """
        if self.has_column(df, f"{field_id}.v"):
            data = [df[field_id].getItem("v").alias(f"{field_id}.value"),
                    df[field_id].getItem("unit").alias(f"{field_id}.unit"),
                    df[field_id].getItem("dv").alias(f"{field_id}.display_value")]
            return data

    def get_user_data(self, df, field_id):
        """
         Below mentioned transformation is applied to a dataframe for User field
         input ==>  "_created_by": {"_id": "User001", "Name": "User1", "Kind": "User"},
         output ==> "_created_by._id": "User001", "_created_by.Name": "User1", "_created_by.Kind": "User", "_created_by": {"_id": "User001", "Name": "User1", "Kind": "User"}
        """
        data = []
        if self.has_column(df, f"{field_id}._id"):
            data.append(df[field_id].getItem("_id").alias(f"{field_id}._id"))
            data.append(df[field_id])
        if self.has_column(df, f"{field_id}.Name"):
            data.append(df[field_id].getItem("Name").alias(f"{field_id}.Name"))
        if self.has_column(df, f"{field_id}.Kind"):
            data.append(df[field_id].getItem("Kind").alias(f"{field_id}.Kind"))
        return data

    def get_datetime_data(self, df, field_id):
        """
        Below mentioned transformation is applied to a dataframe for datetime field
        input ==>  "_created_at": {"td": '', "tz": "GMT", "dv": "2023-04-19T06:51:06Z",
                     "v": datetime.datetime(2023, 4, 24, 6, 8, 36, 887000)},
        output ==> "_created_at.td": "", "_created_at.tz": "GMT", "_created_at.dv": "2023-04-19T06:51:06Z", "_created_at.v": datetime.datetime(2023, 4, 24, 6, 8, 36, 887000)
        """
        if self.has_column(df, f"{field_id}.v"):
            data = [df[field_id].getItem("v").alias(f"{field_id}.value"),
                    df[field_id].getItem("dv").alias(f"{field_id}.display_value"),
                    df[field_id].getItem("td").alias(f"{field_id}.timedelta"),
                    df[field_id].getItem("tz").alias(f"{field_id}.timezone")]
            return data

    def insert_new_column(self, dataframe, child_df, col_list):
        """
        some necessary columns are inserted for child, system dataframe from its parent dataframe
        """
        insert_col = []
        main_col = []
        for ins, main in col_list:
            insert_col.append(ins)
            main_col.append(main)

        def map1(x):
            out = []
            if not x[0]:
                return [None]
            for child in x[0]:
                new_rdd = None
                child = child.asDict()
                for index, column in enumerate(insert_col):
                    child_model = Row(child_df)
                    if child.get(column) is not None:
                        new_rdd = child_model(Row(**child))
                        continue
                    row = x[index + 1]
                    child[column] = row
                    new_rdd = child_model(Row(**child))
                out.append(new_rdd)
            return out

        c_data_frame = dataframe.withColumn("ci", explode(col(child_df))).select("ci.*")

        schema = c_data_frame.schema
        for insert_column, main_column in col_list:
            if insert_column not in c_data_frame.columns:
                schema = schema.add(insert_column, dataframe.schema[main_column].dataType, True)
        new_schema = ArrayType(schema, containsNull=True)
        dataframe = dataframe.select(child_df, *main_col)
        rdd = dataframe.rdd
        rdd = rdd.flatMap(map1)
        df = spark_session.createDataFrame(rdd, schema=new_schema, verifySchema=False)
        df = df.withColumnRenamed("value", child_df)
        df = df.withColumn("ci", explode(child_df)).select("ci.*")
        return df


def do_parent_etl(metadata, dataframe):
    """
    this method does extract the parent table data from dataframe, apply transformation and
    load it to the parent snowflake table.
    """
    print(" *** Parent table ETL is being executed now *** ")
    transformation = Transformation()
    sf_fields = transformation.get_transformed_fields(metadata["Fields"], dataframe)
    table_data = dataframe.select(*sf_fields)
    table_data.write.format("net.snowflake.spark.snowflake") \
        .options(**SF_PARAMS) \
        .option("keep_column_case", "on") \
        .option("column_mapping", "name") \
        .option("column_mismatch_behavior", "ignore") \
        .option("dbtable", f'"{metadata["TableId"]}"') \
        .mode("append") \
        .save()
    print(" *** Parent table ETL is executed successfully *** ")
    print()


def do_child_etl(metadata, dataframe):
    """
    this method does extract the system table data from dataframe, insert some necessary columns, apply transformation and
    load it to the system snowflake table.
    """
    print(" *** Child table(s) ETL is being executed now *** ")
    for child_id in metadata["ChildTables"]:
        transformation = Transformation()
        insertion_column_detail = [("_model_id", "_model_id")]
        dataframe = transformation.insert_new_column(dataframe, child_id, insertion_column_detail)
        sf_fields = transformation.get_transformed_fields(metadata[child_id]["Fields"], dataframe)
        table_data = dataframe.select(*sf_fields)
        table_data.write.format("net.snowflake.spark.snowflake") \
            .options(**SF_PARAMS) \
            .option("keep_column_case", "on") \
            .option("column_mapping", "name") \
            .option("column_mismatch_behavior", "ignore") \
            .option("dbtable", f'"{metadata[child_id]["TableId"]}"') \
            .mode("append") \
            .save()

    print(" *** Child table(s) ETL is executed successfully *** ")
    print()


def do_system_etl(metadata, dataframe):
    """
    this method does extract the child table data from dataframe, insert some necessary columns, apply transformation and
    load it to the child snowflake table.
    """
    print(" *** System table(s) ETL is being executed now *** ")
    for system_id in metadata["SystemTables"]:
        transformation = Transformation()
        insertion_column_detail = [("_model_id", "_model_id")]
        dataframe = transformation.insert_new_column(dataframe, system_id, insertion_column_detail)
        sf_fields = transformation.get_transformed_fields(metadata[system_id]["Fields"], dataframe)
        table_data = dataframe.select(*sf_fields)
        table_data.write.format("net.snowflake.spark.snowflake") \
            .options(**SF_PARAMS) \
            .option("keep_column_case", "on") \
            .option("column_mapping", "name") \
            .option("column_mismatch_behavior", "ignore") \
            .option("dbtable", f'"{metadata[system_id]["TableId"]}"') \
            .mode("append") \
            .save()

    print(" *** System table(s) ETL is executed successfully *** ")
    print()


if __name__ == "__main__":
    # run the table_creation.py script before running this script
    spark_session = SparkSession.builder.appName('Mini Transformation').config('spark.jars', 'jar/snowflake-jdbc-3.13.14.jar,jar/spark-snowflake_2.12-2.10.0-spark_3.0.jar').getOrCreate()
    _dataframe = spark_session.createDataFrame(data=document_data, schema=dataframe_schema)
    do_parent_etl(document_schema, _dataframe)
    do_child_etl(document_schema, _dataframe)
    do_system_etl(document_schema, _dataframe)

