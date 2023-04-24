import snowflake.connector
from snowflake.connector import DictCursor

cursor = snowflake.connector.connect(user="",
                                     password="",
                                     role="",
                                     warehouse="",
                                     account="")

curs = cursor.cursor(DictCursor)

create_database = """ create database "pyspark_snowflake" """

parent_table = """
create table "pyspark_snowflake".public."purchase_order"
("_id" varchar(16777216),
"Name" varchar(16777216),
"_created_by._id" varchar(16777216),
"_created_by.Name" varchar(16777216),
"_created_by.Kind" varchar(16777216),
"_created_by" object,
"_currency_field.value" NUMBER(38,10),
"_currency_field.unit" varchar(16777216),
"_currency_field.display_value" varchar(16777216),
"_created_at.display_value" varchar(16777216),
"_created_at.value" TIMESTAMP_NTZ(9),
"_created_at.timedelta" varchar(16777216),
"_created_at.timezone" varchar(16777216))
"""

child_table = """
create table "pyspark_snowflake".public."purchase_order.child_table_1"
("_id" varchar(16777216),
"Name" varchar(16777216),
"_created_by._id" varchar(16777216),
"_created_by.Name" varchar(16777216),
"_created_by.Kind" varchar(16777216),
"_created_by" object,
"_created_at.display_value" varchar(16777216),
"_created_at.value" TIMESTAMP_NTZ(9),
"_created_at.timedelta" varchar(16777216),
"_created_at.timezone" varchar(16777216))
"""

system_table = """
create table "pyspark_snowflake".public."system_table_1"
("_id" varchar(16777216),
"Name" varchar(16777216),
"_created_by._id" varchar(16777216),
"_created_by.Name" varchar(16777216),
"_created_by.Kind" varchar(16777216),
"_created_by" object,
"_created_at.display_value" varchar(16777216),
"_created_at.value" TIMESTAMP_NTZ(9),
"_created_at.timedelta" varchar(16777216),
"_created_at.timezone" varchar(16777216))
"""


if __name__ == "__main__":
    print(" *** Table creation queries are being executed now *** ")
    curs.execute(create_database)
    curs.execute(parent_table)
    curs.execute(child_table)
    curs.execute(system_table)
    print(" *** Table creation queries are executed successfully *** ")
