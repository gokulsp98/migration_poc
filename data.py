import datetime

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

document_data = [
    {"_id": "item001",
     "_model_id": "purchase_order",
     "_created_by": {"_id": "User001", "Name": "User1", "Kind": "User"},
     "_currency_field": {"unit": 'USD', "dv": "500 USD", "v": 500},
     '_created_at': {'td': '', 'tz': 'GMT', 'dv': '2023-04-19T06:51:06Z',
                     'v': datetime.datetime(2023, 4, 24, 6, 8, 36, 887000)},
     "child_table_1": [
         {"_id": "item001_child001",
          "_created_by": {"_id": "User001", "Name": "User1", "Kind": "User"},
          '_created_at': {'td': '', 'tz': 'GMT', 'dv': '2023-04-19T06:51:06Z',
                          'v': datetime.datetime(2023, 4, 24, 6, 8, 36, 887000)},
          },
         {"_id": "item001_child002",
          "_created_by": {"_id": "User002", "Name": "User2", "Kind": "User"},
          '_created_at': {'td': '', 'tz': 'GMT', 'dv': '2023-04-19T06:51:06Z',
                          'v': datetime.datetime(2023, 4, 24, 6, 8, 36, 887000)},
          }
     ],
     "system_table_1": [
         {"_id": "item001_sys001",
          "_created_by": {"_id": "User001", "Name": "User1", "Kind": "User"},
          '_created_at': {'td': '', 'tz': 'GMT', 'dv': '2023-04-19T06:51:06Z',
                          'v': datetime.datetime(2023, 4, 24, 6, 8, 36, 887000)},
          },
         {"_id": "item001_sys002",
          "_created_by": {"_id": "User002", "Name": "User2", "Kind": "User"},
          '_created_at': {'td': '', 'tz': 'GMT', 'dv': '2023-04-19T06:51:06Z',
                          'v': datetime.datetime(2023, 4, 24, 6, 8, 36, 887000)},
          }
     ]
     },
    {"_id": "item002",
     "_model_id": "purchase_order",
     "_created_by": {"_id": "User001", "Name": "User1", "Kind": "User"},
     "_currency_field": {"unit": 'USD', "dv": "500 USD", "v": 500},
     '_created_at': {'td': '', 'tz': 'GMT', 'dv': '2023-04-19T06:51:06Z',
                     'v': datetime.datetime(2023, 4, 24, 6, 8, 36, 887000)},
     "child_table_1": [
         {"_id": "item002_child001",
          "_created_by": {"_id": "User001", "Name": "User1", "Kind": "User"},
          '_created_at': {'td': '', 'tz': 'GMT', 'dv': '2023-04-19T06:51:06Z',
                          'v': datetime.datetime(2023, 4, 24, 6, 8, 36, 887000)},
          }
     ],
     "system_table_1": [{"_id": "item002_sys001",
                         "_created_by": {"_id": "User001", "Name": "User1", "Kind": "User"},
                         '_created_at': {'td': '', 'tz': 'GMT', 'dv': '2023-04-19T06:51:06Z',
                                         'v': datetime.datetime(2023, 4, 24, 6, 8, 36, 887000)},
                         }
                        ]
     }
]

dataframe_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("_model_id", StringType(), True),
    StructField('_created_by', StructType([
        StructField('_id', StringType(), True),
        StructField('Name', StringType(), True),
        StructField('Kind', StringType(), True)
    ])),
    StructField('_created_at', StructType([
        StructField('td', StringType(), True),
        StructField('tz', StringType(), True),
        StructField('dv', StringType(), True),
        StructField('v', TimestampType(), True)
    ])),
    StructField('_currency_field', StructType([
        StructField('unit', StringType(), True),
        StructField('dv', StringType(), True),
        StructField('v', StringType(), True)
    ])),
    StructField('child_table_1', ArrayType(StructType([
        StructField('_id', StringType(), True),
        StructField('_created_by', StructType([
            StructField('_id', StringType(), True),
            StructField('Name', StringType(), True),
            StructField('Kind', StringType(), True)
        ])),
        StructField('_created_at', StructType([
            StructField('td', StringType(), True),
            StructField('tz', StringType(), True),
            StructField('dv', StringType(), True),
            StructField('v', TimestampType(), True)
        ])),
    ])), True),
    StructField('system_table_1', ArrayType(StructType([
        StructField('_id', StringType(), True),
        StructField('_created_by', StructType([
            StructField('_id', StringType(), True),
            StructField('Name', StringType(), True),
            StructField('Kind', StringType(), True)
        ])),
        StructField('_created_at', StructType([
            StructField('td', StringType(), True),
            StructField('tz', StringType(), True),
            StructField('dv', StringType(), True),
            StructField('v', TimestampType(), True)
        ])),
    ])), True)
])

document_schema = {'Id': 'purchase_order', 'TableId': 'purchase_order',
               'ChildTables': ['child_table_1'],
               'SystemTables': ["system_table_1"],
               'Fields': {
                   '_id': {'Id': '_id', 'Type': 'Text'},
                   '_model_id': {'Id': '_model_id', 'Type': 'Text'},
                   '_created_by': {'Id': '_created_by', 'Type': 'User'},
                   '_created_at': {'Id': '_created_at', 'Type': 'DateTime'},
                   '_currency_field': {'Id': '_currency_field', 'Type': 'Currency'},

               },
               'child_table_1': {'Id': 'child_table_1', 'TableId': 'purchase_order.child_table_1',
                                 'Fields': {
                                     '_id': {'Id': '_id', 'Type': 'Text'},
                                     '_model_id': {'Id': '_model_id', 'Type': 'Text'},
                                     '_created_by': {'Id': '_created_by', 'Type': 'User'},
                                     '_created_at': {'Id': '_created_at', 'Type': 'DateTime'},
                                     '_currency_field': {'Id': '_currency_field', 'Type': 'Currency'}}},

               'system_table_1': {'Id': 'system_table_1',
                                  'TableId': 'system_table_1',
                                  'Fields': {
                                      '_id': {'Id': '_id', 'Type': 'Text'},
                                      '_model_id': {'Id': '_model_id', 'Name': 'ID', 'Type': 'Text'},
                                      '_created_by': {'Id': '_created_by', 'Type': 'User'},
                                      '_created_at': {'Id': '_created_at', 'Type': 'DateTime'},
                                      '_currency_field': {'Id': '_currency_field', 'Type': 'Currency'}}
                                  }}
