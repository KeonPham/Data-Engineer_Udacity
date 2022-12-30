from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

visa_code= {
    1: 'Business',
    2: 'Pleasure',
    3: 'Pleasure'
              }

visa_code_udf=udf(lambda x:visa_code[x],StringType())