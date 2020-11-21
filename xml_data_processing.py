import xml.etree.ElementTree as ET
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,FloatType,DateType

col_name=['CustomerID','CompanyName','ContactName','ContactTitle','Phone','EnrollDate']
ele_to_extract=[ c for c in col_name if c!='CustomerID']


# reading xml file into 1 file as RDD .
def set_schema():
    sch=[]
    for c in col_name:
        if c=='Phone':
            sch.append(StructField(c,IntegerType(),True))
        elif c=='EnrollDate':
            sch.append(StructField(c,DateType(),True))
        else:
            sch.append(StructField(c,StringType(),True))
    return StructType(sch)



def parse_xml(rdd):
    """
    Read the xml string from rdd, parse and extract the elements,
    then return a list of list.
    """
    results = []
    root = ET.fromstring(rdd[0])

    for b in root.findall('Customer'):
        rec = []
        rec.append(b.attrib['CustomerID'])
        for e in ele_to_extract:
            if b.find(e) is None:
                rec.append(None)
                continue
            value=b.find(e).text
            if e=='Phone':\
                value=int(value)
            elif e=='EnrollDate':
                value=datetime.strptime(value,'%Y-%m-%d')

            rec.append(value)



        results.append(rec)

    return results

spark=SparkSession.builder.appName("xml_data_processing").getOrCreate()
usr_schema=set_schema()
read_rdd=spark.read.text("C:/Users/91988/Desktop/Pyspark_practice/xml_input_data/sample_order_test.xml",wholetext=True).rdd
#for i in read_rdd.collect():
#    print(i)
record_rdd=read_rdd.flatMap(parse_xml)
#for j in record_rdd.collect():
#    print(j)
cust_DF=record_rdd.toDF(usr_schema)
cust_DF.show()