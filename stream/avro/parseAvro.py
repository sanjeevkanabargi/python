import avro.schema
from  avro.datafile import DataFileReader
from avro.io import DatumReader 

reader = avro.datafile.DataFileReader(input,avro.io.DatumReader())
schema = reader.datum_reader.writers_schema
print(schema)
