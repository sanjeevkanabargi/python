import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

avroSchema = "/Users/skanabargi/dataSource/schema/fromAzham/event-avro.schema"
schema = avro.schema.Parse(open(avroSchema, "r").read())


avroFile = "/Users/skanabargi/dataSource/schema/fromAzham/cfw-sample-avro.data"
reader = DataFileReader(open(avroFile, "r"), DatumReader())
for user in reader:
    print(user)
reader.close()