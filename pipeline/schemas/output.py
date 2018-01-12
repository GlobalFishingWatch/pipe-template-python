from apache_beam.io.gcp.internal.clients import bigquery

def build():
    schema = bigquery.TableSchema()

    field = bigquery.TableFieldSchema()
    field.name = "id"
    field.type = "INTEGER"
    field.mode="REQUIRED"
    schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = "timestamp"
    field.type = "TIMESTAMP"
    field.mode="REQUIRED"
    schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = "lat"
    field.type = "FLOAT"
    field.mode="REQUIRED"
    schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = "lon"
    field.type = "FLOAT"
    field.mode="REQUIRED"
    schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = "score"
    field.type = "FLOAT"
    field.mode="REQUIRED"
    schema.fields.append(field)

    return schema
