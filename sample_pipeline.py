import random
import string

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from sklearn import preprocessing

GCP_PROJECT = 'mle-exam'
GCS_BUCKET = 'mle-exam-staging'  # Staging GCS bucket
BQ_SOURCE = {
    'project': 'mle-exam',
    'dataset': 'samples',
    'table': 'iris',
}
BQ_OUTPUT = {
    'project': 'mle-exam',
    'dataset': 'samples',
    'table': 'iris_output',
}
OUTPUT_SCHEMA = "sepal_length:FLOAT64,sepal_width:FLOAT64,sepal_area:FLOAT64,petal_length:FLOAT64,petal_width:FLOAT64,petal_area:FLOAT64,species_0:INT64,species_1:INT64,species_2:INT64"
BASE_SPECIES = [['versicolor'], ['virginica'], ['setosa']]


def get_random_string(n):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))


def _create_pipeline_options():
    pipeline_options = PipelineOptions.from_dictionary({
        'project': GCP_PROJECT,
        'runner': 'DataflowRunner',
        'temp_location': 'gs://{}/dataflow-staging/temp'.format(GCS_BUCKET),
        'staging_location': 'gs://{}/dataflow-staging/staging'.format(GCS_BUCKET),
        'job_name': 'sklearn-parallel-{}'.format(get_random_string(6)),
        'num_workers': 1,
        'autoscaling_algorithm': 'NONE',
        'worker_machine_type': 'n1-standard-1',
        'save_main_session': True,
    })
    return pipeline_options


def encode_species(bq_row):
    species = bq_row['species']
    enc = preprocessing.OneHotEncoder()
    enc.fit(BASE_SPECIES)
    one_hot = enc.transform([[species]]).toarray()
    result_row = {
        'sepal_length': bq_row['sepal_length'],
        'sepal_width': bq_row['sepal_width'],
        'sepal_area': bq_row['sepal_length'] * bq_row['sepal_width'],
        'petal_length': bq_row['petal_length'],
        'petal_width': bq_row['petal_width'],
        'petal_area': bq_row['petal_length'] * bq_row['petal_width'],
        'species_0': int(one_hot[0][0]),
        'species_1': int(one_hot[0][1]),
        'species_2': int(one_hot[0][2]),
    }
    return result_row


def create_dataflow_pipeline():
    pipeline_options = _create_pipeline_options()
    p = beam.Pipeline(options=pipeline_options)
    (p
     | 'IngestRecords' >> beam.io.Read(beam.io.BigQuerySource(use_standard_sql=True, **BQ_SOURCE))
     | 'Reshuffle' >> beam.Reshuffle()
     | 'EncodeCategories' >> beam.Map(encode_species)
     | 'WriteBQ' >> beam.io.WriteToBigQuery(schema=OUTPUT_SCHEMA,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,  # Overwrite output table if needed
                                            **BQ_OUTPUT))
    return p


def main():
    pipeline = create_dataflow_pipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
