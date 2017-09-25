def setup(parser):
    google = parser.add_argument_group('required standard dataflow options')
    google.add_argument(
        '--job_name',
        help='Name of the dataflow job',
        required=True,
    )
    google.add_argument(
        '--temp_location',
        help='GCS path for saving temporary output and staging data',
        required=True,
    )
    google.add_argument(
        '--max_num_workers',
        help='Maximum amount of workers to use.',
        required=True
    )
    google.add_argument(
        '--project',
        help='Project on which the source bigquey queries are run. This also specifies where the dataflow jobs will run.',
        required=True,
    )
