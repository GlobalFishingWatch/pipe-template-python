def setup(parser, flags):
    """
    Setup global pipeline options available both on local and remote runs.

    Arguments:
        parser -- argparse.ArgumentParser instance to setup
    """


    required = parser.add_argument_group('global required arguments')

    parser.add_argument(
        '--start-date',
        help='Initial date',
        required=True,
    )
    parser.add_argument(
        '--end-date',
        help='Initial date',
        required=True,
    )
    if not flags.get('shard_only'):
        parser.add_argument(
            '--sink_write_disposition',
            help='How to merge the output of this process with whatever records are already there in the sink tables. Might be WRITE_TRUNCATE to remove all existing data and write the new data, or WRITE_APPEND to add the new date without. Defaults to WRITE_APPEND.',
            default='WRITE_APPEND',
        )
        required.add_argument(
            '--source-table',
            help="BigQuery table where source data is located",
            required=True,
        )
    required.add_argument(
        '--sink-table',
        help="BigQuery table where destination table is placed.",
        required=True,
    )
    required.add_argument(
        '--temp-gcs-location',
        help="GCS bucket for writing temporary files.",
        required=True,
    )
    if not flags.get('create_only'):
        required.add_argument(
            '--shard-location',
            help="GCS bucket for writing sharded files.",
            required=True,
        )

