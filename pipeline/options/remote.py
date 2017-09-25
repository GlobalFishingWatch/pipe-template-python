def setup(parser):
    """
    Setup arguments parsed only on remote dataflow runs

    Arguments:
        parser -- argparse.ArgumentParser instance to setup
    """

    parser.add_argument(
        '--sink_write_disposition',
        help='How to merge the output of this process with whatever records are already there in the sink tables. Might be WRITE_TRUNCATE to remove all existing data and write the new data, or WRITE_APPEND to add the new date without. Defaults to WRITE_APPEND.',
        default='WRITE_APPEND',
    )
    parser.add_argument(
        '--wait',
        help='When present, waits until the dataflow job is done before returning.',
        action='store_true',
        default=False,
    )

    required = parser.add_argument_group('remote required arguments')
    required.add_argument(
        '--sink',
        help='BigQuery table names to which the processed data is uploaded.',
        required=True,
    )
