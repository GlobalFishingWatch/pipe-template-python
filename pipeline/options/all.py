from pipeline.options.actions import ReadFileAction

def setup(parser):
    """
    Setup global pipeline options available both on local and remote runs.

    Arguments:
        parser -- argparse.ArgumentParser instance to setup
    """

    parser.add_argument(
        '--fishing_threshold',
        help='Score threshold to consider a message to be a fishing event',
        type=float,
        default=0.5,
    )

    required = parser.add_argument_group('global required arguments')
    required.add_argument(
        '--source',
        help="BigQuery query that returns the records to process. Might be either a query or a file containing the query if using the `@path/to/file.sql syntax`. See examples/local.sql.",
        required=True,
        action=ReadFileAction,
    )
