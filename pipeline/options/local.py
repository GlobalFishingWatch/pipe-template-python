def setup(parser):
    """
    Setup arguments parsed only on local test runs

    Arguments:
        parser -- argparse.ArgumentParser instance to setup
    """

    required = parser.add_argument_group('local required arguments')
    required.add_argument(
        '--project',
        help='Project on which the source bigquey queries are run.',
        required=True,
    )

