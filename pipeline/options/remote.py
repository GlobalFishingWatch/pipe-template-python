def setup(parser):
    """
    Setup arguments parsed only on remote dataflow runs

    Arguments:
        parser -- argparse.ArgumentParser instance to setup
    """

    parser.add_argument(
        '--wait',
        help='When present, waits until the dataflow job is done before returning.',
        action='store_true',
        default=False,
    )



