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
    google.add_argument(
        '--requirements_file',
        help='File containing install requirements.',
        default="./requirements.txt",
    )
    google.add_argument(
        '--disk_size_gb',
        help='Disk size per instance',
    )
    google.add_argument(
        '--experiments',
        help='Experimental pipeline args',
    )
    parser.add_argument(
        '--extra_package',
        dest='extra_packages',
        action='append',
        default=None,
        help=
        ('Local path to a Python package file. The file is expected to be a '
         'compressed tarball with the suffix \'.tar.gz\' which can be '
         'installed using the easy_install command of the standard setuptools '
         'package. Multiple --extra_package options can be specified if more '
         'than one package is needed. During job submission the files will be '
         'staged in the staging area (--staging_location option) and the '
         'workers will install them in same order they were specified on the '
         'command line.'))