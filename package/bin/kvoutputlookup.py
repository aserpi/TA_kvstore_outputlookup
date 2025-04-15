import import_declare_test  # noqa:F401 isort:skip

import sys

from splunklib.searchcommands import (
    Configuration,
    Option,
    ReportingCommand,
    dispatch,
    validators,
)


@Configuration
class KVOutputlookup(ReportingCommand):
    # TODO: Option 'allow_updates'
    append = Option(
        doc="Append records to the lookup table.",
        default=False,
        require=False,
        validate=validators.Boolean(),
    )
    key_field = Option(doc="Field to use as key.", default=None, require=False)
    required_fields = Option(
        doc="Required fields.",
        default=None,
        require=False,
        validate=validators.Set("accelerated", "all", "none"),
    )

    def __init__(self):
        super().__init__()
        self.chunk = 0

    def reduce(self, records):
        raise NotImplementedError


dispatch(KVOutputlookup, sys.argv, sys.stdin, sys.stdout, __name__)
