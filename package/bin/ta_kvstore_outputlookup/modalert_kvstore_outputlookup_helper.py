import import_declare_test  # noqa:F401

import json

import solnlib

from kvstore_utils import batch_save  # noqa:F401


def process_event(helper, *args, **kwargs):
    helper.log_info("Alert action started.")

    # Default limits for batch_save are 10k records and 50MB
    batch_size = int(
        solnlib.conf_manager.ConfManager(helper.session_key, helper.app)
        .get_conf("limits")
        .get("kvstore")
        .get("max_documents_per_batch_save")
    )
    helper.log_debug(f"Setting 'batch_size' to '{batch_size}'.")

    # Get KV store
    title = helper.get_param("kvstore")
    helper.log_debug(f"Setting 'kvstore' to '{title}'.")
    client = solnlib.splunk_rest_client.SplunkRestClient(helper.session_key, helper.app)
    try:
        kvstore = client.kvstore[title]
    except KeyError:
        raise ValueError(f"KV store '{title}' is not visible from the app")

    # Check if all required fields are present
    require_fields = helper.get_param("require_fields")
    required_fields = None
    if require_fields == "all":  # Get all fields in the KV store
        required_fields = {f[6:] for f in kvstore.content if f.startswith("field.")}
    elif require_fields == "accelerated":  # Get all fields in a KV store index
        kvstore_idx = [
            v for k, v in kvstore.content.items() if k.startswith("accelerated_fields.")
        ]
        required_fields = {f for idx in kvstore_idx for f in json.loads(idx).keys()}
    if required_fields is not None:
        result_fields = set(next(helper.get_events()).keys())  # Get results fields
        missing_fields = required_fields - result_fields
        if missing_fields:
            raise ValueError(
                f"Required KV store fields {', '.join(missing_fields)} are missing "
                f"from the search results"
            )
        helper.log_info(
            "All required KV store fields are included in the search results."
        )

    # Clear KV store if necessary
    if helper.get_param("mode") == "replace":
        kvstore.data.delete()
        helper.log_info("Existing records have been deleted from the KV tore.")

    batch_save(helper.get_events(), kvstore.data, batch_size, helper)
    helper.log_info("Alert action finished.")
    return 0
