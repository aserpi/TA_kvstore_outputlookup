import import_declare_test  # noqa:F401 isort:skip

import json

import solnlib
import splunklib.binding


def _batch_save(batch, kvstore_data, helper):
    """Saves a batch of events into a KV store.

    Returns None if the events were saved with a single batch_save.
    If they are too large, the batch gets recursively halved and the new
    batch size is returned."""
    try:
        kvstore_data.batch_save(*batch)
    except splunklib.binding.HTTPError as e:
        if e.status != 400 or "max_size_per_batch_save_mb" not in e.args[0]:
            raise
        if len(batch) == 1:
            raise ValueError(
                "A record is too large to be inserted into the KV store."
                "Try increasing 'max_size_per_batch_save_mb' in limits.conf"
            )
        batch_size = len(batch) // 2
        helper.log_warn(f"Batch too large, decreasing to {batch_size} events.")
        return _batch_save_cycle(batch, kvstore_data, batch_size, helper)


def _batch_save_cycle(events, kvstore_data, batch_size, helper):
    """Saves events into a KV store in batches.

    If a batch is too large, it is shrunk until a suitable size is found, which
    then becomes the new batch size."""
    batch = []
    for idx, result in enumerate(events):
        batch.append(result)
        if (idx + 1) % batch_size == 0:
            batch_size = _batch_save(batch, kvstore_data, helper) or batch_size
            batch = []
    if batch:
        batch_size = _batch_save(batch, kvstore_data, helper) or batch_size
    return batch_size


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

    title = helper.get_param("kvstore")
    helper.log_debug(f"Setting 'kvstore' to '{title}'.")

    client = solnlib.splunk_rest_client.SplunkRestClient(helper.session_key, helper.app)
    try:
        kvstore = client.kvstore[title]
    except KeyError:
        raise ValueError(f"KV store '{title}' is not visible from the app")

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

    if helper.get_param("mode") == "replace":
        kvstore.data.delete()
        helper.log_info("Existing records have been deleted from the KV Store.")
    _batch_save_cycle(helper.get_events(), kvstore.data, batch_size, helper)

    helper.log_info("Alert action finished.")
    return 0
