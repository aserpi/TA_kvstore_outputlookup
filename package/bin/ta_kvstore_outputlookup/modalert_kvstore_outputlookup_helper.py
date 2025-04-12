import import_declare_test  # noqa:F401 isort:skip

import itertools
import json

import solnlib
import splunklib.binding


def _batch_save(events, kvstore_data, max_batch_size, helper):
    """Saves events into a KV store in batches.

    Batch size halves on failure and doubles (up to the initial max) after
    successful saves.
    """
    batch_size = max_batch_size
    while True:
        full_batch = list(itertools.islice(events, batch_size))
        if not full_batch:
            break

        # Use offset to avoid repeated slicing
        offset = 0
        while offset < len(full_batch):
            batch = full_batch[offset : offset + batch_size]
            try:
                kvstore_data.batch_save(*batch)
                offset += batch_size

                # Double the batch size after success, but not beyond max_batch_size
                increased_batch_size = min(batch_size * 2, max_batch_size)
                if increased_batch_size > batch_size:
                    batch_size = increased_batch_size
                    helper.log_info(
                        f"Batch saved, increasing batch size to {batch_size} events."
                    )

            except splunklib.binding.HTTPError as e:
                if e.status != 400 or "max_size_per_batch_save_mb" not in e.args[0]:
                    raise

                batch_size = len(batch)  # May be smaller in last loop
                if batch_size == 1:
                    raise ValueError(
                        "A record is too large to be inserted into the KV store. "
                        "Try increasing 'max_size_per_batch_save_mb' in limits.conf"
                    )

                # Reduce batch size by half on failure
                batch_size = batch_size // 2
                helper.log_warn(f"Batch too large, decreasing to {batch_size} events.")


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

    _batch_save(helper.get_events(), kvstore.data, batch_size, helper)
    helper.log_info("Alert action finished.")
    return 0
