import import_declare_test  # noqa:F401 isort:skip
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

    # No way to get a KV store by exact match: kvstore.get does not return a
    # KVStoreCollection and search is not exact match.
    client = solnlib.splunk_rest_client.SplunkRestClient(helper.session_key, helper.app)
    kvstore = None
    for k in client.kvstore.iter(search=title):
        if k.name == title:
            kvstore = k
            break
    if kvstore is None:
        raise ValueError(f"KV store '{title}' is not visible from the app")

    if helper.get_param("mode") == "replace":
        kvstore.data.delete()
        helper.log_info("Existing records have been deleted from the KV Store.")
    _batch_save_cycle(helper.get_events(), kvstore.data, batch_size, helper)

    helper.log_info("Alert action finished.")
    return 0
