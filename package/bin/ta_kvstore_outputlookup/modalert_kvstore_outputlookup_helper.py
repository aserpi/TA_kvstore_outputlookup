import import_declare_test  # noqa:F401 isort:skip

import collections.abc
import itertools
import json

import solnlib
import splunklib.binding
from splunklib.client import KVStoreCollectionData
from splunktaucclib.alert_actions_base import ModularAlertBase


def _batch_save(
    events: collections.abc.Iterator[dict],
    kvstore_data: KVStoreCollectionData,
    batch_size: int,
    helper: ModularAlertBase,
):
    """Saves events into a KV store in batches.

    Batch size halves on failure and doubles (up to the initial max) after
    successful saves. Optimized to handle both lists and generators efficiently.

    Args:
        events: A list or generator of event dictionaries to save
        kvstore_data: The KV store collection to save events to
        batch_size: The maximum number of events to save in a single batch
        helper: Helper object for logging
    """
    # Slice whenever possible, as it is faster
    if isinstance(events, collections.abc.Sequence):
        position = 0
        while position < len(events):
            batch = events[position : position + batch_size]
            position += batch_size
            batch_size = _process_batch(batch, kvstore_data, batch_size, helper)
    else:
        # Otherwise, iterate over the elements
        while True:
            batch = list(itertools.islice(events, batch_size))
            if not batch:
                break
            batch_size = _process_batch(batch, kvstore_data, batch_size, helper)


def _process_batch(
    batch: collections.abc.Sequence[dict],
    kvstore_data: KVStoreCollectionData,
    batch_size: int,
    helper: ModularAlertBase,
) -> int:
    """Saves a batch of events, with dynamic batch size adjustment.

    Args:
        batch: List of events to process
        kvstore_data: KV store collection to save events to
        batch_size: Current batch size
        helper: Helper object for logging

    Returns:
        int: Updated batch size for the next operation
    """
    offset = 0
    max_batch_size = batch_size  # Remember the max batch size

    while offset < len(batch):
        current_batch = batch[offset : offset + batch_size]
        try:
            kvstore_data.batch_save(*current_batch)
            offset += batch_size

            # Double the batch size after success, but not beyond max_batch_size
            increased_batch_size = min(batch_size * 2, max_batch_size)
            if increased_batch_size > batch_size:
                batch_size = increased_batch_size
                helper.log_info(
                    f"Batch saved, increasing batch size to {batch_size} events."
                )
        except splunklib.binding.HTTPError as e:
            if e.status != 400 or "max_size_per_batch_save_mb" not in str(e):
                raise

            batch_size = len(current_batch)  # May be smaller in last loop
            if batch_size == 1:
                raise ValueError(
                    "A record is too large to be inserted into the KV store. "
                    "Try increasing 'max_size_per_batch_save_mb' in limits.conf"
                )

            # Halve batch size on failure
            batch_size = batch_size // 2
            helper.log_warn(f"Batch too large, decreasing to {batch_size} events.")
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
