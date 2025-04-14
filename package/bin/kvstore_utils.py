import import_declare_test  # noqa:F401

import collections.abc
import itertools

import splunklib.binding
from splunklib.client import KVStoreCollectionData
from splunktaucclib.alert_actions_base import ModularAlertBase


def batch_save(
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
