from .dedup import HydrusVideoDeduplicator

superdeduper = HydrusVideoDeduplicator()
superdeduper.deduplicate(add_missing=True, overwrite=False)