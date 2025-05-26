# Theory

![Hydrus Video Deduplicator High Level Diagram](img/hvd_high_level_diagram.drawio.svg)

1. First, video files are perceptually hashed. These perceptual hashes are cached in a local database.

1. Then, a similarity search cache is built using the perceptual hashes to make it possible to compare video similarities very quickly.

1. Finally, the search cache is queried for a given relative similarity threshold, and video pairs that exceed that that threshold
will be marked as potential duplicates in Hydrus via the Hydrus Client API.
