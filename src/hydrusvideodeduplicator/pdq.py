from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

from sqlitedict import SqliteDict
from .config import PDQ_DATABASE_FILE, PDQ_TABLE_NAME
import hydrusvideodeduplicator.hydrus_api as hydrus_api


class Relationship:
    def __init__(
        self,
        hash_a: str,
        hash_b: str,
        relationship_type: int = int(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES),
        do_default_content_merge: bool = True,
    ):
        # dict.__init__(
        #     self,
        #     hash_a=hash_a,
        #     hash_b=hash_b,
        #     relationship=relationship_type,
        #     do_default_content_merge=do_default_content_merge,
        # )
        self.hash_a = hash_a
        self.hash_b = hash_b
        self.relationship = relationship_type
        self.do_default_content_merge = do_default_content_merge

    def to_dict(self) -> dict:
        return {
            "hash_a": self.hash_a,
            "hash_b": self.hash_b,
            "relationship": self.relationship,
            "do_default_content_merge": self.do_default_content_merge,
        }

    def get_db_key(self) -> str:
        if self.hash_a > self.hash_b:
            return str(hash(hash(self.hash_a) + hash(self.hash_b)))
        elif self.hash_a < self.hash_b:
            return str(hash(hash(self.hash_b) + hash(self.hash_a)))
        else:
            raise Exception(
                "hash_a is the same as hash_b, which means we're comparing a file with itself - something's not "
                "right here."
            )


class PotentialDuplicatesQueue:
    def __init__(self, client: hydrus_api, flush_count: int = 1):
        self._queue = deque()
        self._client = client
        self._flush_count = flush_count

    def add(self, relationship: Relationship) -> None:
        self._queue.append(relationship)

        if self._flush_count != 0 and len(self._queue) >= self._flush_count:
            # Flush something
            self.flush_to_db()

    def flush_to_db(self, overwrite_db_contents: bool = False) -> None:
        flag = "w" if overwrite_db_contents else "c"
        relationship = None

        try:
            with SqliteDict(str(PDQ_DATABASE_FILE), tablename=PDQ_TABLE_NAME, flag=flag, autocommit=True) as pdq_db:
                while self._queue:
                    relationship = self._queue.pop()
                    pdq_db[relationship.get_db_key()] = relationship
        except Exception as e:
            if relationship is not None:
                self._queue.append(relationship)
            raise e

    def populate_from_db(self) -> None:
        old_flush_count = self._flush_count
        try:
            # temporarily turn off flushing so we aren't writing rows as add them to the queue
            self._flush_count = 0
            with SqliteDict(str(PDQ_DATABASE_FILE), tablename=PDQ_TABLE_NAME, flag="r", autocommit=True) as pdq_db:
                for relationship in pdq_db.values():
                    self._queue.append(relationship)
        finally:
            # turn flushing back on
            self._flush_count = old_flush_count

    def flush_to_client_api(self) -> None:
        if len(self._queue) == 0:
            self.populate_from_db()

        payload = list()

        try:
            while self._queue:
                payload.append(self._queue.pop().to_dict())
                if len(payload) >= self._flush_count:
                    self._client.set_file_relationships(payload)
                    payload.clear()
        except Exception as e:
            for relationship in payload:
                self._queue.append(relationship)
            self.flush_to_db(True)
            raise e

    @staticmethod
    def get_pd_table_key(hash_a: str, hash_b: str) -> str:
        if hash_a > hash_b:
            return str(hash(hash(hash_a) + hash(hash_b)))
        elif hash_a < hash_b:
            return str(hash(hash(hash_b) + hash(hash_a)))
        else:
            raise Exception(
                "hash_a is the same as hash_b, which means we're comparing a file with itself - something's not "
                "right here."
            )
