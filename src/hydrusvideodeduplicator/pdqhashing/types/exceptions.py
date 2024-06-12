# Copyright (c) Meta Platforms, Inc. and affiliates.

from __future__ import annotations


class PDQHashFormatException(Exception):
    def __init__(self, error_message, unacceptableInput=None) -> None:
        super(PDQHashFormatException, self).__init__(error_message)
        self._unacceptableInput = unacceptableInput
