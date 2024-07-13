# Copyright (c) Meta Platforms, Inc. and affiliates.

from __future__ import annotations

from ..pdq_types.exceptions import PDQHashFormatException


def hammingNorm16(h: int):
    return h.bit_count()


class Hash256:
    """256-bit hashes with Hamming distance"""

    # 16 slots of 16 bits each.
    # See hashing/pdq/README-MIH.md in this repo for why not 8x32 or 32x8, etc.
    HASH256_NUM_SLOTS = 16

    HASH256_HEX_NUM_NYBBLES = 4 * HASH256_NUM_SLOTS

    def __init__(self) -> None:
        self.w = [0] * self.HASH256_NUM_SLOTS

    def getNumWords(self):
        return self.HASH256_NUM_SLOTS

    def clone(self):
        rv = Hash256()
        i = 0
        while i < self.HASH256_NUM_SLOTS:
            rv.w[i] = self.w[i]
            i += 1
        return rv

    def __str__(self):
        i = self.HASH256_NUM_SLOTS - 1
        result = []
        while i >= 0:
            result.append("{:04x}".format(self.w[i] & 0xFFFF))
            i = i - 1
        return "".join(result)

    def __repr__(self):
        i = self.HASH256_NUM_SLOTS - 1
        result = []
        while i >= 0:
            result.append("{:04x}".format(self.w[i] & 0xFFFF))
            i = i - 1
        return "".join(result)

    def toHexString(self):
        return self.__str__()

    @classmethod
    def fromHexString(cls, s: str):
        if len(s) != cls.HASH256_HEX_NUM_NYBBLES:
            raise PDQHashFormatException("Incorrect length", s)

        rv = Hash256()
        i = cls.HASH256_NUM_SLOTS
        for x in range(0, len(s), 4):
            try:
                i -= 1
                rv.w[i] = int(s[x : x + 4], 16)
            except ValueError:
                raise PDQHashFormatException("Incorrect format", s)
        return rv

    def clearAll(self):
        for i in range(self.HASH256_NUM_SLOTS):
            self.w[i] = 0

    def setAll(self):
        for i in range(self.HASH256_NUM_SLOTS):
            self.w[i] = 0xFFFF

    def hammingNorm(self):
        n = 0
        i = 0
        while i < self.HASH256_NUM_SLOTS:
            n += hammingNorm16(self.w[i])
            i += 1
        return n

    def hammingDistance(self, that: Hash256):
        n = 0
        for w1, w2 in zip(self.w, that.w):
            n += hammingNorm16(w1 ^ w2)
        return n

    def hammingDistanceLE(self, that: Hash256, d: int | float) -> bool:
        e = 0
        for w1, w2 in zip(self.w, that.w):
            e += hammingNorm16(w1 ^ w2)
            if e > d:
                return False
        return True

    def setBit(self, k: int):
        self.w[(k & 255) >> 4] |= 1 << (k & 15)

    def flipBit(self, k: int):
        self.w[(k & 255) >> 4] ^= 1 << (k & 15)

    def bitwiseXOR(self, that: Hash256):
        rv = Hash256()
        i = 0
        while i < self.HASH256_NUM_SLOTS:
            rv.w[i] = self.w[i] ^ that.w[i]
            i += 1
        return rv

    def bitwiseAND(self, that: Hash256):
        rv = Hash256()
        i = 0
        while i < self.HASH256_NUM_SLOTS:
            rv.w[i] = self.w[i] & that.w[i]
            i += 1
        return rv

    def bitwiseOR(self, that: Hash256):
        rv = Hash256()
        i = 0
        while i < self.HASH256_NUM_SLOTS:
            rv.w[i] = self.w[i] | that.w[i]
            i += 1
        return rv

    def bitwiseNOT(self):
        rv = Hash256()
        i = 0
        while i < self.HASH256_NUM_SLOTS:
            rv.w[i] = (~self.w[i]) & 0xFFFF
            i += 1
        return rv

    def dumpBits(self):
        i = self.HASH256_NUM_SLOTS - 1
        str = []
        while i >= 0:
            word = self.w[i] & 0xFFFF
            j = 15
            bits = []
            while j >= 0:
                if (word & (1 << j)) != 0:
                    bits.append("1")
                else:
                    bits.append("0")
                j -= 1
            str.append(" ".join(bits))
            i -= 1
        return "\n".join(str)

    def dumpBitsAcross(self):
        i = self.HASH256_NUM_SLOTS - 1
        str = []
        while i >= 0:
            word = self.w[i] & 0xFFFF
            j = 15
            while j >= 0:
                if (word & (1 << j)) != 0:
                    str.append("1")
                else:
                    str.append("0")
                j -= 1
            i -= 1
        return " ".join(str)

    def dumpWords(self):
        return ",".join(str(v) for v in list(reversed(self.w)))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (Hash256,)):
            for i in range(self.HASH256_NUM_SLOTS):
                if self.w[i] != other.w[i]:
                    return False
            return True
        else:
            return False

    def __gt__(self, other: Hash256) -> bool:
        for i in range(self.HASH256_NUM_SLOTS):
            if self.w[i] > other.w[i]:
                return True
            elif self.w[i] < other.w[i]:
                return False
        return False

    def __lt__(self, other: Hash256) -> bool:
        for i in range(self.HASH256_NUM_SLOTS):
            if self.w[i] < other.w[i]:
                return True
            elif self.w[i] > other.w[i]:
                return False
        return False
