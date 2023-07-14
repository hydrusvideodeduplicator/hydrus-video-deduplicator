#!/usr/bin/env python
# Copyright (c) Meta Platforms, Inc. and affiliates.


class MatrixUtil:
    @classmethod
    def allocateMatrix(cls, numRows, numCols):
        rv = [[0.0] * numCols for _ in range(numRows)]
        return rv

    @classmethod
    def allocateMatrixAsRowMajorArray(cls, numRows, numCols):
        return [0.0] * numRows * numCols

    @classmethod
    def torben(cls, m, numRows, numCols):
        n = numRows * numCols
        midn = (n + 1) // 2
        min_val = max_val = m[0][0]

        for i in range(numRows):
            for j in range(numCols):
                v = m[i][j]
                if v < min_val:
                    min_val = v
                if v > max_val:
                    max_val = v

        while True:
            guess = (min_val + max_val) / 2
            less = 0
            greater = 0
            equal = 0
            maxltguess = min_val
            mingtguess = max_val

            for i in range(numRows):
                for j in range(numCols):
                    v = m[i][j]
                    if v < guess:
                        less += 1
                        if v > maxltguess:
                            maxltguess = v
                    elif v > guess:
                        greater += 1
                        if v < mingtguess:
                            mingtguess = v
                    else:
                        equal += 1

            if less <= midn and greater <= midn:
                break
            elif less > greater:
                max_val = maxltguess
            else:
                min_val = mingtguess

        if less >= midn:
            return maxltguess
        elif less + equal >= midn:
            return guess
        else:
            return mingtguess
