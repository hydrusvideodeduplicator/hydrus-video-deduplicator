#!/usr/bin/env bash

Command="python -m hydrusvideodeduplicator"
[[ -n "${API_KEY}" ]] && Command="${Command} --api-key='${API_KEY}'"
[[ -n "${API_URL}" ]] && Command="${Command} --api-url='${API_URL}'"
[[ -n "${THRESHOLD}" ]] && Command="${Command} --threshold=${THRESHOLD}"
[[ -n "${JOB_COUNT}" ]] && Command="${Command} --job-count=${JOB_COUNT}"
[[ -n "${FAILED_PAGE_NAME}" ]] && Command="${Command} --failed-page-name=${FAILED_PAGE_NAME}"
[[ -n "${DEDUP_DATABASE_DIR}" ]] && Command="${Command} --dedup-database-dir=${DEDUP_DATABASE_DIR}" # you probably don't want to do this inside docker...

[[ ${SKIP_HASHING} = "true" ]] && Command="${Command} --skip-hashing" || Command="${Command} --no-skip-hashing"
[[ ${CLEAR_SEARCH_TREE} = "true" ]] && Command="${Command} --clear-search-tree" || Command="${Command} --no-clear-search-tree"
[[ ${CLEAR_SEARCH_CACHE} = "true" ]] && Command="${Command} --clear-search-cache" || Command="${Command} --no-clear-search-cache"
[[ ${VERBOSE} = "true" ]] && Command="${Command} --verbose" || Command="${Command} --no-verbose"
[[ ${DEBUG} = "true" ]] && Command="${Command} --debug" || Command="${Command} --no-debug"
echo "${Command}"
eval "${Command}"
