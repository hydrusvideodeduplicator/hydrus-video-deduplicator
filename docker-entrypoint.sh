#!/usr/bin/env bash

Command="python -m hydrusvideodeduplicator"
[[ -n "${API_KEY}" ]] && Command="${Command} --api-key='${API_KEY}'"
[[ -n "${API_URL}" ]] && Command="${Command} --api-url='${API_URL}'"
[[ -n "${THRESHOLD}" ]] && Command="${Command} --threshold=${THRESHOLD}"
[[ -n "${JOB_COUNT}" ]] && Command="${Command} --job-count=${JOB_COUNT}"
[[ -n "${FAILED_PAGE_NAME}" ]] && Command="${Command} --failed-page-name=${FAILED_PAGE_NAME}"

[[ ${CERT} = "true" ]] && Command="${Command} --verify-cert=cert"

[[ ${OVERWRITE} = "true" ]] && Command="${Command} --overwrite" || Command="${Command} --no-overwrite"
[[ ${SKIP_HASHING} = "true" ]] && Command="${Command} --skip-hashing" || Command="${Command} --no-skip-hashing"
[[ ${CLEAR_SEARCH_CACHE} = "true" ]] && Command="${Command} --clear-search-cache" || Command="${Command} --no-clear-search-cache"
[[ ${VERBOSE} = "true" ]] && Command="${Command} --verbose" || Command="${Command} --no-verbose"
[[ ${DEBUG} = "true" ]] && Command="${Command} --debug" || Command="${Command} --no-debug"
echo "${Command}"
eval "${Command}"
