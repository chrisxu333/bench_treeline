#!/bin/sh

sudo ./build/bench/microbench/bench_treeline --value_size=120 --verbose --ops=80000000 --prefill=40000000 --threads=16 --cache_size_mib=2048 --records_per_page_goal=20 --no_read