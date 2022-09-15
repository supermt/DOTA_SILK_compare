//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#ifdef GFLAGS
#ifdef NUMA
#include <numa.h>
#include <numaif.h>
#endif
#ifndef OS_WIN
#include <unistd.h>
#endif
#include <fcntl.h>
#include <gflags/gflags.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <queue>
#include <ctime>
#include <ratio>
#include <chrono>
#include <unistd.h>

#include "db/db_impl.h"
#include "db/version_set.h"
#include "hdfs/env_hdfs.h"
#include "monitoring/histogram.h"
#include "monitoring/statistics.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/sim_cache.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/write_batch.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "util/testutil.h"
#include "util/transaction_test_util.h"
#include "util/xxhash.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/merge_operators.h"
#include "utilities/persistent_cache/block_cache_tier.h"
#include "util/zipf.h"
#include "util/latest-generator.h"

#include "ycsbcore/client.h"
#include "ycsbcore/core_workload.h"
#include "ycsbcore/countdown_latch.h"
#include "ycsbcore/db_factory.h"
#include "ycsbcore/measurements.h"
#include "ycsbcore/timer.h"
#include "ycsbcore/utils.h"

#ifdef OS_WIN
#include <io.h>  // open/close
#endif

using GFLAGS::ParseCommandLineFlags;
using GFLAGS::RegisterFlagValidator;
using GFLAGS::SetUsageMessage;

DEFINE_string(
    benchmarks,
    "fillseq,"
    "fillseqdeterministic,"
    "fillsync,"
    "fillrandom,"
    "filluniquerandomdeterministic,"
    "overwrite,"
    "readrandom,"
    "newiterator,"
    "newiteratorwhilewriting,"
    "seekrandom,"
    "seekrandomwhilewriting,"
    "seekrandomwhilemerging,"
    "readseq,"
    "readreverse,"
    "compact,"
    "compactall,"
    "readrandom,"
    "multireadrandom,"
    "readseq,"
    "readtocache,"
    "readreverse,"
    "readwhilewriting,"
    "readwhilemerging,"
    "readrandomwriterandom,"
    "readrandomwriterandomdifferentvaluesizes,"
    "readrandomwriterandomsplitrange,"
    "readrandomwriterandomsplitrangedifferentvaluesizes,"
    "readrandomwriterandomskewed,"
    "readrandomwriterandomsplitrangeskewed,"
    "differentvaluesizesperthread,"
    "differentvaluesizesperthreadcomparison,"
    "longpeak,"
    "testzipf,"
    "ycsb,"
    "testlatestgenerator,"
    "updaterandom,"
    "randomwithverify,"
    "readwriteskewedworkload,"
    "fill100K,"
    "crc32c,"
    "xxhash,"
    "compress,"
    "uncompress,"
    "acquireload,"
    "fillseekseq,"
    "randomtransaction,"
    "randomreplacekeys,"
    "timeseries",

    "Comma-separated list of operations to run in the specified"
    " order. Available benchmarks:\n"
    "\tfillseq       -- write N values in sequential key"
    " order in async mode\n"
    "\tfillseqdeterministic       -- write N values in the specified"
    " key order and keep the shape of the LSM tree\n"
    "\tfillrandom    -- write N values in random key order in async"
    " mode\n"
    "\tfilluniquerandomdeterministic       -- write N values in a random"
    " key order and keep the shape of the LSM tree\n"
    "\toverwrite     -- overwrite N values in random key order in"
    " async mode\n"
    "\tfillsync      -- write N/100 values in random key order in "
    "sync mode\n"
    "\tfill100K      -- write N/1000 100K values in random order in"
    " async mode\n"
    "\tdeleteseq     -- delete N keys in sequential order\n"
    "\tdeleterandom  -- delete N keys in random order\n"
    "\treadseq       -- read N times sequentially\n"
    "\treadtocache   -- 1 thread reading database sequentially\n"
    "\treadreverse   -- read N times in reverse order\n"
    "\treadrandom    -- read N times in random order\n"
    "\treadmissing   -- read N missing keys in random order\n"
    "\treadwhilewriting      -- 1 writer, N threads doing random "
    "reads\n"
    "\treadwhilemerging      -- 1 merger, N threads doing random "
    "reads\n"
    "\treadrandomwriterandom -- N threads doing random-read, "
    "random-write\n"
    "\tprefixscanrandom      -- prefix scan N times in random order\n"
    "\tupdaterandom  -- N threads doing read-modify-write for random "
    "keys\n"
    "\tappendrandom  -- N threads doing read-modify-write with "
    "growing values\n"
    "\tmergerandom   -- same as updaterandom/appendrandom using merge"
    " operator. "
    "Must be used with merge_operator\n"
    "\treadrandommergerandom -- perform N random read-or-merge "
    "operations. Must be used with merge_operator\n"
    "\tnewiterator   -- repeated iterator creation\n"
    "\tseekrandom    -- N random seeks, call Next seek_nexts times "
    "per seek\n"
    "\tseekrandomwhilewriting -- seekrandom and 1 thread doing "
    "overwrite\n"
    "\tseekrandomwhilemerging -- seekrandom and 1 thread doing "
    "merge\n"
    "\tcrc32c        -- repeated crc32c of 4K of data\n"
    "\txxhash        -- repeated xxHash of 4K of data\n"
    "\tacquireload   -- load N*1000 times\n"
    "\tfillseekseq   -- write N values in sequential key, then read "
    "them by seeking to each key\n"
    "\trandomtransaction     -- execute N random transactions and "
    "verify correctness\n"
    "\trandomreplacekeys     -- randomly replaces N keys by deleting "
    "the old version and putting the new version\n\n"
    "\ttimeseries            -- 1 writer generates time series data "
    "and multiple readers doing random reads on id\n\n"
    "Meta operations:\n"
    "\tcompact     -- Compact the entire DB; If multiple, randomly choose one\n"
    "\tcompactall  -- Compact the entire DB\n"
    "\tstats       -- Print DB stats\n"
    "\tresetstats  -- Reset DB stats\n"
    "\tlevelstats  -- Print the number of files and bytes per level\n"
    "\tsstables    -- Print sstable info\n"
    "\theapprofile -- Dump a heap profile (if supported by this"
    " port)\n");

DEFINE_int64(prob, 9900, " the threshold for the prob variable");

DEFINE_int64(num, 1000000, "Number of key/values to place in database");

DEFINE_int64(numdistinct, 1000,
             "Number of distinct keys to use. Used in RandomWithVerify to "
             "read/write on fewer keys so that gets are more likely to find the"
             " key and puts are more likely to update the same key");

DEFINE_int64(datasize, 1000,
             "Number of distinct keys to use. Used in ReadWriteSkewedWorkload to "
             "indicate database size");


DEFINE_int64(merge_keys, -1,
             "Number of distinct keys to use for MergeRandom and "
             "ReadRandomMergeRandom. "
             "If negative, there will be FLAGS_num keys.");
DEFINE_int32(num_column_families, 1, "Number of Column Families to use.");

DEFINE_int32(
    num_hot_column_families, 0,
    "Number of Hot Column Families. If more than 0, only write to this "
    "number of column families. After finishing all the writes to them, "
    "create new set of column families and insert to them. Only used "
    "when num_column_families > 1.");

DEFINE_int64(reads, -1, "Number of read operations to do.  "
             "If negative, do FLAGS_num reads.");

DEFINE_int64(deletes, -1, "Number of delete operations to do.  "
             "If negative, do FLAGS_num deletions.");

DEFINE_int32(bloom_locality, 0, "Control bloom filter probes locality");

DEFINE_int64(seed, 0, "Seed base for random number generators. "
             "When 0 it is deterministic.");

DEFINE_int32(threads, 1, "Number of concurrent threads to run.");

DEFINE_int32(duration, 0, "Time in seconds for the random-ops tests to run."
             " When 0 then num & reads determine the test duration");

DEFINE_int32(value_size, 100, "Size of each value");

DEFINE_int32(seek_nexts, 0,
             "How many times to call Next() after Seek() in "
             "fillseekseq, seekrandom, seekrandomwhilewriting and "
             "seekrandomwhilemerging");

DEFINE_bool(reverse_iterator, false,
            "When true use Prev rather than Next for iterators that do "
            "Seek and then Next");

DEFINE_bool(use_uint64_comparator, false, "use Uint64 user comparator");

DEFINE_bool(autotuned_rate_limiter, false, "Are we using RocksDB autotuned rate limiter or not");

DEFINE_bool(dynamic_compaction_rate, false, "dynamically adjust compaction and flushing rate; used in FluctuatingThroughput tests");

DEFINE_int64(batch_size, 1, "Batch size");

static bool ValidateKeySize(const char* flagname, int32_t value) {
  return true;
}

static bool ValidateUint32Range(const char* flagname, uint64_t value) {
  if (value > std::numeric_limits<uint32_t>::max()) {
    fprintf(stderr, "Invalid value for --%s: %lu, overflow\n", flagname,
            (unsigned long)value);
    return false;
  }
  return true;
}

DEFINE_int32(key_size, 16, "size of each key");

DEFINE_int32(num_multi_db, 0,
             "Number of DBs used in the benchmark. 0 means single DB.");

DEFINE_double(compression_ratio, 0.5, "Arrange to generate values that shrink"
              " to this fraction of their original size after compression");

DEFINE_double(read_random_exp_range, 0.0,
              "Read random's key will be generated using distribution of "
              "num * exp(-r) where r is uniform number from 0 to this value. "
              "The larger the number is, the more skewed the reads are. "
              "Only used in readrandom and multireadrandom benchmarks.");

DEFINE_bool(histogram, false, "Print histogram of operation timings");

DEFINE_bool(enable_numa, false,
            "Make operations aware of NUMA architecture and bind memory "
            "and cpus corresponding to nodes together. In NUMA, memory "
            "in same node as CPUs are closer when compared to memory in "
            "other nodes. Reads can be faster when the process is bound to "
            "CPU and memory of same node. Use \"$numactl --hardware\" command "
            "to see NUMA memory architecture.");

DEFINE_int64(db_write_buffer_size, rocksdb::Options().db_write_buffer_size,
             "Number of bytes to buffer in all memtables before compacting");

DEFINE_bool(cost_write_buffer_to_cache, false,
            "The usage of memtable is costed to the block cache");

DEFINE_int64(write_buffer_size, rocksdb::Options().write_buffer_size,
             "Number of bytes to buffer in memtable before compacting");

DEFINE_int32(max_write_buffer_number,
             rocksdb::Options().max_write_buffer_number,
             "The number of in-memory memtables. Each memtable is of size"
             "write_buffer_size.");

DEFINE_int32(min_write_buffer_number_to_merge,
             rocksdb::Options().min_write_buffer_number_to_merge,
             "The minimum number of write buffers that will be merged together"
             "before writing to storage. This is cheap because it is an"
             "in-memory merge. If this feature is not enabled, then all these"
             "write buffers are flushed to L0 as separate files and this "
             "increases read amplification because a get request has to check"
             " in all of these files. Also, an in-memory merge may result in"
             " writing less data to storage if there are duplicate records "
             " in each of these individual write buffers.");

DEFINE_int32(max_write_buffer_number_to_maintain,
             rocksdb::Options().max_write_buffer_number_to_maintain,
             "The total maximum number of write buffers to maintain in memory "
             "including copies of buffers that have already been flushed. "
             "Unlike max_write_buffer_number, this parameter does not affect "
             "flushing. This controls the minimum amount of write history "
             "that will be available in memory for conflict checking when "
             "Transactions are used. If this value is too low, some "
             "transactions may fail at commit time due to not being able to "
             "determine whether there were any write conflicts. Setting this "
             "value to 0 will cause write buffers to be freed immediately "
             "after they are flushed.  If this value is set to -1, "
             "'max_write_buffer_number' will be used.");

DEFINE_int32(max_background_jobs,
             rocksdb::Options().max_background_jobs,
             "The maximum number of concurrent background jobs that can occur "
             "in parallel.");

DEFINE_int32(max_background_compactions,
             rocksdb::Options().max_background_compactions,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_int32(base_background_compactions, -1, "DEPRECATED");

DEFINE_uint64(subcompactions, 1,
              "Maximum number of subcompactions to divide L0-L1 compactions "
              "into.");
static const bool FLAGS_subcompactions_dummy
    __attribute__((unused)) = RegisterFlagValidator(&FLAGS_subcompactions,
                                                    &ValidateUint32Range);

DEFINE_int32(max_background_flushes,
             rocksdb::Options().max_background_flushes,
             "The maximum number of concurrent background flushes"
             " that can occur in parallel.");

static rocksdb::CompactionStyle FLAGS_compaction_style_e;
DEFINE_int32(compaction_style, (int32_t) rocksdb::Options().compaction_style,
             "style of compaction: level-based, universal and fifo");

static rocksdb::CompactionPri FLAGS_compaction_pri_e;
DEFINE_int32(compaction_pri, (int32_t)rocksdb::Options().compaction_pri,
             "priority of files to compaction: by size or by data age");

DEFINE_int32(universal_size_ratio, 0,
             "Percentage flexibility while comparing file size"
             " (for universal compaction only).");

DEFINE_int32(universal_min_merge_width, 0, "The minimum number of files in a"
             " single compaction run (for universal compaction only).");

DEFINE_int32(universal_max_merge_width, 0, "The max number of files to compact"
             " in universal style compaction");

DEFINE_int32(universal_max_size_amplification_percent, 0,
             "The max size amplification for universal style compaction");

DEFINE_int32(universal_compression_size_percent, -1,
             "The percentage of the database to compress for universal "
             "compaction. -1 means compress everything.");

DEFINE_bool(universal_allow_trivial_move, false,
            "Allow trivial move in universal compaction.");

DEFINE_int64(cache_size, 8 << 20,  // 8MB
             "Number of bytes to use as a cache of uncompressed data");

DEFINE_int32(cache_numshardbits, 6,
             "Number of shards for the block cache"
             " is 2 ** cache_numshardbits. Negative means use default settings."
             " This is applied only if FLAGS_cache_size is non-negative.");

DEFINE_double(cache_high_pri_pool_ratio, 0.0,
              "Ratio of block cache reserve for high pri blocks. "
              "If > 0.0, we also enable "
              "cache_index_and_filter_blocks_with_high_priority.");

DEFINE_bool(use_clock_cache, false,
            "Replace default LRU block cache with clock cache.");

DEFINE_int64(simcache_size, -1,
             "Number of bytes to use as a simcache of "
             "uncompressed data. Nagative value disables simcache.");

DEFINE_bool(cache_index_and_filter_blocks, false,
            "Cache index/filter blocks in block cache.");

DEFINE_bool(partition_index_and_filters, false,
            "Partition index and filter blocks.");

DEFINE_int64(metadata_block_size,
             rocksdb::BlockBasedTableOptions().metadata_block_size,
             "Max partition size when partitioning index/filters");

// The default reduces the overhead of reading time with flash. With HDD, which
// offers much less throughput, however, this number better to be set to 1.
DEFINE_int32(ops_between_duration_checks, 1000,
             "Check duration limit every x ops");

DEFINE_bool(pin_l0_filter_and_index_blocks_in_cache, false,
            "Pin index/filter blocks of L0 files in block cache.");

DEFINE_int32(block_size,
             static_cast<int32_t>(rocksdb::BlockBasedTableOptions().block_size),
             "Number of bytes in a block.");

DEFINE_int32(block_restart_interval,
             rocksdb::BlockBasedTableOptions().block_restart_interval,
             "Number of keys between restart points "
             "for delta encoding of keys in data block.");

DEFINE_int32(index_block_restart_interval,
             rocksdb::BlockBasedTableOptions().index_block_restart_interval,
             "Number of keys between restart points "
             "for delta encoding of keys in index block.");

DEFINE_int32(read_amp_bytes_per_bit,
             rocksdb::BlockBasedTableOptions().read_amp_bytes_per_bit,
             "Number of bytes per bit to be used in block read-amp bitmap");

DEFINE_int64(compressed_cache_size, -1,
             "Number of bytes to use as a cache of compressed data.");

DEFINE_int64(row_cache_size, 0,
             "Number of bytes to use as a cache of individual rows"
             " (0 = disabled).");

DEFINE_int32(open_files, rocksdb::Options().max_open_files,
             "Maximum number of files to keep open at the same time"
             " (use default if == 0)");

DEFINE_int32(file_opening_threads, rocksdb::Options().max_file_opening_threads,
             "If open_files is set to -1, this option set the number of "
             "threads that will be used to open files during DB::Open()");

DEFINE_int32(new_table_reader_for_compaction_inputs, true,
             "If true, uses a separate file handle for compaction inputs");

DEFINE_int32(compaction_readahead_size, 0, "Compaction readahead size");

DEFINE_int32(random_access_max_buffer_size, 1024 * 1024,
             "Maximum windows randomaccess buffer size");

DEFINE_int32(writable_file_max_buffer_size, 1024 * 1024,
             "Maximum write buffer for Writable File");

DEFINE_int32(bloom_bits, -1, "Bloom filter bits per key. Negative means"
             " use default settings.");
DEFINE_double(memtable_bloom_size_ratio, 0,
              "Ratio of memtable size used for bloom filter. 0 means no bloom "
              "filter.");
DEFINE_bool(memtable_use_huge_page, false,
            "Try to use huge page in memtables.");

DEFINE_bool(use_existing_db, false, "If true, do not destroy the existing"
            " database.  If you set this flag and also specify a benchmark that"
            " wants a fresh database, that benchmark will fail.");

DEFINE_bool(show_table_properties, false,
            "If true, then per-level table"
            " properties will be printed on every stats-interval when"
            " stats_interval is set and stats_per_interval is on.");

DEFINE_string(db, "", "Use the db with the following name.");

// Read cache flags

DEFINE_string(read_cache_path, "",
              "If not empty string, a read cache will be used in this path");

DEFINE_int64(read_cache_size, 4LL * 1024 * 1024 * 1024,
             "Maximum size of the read cache");

DEFINE_bool(read_cache_direct_write, true,
            "Whether to use Direct IO for writing to the read cache");

DEFINE_bool(read_cache_direct_read, true,
            "Whether to use Direct IO for reading from read cache");

static bool ValidateCacheNumshardbits(const char* flagname, int32_t value) {
  if (value >= 20) {
    fprintf(stderr, "Invalid value for --%s: %d, must be < 20\n",
            flagname, value);
    return false;
  }
  return true;
}

DEFINE_bool(verify_checksum, true,
            "Verify checksum for every block read"
            " from storage");

DEFINE_bool(statistics, false, "Database statistics");
DEFINE_string(statistics_string, "", "Serialized statistics string");
static class std::shared_ptr<rocksdb::Statistics> dbstats;

DEFINE_int64(writes, -1, "Number of write operations to do. If negative, do"
             " --num reads.");

DEFINE_bool(finish_after_writes, false, "Write thread terminates after all writes are finished");

DEFINE_bool(sync, false, "Sync all writes to disk");

DEFINE_bool(use_fsync, false, "If true, issue fsync instead of fdatasync");

DEFINE_bool(disable_wal, false, "If true, do not write WAL for write.");

DEFINE_string(wal_dir, "", "If not empty, use the given dir for WAL");

DEFINE_string(truth_db, "/dev/shm/truth_db/dbbench",
              "Truth key/values used when using verify");

DEFINE_int32(num_levels, 7, "The total number of levels");

DEFINE_int64(target_file_size_base, rocksdb::Options().target_file_size_base,
             "Target file size at level-1");

DEFINE_int32(target_file_size_multiplier,
             rocksdb::Options().target_file_size_multiplier,
             "A multiplier to compute target level-N file size (N >= 2)");

DEFINE_uint64(max_bytes_for_level_base,
              rocksdb::Options().max_bytes_for_level_base,
              "Max bytes for level-1");

DEFINE_bool(level_compaction_dynamic_level_bytes, false,
            "Whether level size base is dynamic");

DEFINE_double(max_bytes_for_level_multiplier, 10,
              "A multiplier to compute max bytes for level-N (N >= 2)");

static std::vector<int> FLAGS_max_bytes_for_level_multiplier_additional_v;
DEFINE_string(max_bytes_for_level_multiplier_additional, "",
              "A vector that specifies additional fanout per level");

DEFINE_int32(level0_stop_writes_trigger,
             rocksdb::Options().level0_stop_writes_trigger,
             "Number of files in level-0"
             " that will trigger put stop.");

DEFINE_int32(level0_slowdown_writes_trigger,
             rocksdb::Options().level0_slowdown_writes_trigger,
             "Number of files in level-0"
             " that will slow down writes.");

DEFINE_int32(level0_file_num_compaction_trigger,
             rocksdb::Options().level0_file_num_compaction_trigger,
             "Number of files in level-0"
             " when compactions start");

static bool ValidateInt32Percent(const char* flagname, int32_t value) {
  if (value <= 0 || value>=100) {
    fprintf(stderr, "Invalid value for --%s: %d, 0< pct <100 \n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_int32(readwritepercent, 90, "Ratio of reads to reads/writes (expressed"
             " as percentage) for the ReadRandomWriteRandom workload. The "
             "default value 90 means 90% operations out of all reads and writes"
             " operations are reads. In other words, 9 gets for every 1 put.");

DEFINE_int32(mergereadpercent, 70, "Ratio of merges to merges&reads (expressed"
             " as percentage) for the ReadRandomMergeRandom workload. The"
             " default value 70 means 70% out of all read and merge operations"
             " are merges. In other words, 7 merges for every 3 gets.");

DEFINE_int32(deletepercent, 2, "Percentage of deletes out of reads/writes/"
             "deletes (used in RandomWithVerify only). RandomWithVerify "
             "calculates writepercent as (100 - FLAGS_readwritepercent - "
             "deletepercent), so deletepercent must be smaller than (100 - "
             "FLAGS_readwritepercent)");

DEFINE_bool(optimize_filters_for_hits, false,
            "Optimizes bloom filters for workloads for most lookups return "
            "a value. For now this doesn't create bloom filters for the max "
            "level of the LSM to reduce metadata that should fit in RAM. ");

DEFINE_uint64(delete_obsolete_files_period_micros, 0,
              "Ignored. Left here for backward compatibility");

DEFINE_int64(writes_per_range_tombstone, 0,
             "Number of writes between range "
             "tombstones");

DEFINE_int64(range_tombstone_width, 100, "Number of keys in tombstone's range");

DEFINE_int64(max_num_range_tombstones, 0,
             "Maximum number of range tombstones "
             "to insert.");

DEFINE_bool(expand_range_tombstones, false,
            "Expand range tombstone into sequential regular tombstones.");
uint64_t kMicrosInSecond = 1000l * 1000l;
DEFINE_int64(load_duration, 0, "The loading duration of YCSB");
DEFINE_string(ycsb_workload, "", "The workload of YCSB");
DEFINE_int64(ycsb_request_speed, 100, "The request speed of YCSB, in MB/s");
DEFINE_int64(load_num, 100000, "Num of operations in loading phrase");
DEFINE_int64(running_num, 100000, "Num of operations in running phrase");
DEFINE_int64(core_num, 20, "The limit of thread number");
DEFINE_int64(max_memtable_size, rocksdb::Options().max_memtable_size,
             "The size of Max batch size");
DEFINE_bool(DOTA_enable, false, "Whether trigger the DOTA framework");
DEFINE_bool(FEA_enable, false, "Trigger FEAT tuner's FEA component");
DEFINE_bool(TEA_enable, false, "Trigger FEAT tuner's TEA component");
DEFINE_int32(SILK_bandwidth_limitation, 200, "MBPS of disk limitation");
DEFINE_bool(SILK_triggered, false, "Whether the SILK tuner is triggered");
DEFINE_double(idle_rate, 1.25,
              "TEA will decide this as the idle rate of the threads");
DEFINE_double(FEA_gap_threshold, 1.5,
              "The negative feedback loop's threshold");
DEFINE_double(TEA_slow_flush, 0.5, "The negative feedback loop's threshold");
DEFINE_double(DOTA_tuning_gap, 1.0, "Tuning gap of the DOTA agent, in secs ");
DEFINE_int64(random_fill_average, 150,
             "average inputs rate of background write operations");
DEFINE_bool(detailed_running_stats, false,
            "Whether record more detailed information in report agent");


#ifndef ROCKSDB_LITE
DEFINE_bool(optimistic_transaction_db, false,
            "Open a OptimisticTransactionDB instance. "
            "Required for randomtransaction benchmark.");

DEFINE_bool(use_blob_db, false,
            "Open a BlobDB instance. "
            "Required for largevalue benchmark.");

DEFINE_bool(transaction_db, false,
            "Open a TransactionDB instance. "
            "Required for randomtransaction benchmark.");

DEFINE_uint64(transaction_sets, 2,
              "Number of keys each transaction will "
              "modify (use in RandomTransaction only).  Max: 9999");

DEFINE_bool(transaction_set_snapshot, false,
            "Setting to true will have each transaction call SetSnapshot()"
            " upon creation.");

DEFINE_int32(transaction_sleep, 0,
             "Max microseconds to sleep in between "
             "reading and writing a value (used in RandomTransaction only). ");

DEFINE_uint64(transaction_lock_timeout, 100,
              "If using a transaction_db, specifies the lock wait timeout in"
              " milliseconds before failing a transaction waiting on a lock");
DEFINE_string(
    options_file, "",
    "The path to a RocksDB options file.  If specified, then db_bench will "
    "run with the RocksDB options in the default column family of the "
    "specified options file. "
    "Note that with this setting, db_bench will ONLY accept the following "
    "RocksDB options related command-line arguments, all other arguments "
    "that are related to RocksDB options will be ignored:\n"
    "\t--use_existing_db\n"
    "\t--statistics\n"
    "\t--row_cache_size\n"
    "\t--row_cache_numshardbits\n"
    "\t--enable_io_prio\n"
    "\t--dump_malloc_stats\n"
    "\t--num_multi_db\n");

DEFINE_uint64(fifo_compaction_max_table_files_size_mb, 0,
              "The limit of total table file sizes to trigger FIFO compaction");
DEFINE_bool(fifo_compaction_allow_compaction, true,
            "Allow compaction in FIFO compaction.");
DEFINE_uint64(fifo_compaction_ttl, 0, "TTL for the SST Files in seconds.");
#endif  // ROCKSDB_LITE

DEFINE_bool(report_bg_io_stats, false,
            "Measure times spents on I/Os while in compactions. ");

DEFINE_bool(use_stderr_info_logger, false,
            "Write info logs to stderr instead of to LOG file. ");

DEFINE_bool(YCSB_uniform_distribution, false, "Uniform key distribution for YCSB");

static enum rocksdb::CompressionType StringToCompressionType(const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "none"))
    return rocksdb::kNoCompression;
  else if (!strcasecmp(ctype, "snappy"))
    return rocksdb::kSnappyCompression;
  else if (!strcasecmp(ctype, "zlib"))
    return rocksdb::kZlibCompression;
  else if (!strcasecmp(ctype, "bzip2"))
    return rocksdb::kBZip2Compression;
  else if (!strcasecmp(ctype, "lz4"))
    return rocksdb::kLZ4Compression;
  else if (!strcasecmp(ctype, "lz4hc"))
    return rocksdb::kLZ4HCCompression;
  else if (!strcasecmp(ctype, "xpress"))
    return rocksdb::kXpressCompression;
  else if (!strcasecmp(ctype, "zstd"))
    return rocksdb::kZSTD;

  fprintf(stdout, "Cannot parse compression type '%s'\n", ctype);
  return rocksdb::kSnappyCompression;  // default value
}

static std::string ColumnFamilyName(size_t i) {
  if (i == 0) {
    return rocksdb::kDefaultColumnFamilyName;
  } else {
    char name[100];
    snprintf(name, sizeof(name), "column_family_name_%06zu", i);
    return std::string(name);
  }
}

DEFINE_string(compression_type, "snappy",
              "Algorithm to use to compress the database");
static enum rocksdb::CompressionType FLAGS_compression_type_e =
    rocksdb::kSnappyCompression;

DEFINE_int32(compression_level, -1,
             "Compression level. For zlib this should be -1 for the "
             "default level, or between 0 and 9.");

DEFINE_int32(compression_max_dict_bytes, 0,
             "Maximum size of dictionary used to prime the compression "
             "library.");

static bool ValidateCompressionLevel(const char* flagname, int32_t value) {
  if (value < -1 || value > 9) {
    fprintf(stderr, "Invalid value for --%s: %d, must be between -1 and 9\n",
            flagname, value);
    return false;
  }
  return true;
}

static const bool FLAGS_compression_level_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_compression_level, &ValidateCompressionLevel);

DEFINE_int32(min_level_to_compress, -1, "If non-negative, compression starts"
             " from this level. Levels with number < min_level_to_compress are"
             " not compressed. Otherwise, apply compression_type to "
             "all levels.");

static bool ValidateTableCacheNumshardbits(const char* flagname,
                                           int32_t value) {
  if (0 >= value || value > 20) {
    fprintf(stderr, "Invalid value for --%s: %d, must be  0 < val <= 20\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_int32(table_cache_numshardbits, 4, "");

#ifndef ROCKSDB_LITE
DEFINE_string(env_uri, "", "URI for registry Env lookup. Mutually exclusive"
              " with --hdfs.");
#endif  // ROCKSDB_LITE
DEFINE_string(hdfs, "", "Name of hdfs environment. Mutually exclusive with"
              " --env_uri.");
static rocksdb::Env* FLAGS_env = rocksdb::Env::Default();

DEFINE_int64(stats_interval, 0, "Stats are reported every N operations when "
             "this is greater than zero. When 0 the interval grows over time.");

DEFINE_int64(stats_interval_seconds, 0, "Report stats every N seconds. This "
             "overrides stats_interval when both are > 0.");

DEFINE_int32(stats_per_interval, 0, "Reports additional stats per interval when"
             " this is greater than 0.");

DEFINE_int64(report_interval_seconds, 0,
             "If greater than zero, it will write simple stats in CVS format "
             "to --report_file every N seconds");

DEFINE_string(report_file, "report.csv",
              "Filename where some simple stats are reported to (if "
              "--report_interval_seconds is bigger than 0)");

DEFINE_int32(thread_status_per_interval, 0,
             "Takes and report a snapshot of the current status of each thread"
             " when this is greater than 0.");

DEFINE_int32(perf_level, rocksdb::PerfLevel::kDisable, "Level of perf collection");

static bool ValidateRateLimit(const char* flagname, double value) {
  const double EPSILON = 1e-10;
  if ( value < -EPSILON ) {
    fprintf(stderr, "Invalid value for --%s: %12.6f, must be >= 0.0\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_double(soft_rate_limit, 0.0, "DEPRECATED");

DEFINE_double(hard_rate_limit, 0.0, "DEPRECATED");

DEFINE_uint64(soft_pending_compaction_bytes_limit, 64ull * 1024 * 1024 * 1024,
              "Slowdown writes if pending compaction bytes exceed this number");

DEFINE_uint64(hard_pending_compaction_bytes_limit, 128ull * 1024 * 1024 * 1024,
              "Stop writes if pending compaction bytes exceed this number");

DEFINE_uint64(delayed_write_rate, 8388608u,
              "Limited bytes allowed to DB when soft_rate_limit or "
              "level0_slowdown_writes_trigger triggers");

DEFINE_bool(enable_pipelined_write, true,
            "Allow WAL and memtable writes to be pipelined");

DEFINE_bool(allow_concurrent_memtable_write, false,
            "Allow multi-writers to update mem tables in parallel.");

DEFINE_bool(enable_write_thread_adaptive_yield, true,
            "Use a yielding spin loop for brief writer thread waits.");

DEFINE_uint64(
    write_thread_max_yield_usec, 100,
    "Maximum microseconds for enable_write_thread_adaptive_yield operation.");

DEFINE_uint64(write_thread_slow_yield_usec, 3,
              "The threshold at which a slow yield is considered a signal that "
              "other processes or threads want the core.");

DEFINE_int32(rate_limit_delay_max_milliseconds, 1000,
             "When hard_rate_limit is set then this is the max time a put will"
             " be stalled.");

DEFINE_int32(SILK_bandwidth_check_interval, 10000,
             "How  much time to wait between sampling the bandwidth and adjusting compaction rate in SILK");

DEFINE_uint64(rate_limiter_bytes_per_sec, 0, "Set options.rate_limiter value.");

DEFINE_bool(rate_limit_bg_reads, false,
            "Use options.rate_limiter on compaction reads");

DEFINE_uint64(
    benchmark_write_rate_limit, 0,
    "If non-zero, db_bench will rate-limit the writes going into RocksDB. This "
    "is the global rate in bytes/second.");

DEFINE_uint64(
    benchmark_read_rate_limit, 0,
    "If non-zero, db_bench will rate-limit the reads from RocksDB. This "
    "is the global rate in ops/second.");

DEFINE_uint64(max_compaction_bytes, rocksdb::Options().max_compaction_bytes,
              "Max bytes allowed in one compaction");

#ifndef ROCKSDB_LITE
DEFINE_bool(readonly, false, "Run read only benchmarks.");
#endif  // ROCKSDB_LITE

DEFINE_bool(disable_auto_compactions, false, "Do not auto trigger compactions");

DEFINE_uint64(wal_ttl_seconds, 0, "Set the TTL for the WAL Files in seconds.");
DEFINE_uint64(wal_size_limit_MB, 0, "Set the size limit for the WAL Files"
              " in MB.");
DEFINE_uint64(max_total_wal_size, 0, "Set total max WAL size");

DEFINE_bool(mmap_read, rocksdb::Options().allow_mmap_reads,
            "Allow reads to occur via mmap-ing files");

DEFINE_bool(mmap_write, rocksdb::Options().allow_mmap_writes,
            "Allow writes to occur via mmap-ing files");

DEFINE_bool(use_direct_reads, rocksdb::Options().use_direct_reads,
            "Use O_DIRECT for reading data");

DEFINE_bool(use_direct_io_for_flush_and_compaction,
            rocksdb::Options().use_direct_io_for_flush_and_compaction,
            "Use O_DIRECT for background flush and compaction I/O");

DEFINE_bool(advise_random_on_open, rocksdb::Options().advise_random_on_open,
            "Advise random access on table file open");

DEFINE_string(compaction_fadvice, "NORMAL",
              "Access pattern advice when a file is compacted");
static auto FLAGS_compaction_fadvice_e =
  rocksdb::Options().access_hint_on_compaction_start;

DEFINE_bool(use_tailing_iterator, false,
            "Use tailing iterator to access a series of keys instead of get");

DEFINE_bool(use_adaptive_mutex, rocksdb::Options().use_adaptive_mutex,
            "Use adaptive mutex");

DEFINE_uint64(bytes_per_sync,  rocksdb::Options().bytes_per_sync,
              "Allows OS to incrementally sync SST files to disk while they are"
              " being written, in the background. Issue one request for every"
              " bytes_per_sync written. 0 turns it off.");

DEFINE_uint64(wal_bytes_per_sync,  rocksdb::Options().wal_bytes_per_sync,
              "Allows OS to incrementally sync WAL files to disk while they are"
              " being written, in the background. Issue one request for every"
              " wal_bytes_per_sync written. 0 turns it off.");

DEFINE_bool(use_single_deletes, true,
            "Use single deletes (used in RandomReplaceKeys only).");

DEFINE_double(stddev, 2000.0,
              "Standard deviation of normal distribution used for picking keys"
              " (used in RandomReplaceKeys only).");

DEFINE_int32(key_id_range, 100000,
             "Range of possible value of key id (used in TimeSeries only).");

DEFINE_string(expire_style, "none",
              "Style to remove expired time entries. Can be one of the options "
              "below: none (do not expired data), compaction_filter (use a "
              "compaction filter to remove expired data), delete (seek IDs and "
              "remove expired data) (used in TimeSeries only).");

DEFINE_uint64(
    time_range, 100000,
    "Range of timestamp that store in the database (used in TimeSeries"
    " only).");

DEFINE_int32(num_deletion_threads, 1,
             "Number of threads to do deletion (used in TimeSeries and delete "
             "expire_style only).");

DEFINE_int32(max_successive_merges, 0, "Maximum number of successive merge"
             " operations on a key in the memtable");

static bool ValidatePrefixSize(const char* flagname, int32_t value) {
  if (value < 0 || value>=2000000000) {
    fprintf(stderr, "Invalid value for --%s: %d. 0<= PrefixSize <=2000000000\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_int32(prefix_size, 0, "control the prefix size for HashSkipList and "
             "plain table");
DEFINE_int64(keys_per_prefix, 0, "control average number of keys generated "
             "per prefix, 0 means no special handling of the prefix, "
             "i.e. use the prefix comes with the generated random number.");
DEFINE_int32(memtable_insert_with_hint_prefix_size, 0,
             "If non-zero, enable "
             "memtable insert with hint with the given prefix size.");
DEFINE_bool(enable_io_prio, false, "Lower the background flush/compaction "
            "threads' IO priority");
DEFINE_bool(identity_as_first_hash, false, "the first hash function of cuckoo "
            "table becomes an identity function. This is only valid when key "
            "is 8 bytes");
DEFINE_bool(dump_malloc_stats, true, "Dump malloc stats in LOG ");

enum RepFactory {
  kSkipList,
  kPrefixHash,
  kVectorRep,
  kHashLinkedList,
  kCuckoo
};

static enum RepFactory StringToRepFactory(const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "skip_list"))
    return kSkipList;
  else if (!strcasecmp(ctype, "prefix_hash"))
    return kPrefixHash;
  else if (!strcasecmp(ctype, "vector"))
    return kVectorRep;
  else if (!strcasecmp(ctype, "hash_linkedlist"))
    return kHashLinkedList;
  else if (!strcasecmp(ctype, "cuckoo"))
    return kCuckoo;

  fprintf(stdout, "Cannot parse memreptable %s\n", ctype);
  return kSkipList;
}

static enum RepFactory FLAGS_rep_factory;
DEFINE_string(memtablerep, "skip_list", "");
DEFINE_int64(hash_bucket_count, 1024 * 1024, "hash bucket count");
DEFINE_bool(use_plain_table, false, "if use plain table "
            "instead of block-based table format");
DEFINE_bool(use_cuckoo_table, false, "if use cuckoo table format");
DEFINE_double(cuckoo_hash_ratio, 0.9, "Hash ratio for Cuckoo SST table.");
DEFINE_bool(use_hash_search, false, "if use kHashSearch "
            "instead of kBinarySearch. "
            "This is valid if only we use BlockTable");
DEFINE_bool(use_block_based_filter, false, "if use kBlockBasedFilter "
            "instead of kFullFilter for filter block. "
            "This is valid if only we use BlockTable");
DEFINE_string(merge_operator, "", "The merge operator to use with the database."
              "If a new merge operator is specified, be sure to use fresh"
              " database The possible merge operators are defined in"
              " utilities/merge_operators.h");
DEFINE_int32(skip_list_lookahead, 0, "Used with skip_list memtablerep; try "
             "linear search first for this many steps from the previous "
             "position");
DEFINE_bool(report_file_operations, false, "if report number of file "
            "operations");

static const bool FLAGS_soft_rate_limit_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_soft_rate_limit, &ValidateRateLimit);

static const bool FLAGS_hard_rate_limit_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_hard_rate_limit, &ValidateRateLimit);

static const bool FLAGS_prefix_size_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_prefix_size, &ValidatePrefixSize);

static const bool FLAGS_key_size_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_key_size, &ValidateKeySize);

static const bool FLAGS_cache_numshardbits_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_cache_numshardbits,
                          &ValidateCacheNumshardbits);

static const bool FLAGS_readwritepercent_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_readwritepercent, &ValidateInt32Percent);

DEFINE_int32(disable_seek_compaction, false,
             "Not used, left here for backwards compatibility");

static const bool FLAGS_deletepercent_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_deletepercent, &ValidateInt32Percent);
static const bool FLAGS_table_cache_numshardbits_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_table_cache_numshardbits,
                          &ValidateTableCacheNumshardbits);

namespace rocksdb {
const double bandwidth_in_one_hour[3600] = {
    100.0, 28.43, 38.89, 48.66, 35.25, 49.34, 35.7,  31.31, 28.79, 32.26, 34.2,
    28.92, 22.36, 30.46, 34.89, 35.84, 27.15, 40.16, 39.34, 29.02, 30.82, 34.69,
    26.3,  30.2,  29.93, 33.11, 44.1,  39.15, 41.05, 35.48, 34.03, 34.49, 41.11,
    30.46, 34.92, 40.59, 35.74, 26.36, 58.23, 34.36, 32.52, 36.1,  55.05, 42.1,
    39.8,  40.62, 43.48, 39.15, 45.34, 43.87, 41.11, 39.97, 38.1,  66.95, 46.07,
    40.1,  29.67, 38.89, 38.95, 46.23, 35.67, 36.79, 35.18, 34.49, 35.8,  27.9,
    39.08, 44.36, 47.21, 32.43, 40.26, 35.7,  46.43, 41.67, 48.82, 39.9,  32.92,
    43.21, 40.92, 41.84, 39.08, 42.49, 43.7,  45.97, 45.41, 44.1,  38.85, 36.79,
    41.8,  42.13, 32.39, 45.51, 31.34, 37.57, 28.89, 30.62, 34.72, 29.77, 34.69,
    29.34, 29.64, 37.97, 36.59, 30.85, 31.15, 38.2,  31.67, 30.39, 26.03, 25.34,
    24.95, 27.61, 35.87, 31.97, 34.66, 36.43, 30.52, 34.79, 41.34, 39.64, 44.66,
    45.15, 38.0,  40.07, 38.92, 42.62, 40.07, 30.89, 43.84, 34.26, 30.56, 35.25,
    33.67, 46.79, 48.2,  36.85, 34.59, 51.8,  42.62, 39.48, 37.15, 45.02, 38.23,
    43.9,  36.62, 32.2,  39.25, 41.67, 45.05, 49.84, 41.61, 36.26, 31.54, 34.16,
    40.69, 32.92, 33.05, 35.48, 44.03, 46.92, 36.66, 45.8,  42.43, 42.66, 41.97,
    51.34, 42.56, 31.28, 36.16, 43.21, 43.15, 38.16, 49.87, 40.59, 44.23, 43.67,
    52.33, 41.31, 41.48, 46.07, 39.18, 38.0,  35.34, 41.67, 40.62, 41.87, 35.41,
    35.18, 35.57, 33.08, 31.25, 31.77, 25.77, 30.62, 30.2,  30.36, 33.05, 33.9,
    32.69, 36.46, 29.9,  37.48, 37.7,  33.57, 36.03, 31.15, 37.05, 45.31, 44.59,
    60.03, 36.69, 42.13, 43.11, 40.07, 46.33, 46.59, 39.48, 46.92, 42.36, 42.59,
    38.82, 41.97, 49.21, 45.28, 48.56, 43.9,  44.79, 41.74, 37.48, 42.72, 33.87,
    57.15, 39.84, 36.03, 37.05, 35.51, 32.56, 35.25, 40.0,  31.57, 33.21, 30.33,
    31.9,  43.28, 49.54, 54.82, 52.26, 42.62, 42.72, 45.84, 41.67, 55.05, 53.67,
    44.23, 47.34, 47.64, 37.15, 41.77, 47.93, 37.97, 38.13, 40.3,  45.44, 45.02,
    40.52, 46.62, 39.64, 48.66, 41.54, 91.11, 33.93, 34.03, 37.7,  35.87, 38.62,
    39.64, 40.07, 39.54, 38.62, 42.85, 56.98, 41.93, 44.23, 39.74, 46.66, 57.11,
    41.8,  38.0,  36.85, 57.38, 55.15, 53.9,  47.9,  38.89, 45.31, 44.0,  38.0,
    32.39, 48.1,  29.84, 36.66, 33.44, 36.75, 38.75, 42.0,  41.15, 45.51, 38.36,
    34.16, 37.41, 36.69, 37.67, 44.36, 37.28, 39.61, 39.44, 21.93, 42.36, 33.84,
    39.54, 30.43, 34.82, 28.43, 27.93, 32.2,  33.34, 46.69, 35.9,  34.2,  28.16,
    32.75, 36.75, 32.98, 38.26, 40.36, 40.2,  41.7,  49.08, 38.79, 39.67, 39.87,
    37.64, 32.85, 33.84, 37.54, 50.33, 36.98, 46.3,  41.11, 40.39, 43.48, 39.97,
    41.31, 46.85, 37.21, 38.66, 37.7,  41.18, 28.33, 33.44, 25.57, 28.07, 28.1,
    29.15, 29.84, 29.7,  32.43, 35.28, 31.11, 32.0,  33.08, 37.15, 39.51, 37.08,
    41.9,  38.75, 34.66, 36.1,  38.46, 49.28, 50.16, 46.03, 38.49, 41.54, 46.23,
    54.89, 53.31, 44.85, 42.49, 41.44, 39.87, 43.77, 36.62, 43.11, 44.3,  37.28,
    41.25, 42.72, 36.3,  32.36, 35.77, 33.93, 39.97, 31.11, 37.93, 45.31, 40.98,
    40.56, 46.43, 40.79, 42.26, 36.23, 43.57, 45.57, 44.75, 44.2,  37.15, 55.77,
    38.75, 36.3,  45.93, 39.9,  40.95, 49.41, 41.64, 42.89, 50.3,  46.43, 44.95,
    41.18, 42.98, 43.48, 45.38, 44.85, 48.33, 53.02, 45.15, 50.03, 41.8,  42.82,
    40.33, 31.9,  34.46, 27.61, 28.3,  33.11, 35.41, 38.43, 33.8,  29.9,  50.0,
    46.79, 47.38, 57.77, 45.7,  56.95, 49.67, 45.05, 55.44, 42.16, 53.51, 47.54,
    53.02, 53.48, 49.64, 49.18, 54.59, 57.08, 50.3,  56.3,  50.03, 57.21, 56.62,
    51.8,  44.03, 52.69, 40.66, 54.07, 55.84, 58.66, 42.13, 50.52, 38.49, 42.43,
    82.69, 41.48, 39.64, 41.18, 50.0,  44.52, 43.44, 46.98, 41.74, 42.3,  39.15,
    38.36, 43.57, 41.05, 37.7,  45.08, 49.74, 43.57, 41.51, 40.69, 44.79, 40.62,
    47.34, 39.15, 37.41, 40.69, 41.25, 44.69, 36.89, 43.93, 41.15, 42.69, 42.23,
    36.2,  34.56, 32.33, 42.62, 52.16, 36.75, 36.33, 43.44, 44.16, 42.23, 46.59,
    39.25, 44.56, 36.13, 37.9,  36.95, 41.7,  42.92, 50.23, 39.11, 42.23, 37.57,
    34.26, 36.75, 34.92, 37.87, 46.26, 42.52, 49.21, 43.21, 38.03, 57.84, 42.46,
    39.34, 36.62, 41.21, 46.66, 52.98, 71.05, 55.25, 45.28, 48.1,  48.66, 60.2,
    44.95, 46.62, 43.87, 53.21, 43.93, 45.08, 52.3,  43.48, 49.67, 43.8,  45.18,
    43.11, 54.56, 40.26, 38.59, 36.79, 42.72, 37.38, 45.48, 43.31, 48.89, 44.1,
    44.49, 49.38, 42.49, 48.52, 44.89, 45.87, 39.25, 51.57, 39.54, 41.84, 45.15,
    49.77, 41.48, 41.97, 42.56, 34.52, 43.54, 38.85, 38.79, 37.64, 43.44, 42.0,
    48.03, 46.39, 46.62, 41.9,  34.43, 49.11, 41.25, 43.08, 37.28, 36.26, 35.87,
    26.52, 32.56, 33.97, 29.84, 45.87, 46.2,  42.98, 47.25, 44.49, 44.16, 49.34,
    55.67, 45.67, 44.13, 48.95, 48.59, 50.75, 43.28, 51.67, 44.03, 36.75, 45.02,
    37.84, 50.1,  34.3,  37.48, 38.39, 40.59, 37.93, 41.41, 34.07, 39.97, 41.87,
    38.72, 31.21, 35.61, 42.52, 34.46, 66.92, 59.08, 42.56, 42.07, 37.38, 40.89,
    40.36, 40.46, 38.33, 59.25, 46.62, 39.31, 44.0,  64.46, 47.8,  50.75, 40.75,
    51.9,  47.41, 45.28, 51.84, 42.72, 42.03, 38.1,  41.08, 45.41, 41.87, 48.49,
    43.93, 46.16, 52.23, 45.38, 50.3,  42.52, 52.92, 46.0,  49.61, 50.85, 73.61,
    51.61, 44.46, 46.0,  79.11, 47.05, 57.7,  43.57, 39.05, 36.95, 31.9,  36.49,
    34.95, 38.03, 40.26, 37.67, 41.61, 38.95, 41.84, 42.26, 38.07, 28.1,  29.57,
    34.75, 34.82, 36.59, 34.85, 37.51, 35.9,  37.97, 47.97, 37.15, 31.93, 37.54,
    32.75, 31.8,  34.07, 45.41, 42.49, 31.28, 32.39, 29.21, 34.46, 37.34, 34.13,
    32.66, 34.3,  35.02, 41.05, 36.89, 46.2,  40.39, 46.07, 35.34, 42.33, 55.87,
    45.9,  44.39, 43.7,  38.82, 46.79, 46.23, 100.0, 46.52, 50.0,  47.74, 47.61,
    45.48, 44.69, 49.54, 48.39, 35.64, 40.39, 43.67, 46.95, 45.67, 45.21, 46.59,
    42.92, 42.0,  39.28, 37.97, 40.16, 52.43, 49.54, 43.67, 44.95, 40.75, 40.13,
    38.75, 39.64, 46.49, 46.13, 54.95, 39.51, 43.93, 50.23, 50.0,  50.07, 39.77,
    65.93, 49.7,  45.15, 44.23, 44.98, 53.51, 46.33, 48.52, 57.02, 49.25, 46.56,
    50.79, 53.93, 50.07, 44.79, 44.89, 40.72, 47.41, 46.07, 51.67, 38.72, 40.13,
    41.34, 36.92, 38.72, 49.25, 45.31, 36.0,  48.07, 38.92, 40.07, 42.36, 40.36,
    38.2,  43.34, 41.9,  33.54, 41.28, 37.8,  41.57, 37.51, 40.2,  41.31, 45.57,
    33.54, 48.2,  38.95, 39.74, 36.07, 42.13, 39.51, 43.02, 37.97, 43.31, 41.31,
    42.92, 42.03, 35.61, 31.77, 34.0,  34.39, 31.57, 32.82, 58.07, 41.21, 36.95,
    41.25, 39.51, 36.07, 40.2,  42.98, 38.0,  34.3,  39.34, 42.1,  41.08, 44.46,
    36.1,  40.13, 46.26, 53.8,  38.07, 41.61, 43.93, 42.82, 40.95, 46.98, 38.98,
    44.46, 46.62, 52.98, 50.3,  49.87, 49.57, 45.28, 41.48, 43.02, 42.49, 45.18,
    50.98, 53.08, 46.3,  48.69, 43.51, 45.61, 50.26, 54.82, 48.1,  44.2,  47.9,
    46.66, 51.77, 53.48, 47.84, 41.08, 48.46, 40.62, 47.9,  87.84, 50.23, 38.1,
    43.28, 46.69, 35.7,  44.66, 37.18, 36.52, 44.98, 44.75, 47.57, 43.61, 36.95,
    39.74, 30.39, 31.8,  48.52, 30.33, 40.3,  36.33, 40.56, 41.11, 40.49, 45.54,
    35.25, 44.3,  37.64, 35.31, 41.34, 42.62, 35.44, 35.11, 34.07, 34.89, 40.49,
    33.67, 38.1,  31.34, 36.33, 36.85, 33.25, 41.15, 37.8,  37.77, 32.36, 36.98,
    30.85, 46.72, 41.28, 48.89, 41.02, 38.72, 42.39, 44.36, 46.13, 48.23, 39.34,
    50.92, 49.74, 47.02, 48.0,  55.74, 57.11, 54.0,  50.72, 46.49, 51.57, 55.18,
    52.0,  48.33, 59.21, 58.95, 54.89, 53.15, 59.28, 39.8,  47.25, 47.08, 45.18,
    52.43, 44.39, 51.64, 50.0,  39.08, 48.43, 48.0,  45.08, 39.77, 39.41, 43.08,
    46.95, 48.39, 43.02, 41.97, 42.92, 39.08, 47.64, 44.49, 43.28, 48.36, 36.56,
    41.77, 34.33, 42.46, 42.85, 44.26, 44.03, 43.21, 41.41, 43.15, 45.28, 57.41,
    39.18, 45.77, 55.02, 48.66, 41.34, 39.15, 40.62, 39.51, 46.1,  70.92, 32.36,
    36.1,  33.28, 40.3,  34.72, 36.95, 34.79, 37.05, 37.48, 40.23, 32.56, 41.74,
    30.62, 42.36, 38.07, 43.51, 40.46, 40.03, 38.85, 42.46, 36.72, 80.52, 40.82,
    42.72, 43.64, 48.26, 44.98, 43.97, 43.51, 54.43, 64.56, 49.97, 49.18, 39.64,
    37.57, 41.61, 40.62, 42.16, 33.48, 42.56, 41.77, 48.46, 43.44, 37.11, 41.08,
    40.89, 48.36, 39.97, 36.0,  34.92, 38.95, 34.23, 37.51, 37.44, 36.1,  47.15,
    50.82, 46.1,  36.95, 36.56, 45.05, 41.9,  44.36, 49.44, 37.25, 41.77, 42.59,
    44.1,  43.48, 42.26, 43.18, 34.3,  45.77, 33.67, 32.23, 40.43, 39.54, 37.64,
    36.43, 44.46, 34.59, 39.61, 33.87, 45.74, 38.07, 37.61, 42.59, 33.87, 43.34,
    37.25, 38.07, 39.48, 34.23, 38.1,  36.89, 45.51, 37.34, 36.07, 37.44, 37.28,
    41.7,  43.28, 37.05, 43.7,  47.7,  41.54, 45.54, 44.36, 50.03, 40.52, 33.38,
    44.03, 35.38, 39.51, 33.34, 30.46, 33.31, 29.9,  32.43, 27.41, 38.85, 28.46,
    41.93, 35.64, 69.51, 39.61, 39.77, 37.18, 41.51, 39.48, 39.61, 40.46, 40.69,
    37.15, 36.03, 51.67, 49.08, 48.92, 40.89, 54.56, 51.34, 57.84, 57.64, 53.57,
    44.85, 42.0,  44.75, 47.7,  48.52, 39.87, 41.25, 40.13, 36.03, 40.1,  41.54,
    42.0,  41.08, 40.75, 37.41, 41.51, 47.31, 39.9,  33.34, 40.3,  43.18, 51.21,
    30.07, 42.13, 36.07, 33.31, 43.28, 39.97, 37.11, 45.21, 40.43, 38.66, 36.3,
    45.34, 37.25, 33.08, 45.18, 37.9,  38.33, 52.69, 41.64, 46.36, 38.13, 39.64,
    41.11, 38.72, 37.41, 41.05, 53.28, 36.62, 34.66, 35.57, 40.59, 36.49, 52.03,
    37.25, 40.52, 41.34, 43.51, 45.84, 48.46, 52.23, 40.66, 36.23, 36.98, 48.23,
    37.31, 32.56, 40.26, 36.07, 36.16, 31.34, 38.23, 32.52, 32.59, 34.1,  37.84,
    33.74, 33.84, 31.61, 34.52, 32.0,  36.1,  38.98, 29.51, 34.52, 35.48, 32.72,
    41.84, 43.97, 39.77, 38.59, 40.69, 36.49, 36.52, 38.03, 53.11, 56.69, 47.25,
    44.49, 41.18, 48.0,  49.38, 50.79, 45.02, 55.67, 42.92, 44.13, 46.23, 43.38,
    45.93, 47.48, 49.54, 47.05, 42.62, 93.97, 43.02, 36.72, 29.54, 35.8,  40.85,
    36.43, 36.07, 41.84, 36.03, 48.39, 35.57, 44.26, 36.36, 44.56, 56.46, 32.85,
    46.36, 43.61, 43.31, 37.02, 38.95, 39.9,  45.21, 42.82, 48.03, 44.36, 50.59,
    50.43, 44.85, 39.9,  50.46, 48.89, 53.25, 43.15, 43.74, 44.1,  41.84, 43.18,
    39.8,  37.84, 35.61, 32.23, 35.54, 35.18, 35.05, 41.05, 39.38, 34.0,  33.67,
    39.05, 38.43, 36.43, 31.54, 42.39, 34.52, 38.72, 46.39, 41.77, 40.3,  39.15,
    32.13, 43.51, 33.31, 38.79, 37.87, 35.34, 35.57, 33.84, 41.57, 27.93, 26.75,
    17.67, 25.51, 30.16, 26.13, 35.9,  27.51, 26.33, 34.66, 26.26, 23.25, 30.07,
    38.43, 22.39, 32.49, 28.98, 28.75, 39.51, 40.07, 37.51, 68.26, 36.16, 35.21,
    37.77, 43.57, 31.84, 30.66, 37.38, 32.52, 32.13, 33.28, 45.7,  40.23, 40.98,
    31.7,  30.66, 34.56, 33.28, 33.8,  31.21, 32.43, 27.31, 30.69, 36.85, 41.64,
    45.18, 39.84, 40.59, 38.36, 35.02, 35.25, 29.9,  32.98, 31.74, 30.62, 32.39,
    29.21, 26.89, 27.57, 28.82, 28.43, 31.15, 39.15, 30.79, 44.03, 29.38, 38.43,
    39.41, 36.66, 41.84, 38.39, 72.43, 32.72, 38.2,  34.56, 28.39, 33.44, 37.44,
    36.66, 33.11, 28.62, 31.87, 33.11, 39.18, 35.77, 33.64, 30.39, 28.39, 29.21,
    32.23, 34.07, 28.23, 32.46, 26.13, 21.7,  23.08, 26.79, 31.93, 28.39, 29.25,
    28.1,  32.13, 33.25, 35.48, 49.11, 34.33, 30.59, 28.98, 33.08, 34.26, 28.1,
    27.61, 31.74, 27.87, 30.56, 32.0,  42.79, 32.82, 42.16, 41.77, 34.43, 30.26,
    25.28, 34.36, 30.59, 30.13, 24.59, 33.31, 38.43, 36.98, 34.92, 32.39, 34.16,
    37.25, 30.79, 33.64, 36.03, 38.52, 31.28, 30.89, 35.67, 23.48, 32.3,  27.64,
    34.13, 27.21, 31.48, 27.18, 33.34, 31.21, 28.79, 34.36, 38.66, 48.66, 34.46,
    42.16, 32.72, 30.26, 33.21, 25.97, 31.57, 37.8,  32.66, 36.2,  42.23, 34.36,
    37.93, 29.77, 27.34, 26.79, 30.59, 32.62, 28.69, 37.74, 37.18, 33.51, 59.77,
    30.23, 29.77, 31.97, 42.95, 39.97, 38.89, 28.72, 30.03, 30.2,  33.18, 30.75,
    35.34, 33.15, 37.15, 34.95, 34.75, 30.36, 35.84, 28.85, 25.11, 27.02, 28.56,
    23.8,  28.39, 23.87, 24.49, 24.82, 25.9,  27.93, 26.1,  24.13, 22.49, 31.54,
    26.52, 30.1,  33.21, 23.97, 33.25, 35.15, 29.54, 28.79, 33.64, 31.25, 32.03,
    39.57, 27.41, 31.11, 29.9,  29.74, 43.25, 44.46, 43.48, 42.1,  38.33, 36.79,
    34.52, 40.26, 39.25, 38.33, 29.21, 30.75, 34.66, 34.39, 45.18, 46.95, 42.2,
    51.25, 36.79, 47.87, 45.61, 40.03, 50.1,  42.13, 32.3,  36.03, 38.13, 43.44,
    36.43, 26.59, 40.85, 35.51, 30.85, 36.75, 28.75, 30.36, 29.57, 33.05, 31.08,
    33.34, 36.89, 37.57, 34.69, 33.25, 37.74, 37.93, 39.31, 36.36, 34.95, 36.95,
    35.61, 40.52, 35.18, 33.84, 34.49, 31.97, 32.56, 40.72, 30.89, 37.48, 34.72,
    33.51, 35.18, 38.92, 38.3,  32.92, 33.18, 33.28, 36.33, 40.1,  41.25, 37.38,
    36.03, 40.49, 36.79, 43.34, 34.56, 44.89, 33.18, 42.66, 28.92, 30.95, 35.77,
    50.16, 31.93, 28.85, 27.51, 26.95, 23.61, 20.82, 21.54, 20.98, 21.31, 31.28,
    29.84, 28.72, 31.21, 27.8,  41.41, 45.87, 31.57, 54.66, 54.95, 40.62, 47.38,
    45.51, 36.26, 44.23, 32.92, 32.52, 37.61, 38.92, 39.48, 33.34, 43.02, 31.64,
    33.64, 37.8,  32.07, 35.97, 31.8,  39.74, 34.98, 36.59, 34.23, 32.92, 35.57,
    39.21, 34.39, 35.84, 33.77, 44.56, 40.82, 44.69, 34.07, 39.21, 37.93, 37.08,
    32.52, 40.43, 31.57, 34.82, 39.44, 33.54, 41.05, 39.9,  35.74, 40.43, 33.84,
    39.87, 37.34, 39.02, 36.2,  32.49, 39.44, 33.21, 36.33, 28.92, 42.72, 33.77,
    36.26, 30.2,  33.38, 32.75, 95.44, 50.95, 27.97, 31.57, 33.77, 43.25, 40.72,
    38.3,  37.8,  42.3,  34.56, 38.36, 44.66, 31.31, 28.72, 36.62, 43.38, 29.61,
    32.95, 33.8,  34.23, 36.85, 31.31, 36.03, 34.1,  24.52, 27.21, 34.36, 35.41,
    36.1,  27.9,  27.54, 31.38, 33.18, 36.75, 37.15, 37.11, 35.31, 37.02, 42.72,
    38.69, 35.08, 31.77, 26.56, 32.0,  32.07, 33.11, 28.62, 26.39, 29.38, 34.3,
    26.72, 32.33, 21.44, 36.07, 38.56, 36.39, 32.23, 37.7,  27.11, 41.38, 39.11,
    33.8,  33.64, 37.64, 30.72, 38.92, 38.43, 35.54, 33.84, 33.74, 36.69, 40.82,
    36.46, 34.72, 39.48, 40.72, 37.18, 36.43, 35.77, 44.03, 44.72, 46.26, 51.54,
    45.34, 38.39, 34.52, 31.87, 32.46, 33.51, 33.34, 36.69, 28.52, 37.7,  37.74,
    30.66, 30.72, 36.72, 32.82, 38.46, 31.28, 42.69, 40.75, 39.67, 41.41, 38.95,
    35.18, 33.93, 31.77, 32.59, 27.7,  32.89, 30.26, 29.05, 25.77, 30.2,  35.57,
    28.13, 25.7,  28.52, 36.75, 41.8,  34.59, 28.59, 26.89, 44.13, 32.36, 30.62,
    28.07, 34.16, 35.11, 30.56, 31.25, 27.08, 31.44, 32.0,  31.97, 39.02, 41.18,
    36.23, 43.54, 40.03, 36.33, 33.84, 40.23, 42.62, 48.33, 38.59, 44.69, 44.16,
    36.26, 31.51, 47.48, 44.85, 41.9,  74.3,  33.9,  33.25, 30.89, 31.25, 38.82,
    37.87, 35.15, 44.82, 38.49, 41.67, 45.51, 33.74, 38.03, 34.03, 38.43, 29.08,
    33.34, 42.46, 39.57, 37.57, 35.8,  37.25, 38.3,  34.23, 35.28, 34.92, 43.38,
    39.38, 38.03, 39.02, 43.84, 40.39, 39.51, 43.97, 41.77, 37.44, 39.34, 38.3,
    35.31, 34.3,  42.0,  32.89, 29.97, 31.77, 38.59, 47.84, 31.93, 38.3,  33.41,
    27.51, 26.85, 24.66, 37.77, 22.89, 28.79, 29.38, 29.18, 30.52, 27.57, 30.69,
    31.8,  38.03, 45.08, 30.3,  40.52, 40.07, 27.74, 28.72, 32.26, 33.08, 25.05,
    25.54, 28.82, 34.03, 32.95, 30.2,  32.66, 34.26, 39.54, 37.9,  35.84, 35.97,
    31.64, 30.72, 33.57, 34.46, 35.74, 30.92, 32.66, 33.8,  38.33, 50.89, 36.95,
    36.1,  33.31, 33.8,  31.67, 26.72, 32.33, 36.36, 32.07, 22.59, 26.79, 31.08,
    35.64, 30.36, 37.28, 41.11, 36.0,  36.69, 40.49, 37.15, 36.03, 42.66, 39.8,
    41.7,  40.75, 34.36, 37.25, 40.03, 43.77, 40.49, 37.54, 40.16, 38.13, 50.82,
    40.43, 43.87, 41.15, 43.05, 43.57, 35.57, 37.87, 36.03, 53.64, 30.75, 41.74,
    34.36, 39.57, 31.02, 34.95, 42.43, 42.49, 31.9,  36.75, 35.25, 33.97, 33.9,
    31.84, 37.18, 31.97, 28.43, 33.25, 31.77, 34.75, 30.95, 37.18, 34.98, 30.07,
    31.05, 31.41, 49.51, 38.72, 37.18, 34.3,  34.07, 29.93, 39.51, 32.49, 38.52,
    31.7,  38.56, 31.08, 35.21, 28.2,  32.07, 40.46, 34.33, 33.34, 33.87, 32.26,
    30.16, 28.85, 27.64, 27.77, 25.93, 32.85, 26.59, 28.95, 34.36, 39.48, 28.75,
    33.84, 42.3,  41.48, 52.23, 44.0,  52.89, 49.48, 55.02, 50.26, 44.39, 51.05,
    44.03, 45.05, 46.43, 41.54, 40.92, 37.34, 40.56, 44.66, 44.75, 38.3,  44.03,
    49.64, 52.1,  55.05, 44.2,  44.75, 53.97, 40.89, 37.8,  74.82, 38.16, 42.95,
    39.54, 41.64, 36.95, 43.67, 41.21, 38.72, 37.8,  39.18, 37.41, 45.21, 43.54,
    45.28, 52.2,  38.52, 50.59, 43.31, 45.61, 38.92, 47.87, 37.44, 49.61, 53.48,
    47.64, 49.34, 43.84, 43.93, 46.89, 44.13, 47.67, 40.69, 49.08, 37.05, 40.69,
    42.56, 42.95, 37.48, 35.67, 47.9,  37.7,  28.69, 30.33, 35.8,  44.79, 45.64,
    46.2,  45.38, 40.56, 33.57, 35.11, 32.72, 34.2,  38.33, 39.31, 35.41, 41.77,
    37.18, 55.97, 38.16, 33.54, 38.23, 34.2,  34.16, 31.54, 38.75, 39.28, 42.33,
    39.7,  42.85, 37.87, 39.21, 42.36, 42.62, 35.51, 42.59, 46.92, 41.77, 42.23,
    40.36, 52.03, 46.49, 39.38, 31.93, 31.77, 30.52, 33.61, 39.25, 30.62, 29.77,
    36.95, 35.25, 35.57, 38.1,  39.44, 35.25, 39.05, 54.36, 35.34, 36.69, 42.16,
    40.3,  40.52, 40.1,  38.72, 47.77, 41.8,  39.28, 43.74, 38.23, 39.8,  42.33,
    45.21, 38.95, 47.18, 38.23, 36.75, 39.38, 38.3,  41.74, 39.97, 39.21, 42.59,
    37.93, 41.28, 40.59, 38.43, 38.43, 37.31, 37.97, 36.36, 31.84, 30.66, 28.82,
    29.02, 27.34, 29.48, 27.08, 30.98, 28.1,  28.46, 29.18, 31.84, 34.98, 34.62,
    26.92, 31.44, 40.72, 29.18, 35.77, 33.97, 46.13, 51.57, 38.46, 35.48, 49.05,
    39.61, 32.3,  35.97, 37.9,  35.9,  41.38, 35.87, 33.57, 36.33, 31.7,  32.16,
    37.48, 40.26, 32.26, 28.69, 37.57, 31.64, 31.93, 53.15, 30.72, 32.07, 30.79,
    38.03, 36.56, 41.97, 41.28, 37.02, 37.87, 37.05, 45.77, 54.46, 43.61, 47.74,
    42.75, 52.03, 43.54, 37.84, 46.98, 42.26, 38.69, 40.43, 49.44, 36.49, 38.13,
    36.95, 35.34, 35.21, 38.98, 43.84, 36.2,  42.33, 29.18, 37.44, 38.69, 35.18,
    35.34, 34.69, 35.74, 34.13, 31.54, 36.3,  31.41, 32.52, 37.05, 34.16, 36.95,
    38.59, 41.18, 37.48, 46.03, 31.57, 32.59, 30.26, 34.52, 35.54, 38.92, 40.0,
    50.62, 40.0,  59.64, 48.43, 48.3,  34.39, 37.54, 32.69, 31.67, 32.0,  33.31,
    47.48, 31.41, 35.11, 32.62, 34.46, 31.77, 38.66, 31.18, 39.9,  33.9,  32.03,
    28.3,  29.08, 35.41, 42.75, 45.41, 44.07, 39.11, 29.38, 38.26, 37.34, 33.25,
    35.61, 38.03, 34.56, 35.97, 40.95, 46.03, 43.51, 32.3,  44.75, 49.93, 50.33,
    35.84, 41.25, 42.13, 40.23, 46.0,  33.93, 39.97, 37.38, 36.0,  36.82, 36.75,
    37.41, 33.64, 43.67, 35.02, 40.16, 31.54, 33.41, 39.41, 34.95, 28.56, 35.51,
    34.52, 33.31, 33.44, 30.43, 28.56, 36.1,  37.97, 43.02, 35.8,  36.3,  40.26,
    44.0,  40.1,  45.15, 44.3,  40.49, 44.33, 43.93, 41.31, 48.33, 44.36, 34.03,
    33.18, 38.23, 40.82, 34.52, 39.57, 33.44, 38.79, 32.95, 29.34, 35.8,  33.38,
    36.3,  46.85, 51.61, 35.9,  35.21, 39.97, 35.8,  35.64, 38.92, 33.77, 32.13,
    33.18, 29.41, 28.49, 27.08, 28.66, 28.72, 27.28, 29.31, 33.67, 34.89, 26.69,
    31.38, 36.36, 44.52, 43.77, 32.16, 32.69, 37.51, 36.33, 40.23, 36.75, 35.31,
    35.93, 46.23, 51.08, 45.15, 35.21, 37.54, 44.03, 40.33, 39.34, 39.28, 33.61,
    38.66, 39.51, 39.9,  39.15, 44.85, 44.56, 35.44, 35.05, 37.38, 38.59, 38.49,
    38.56, 50.07, 48.62, 38.0,  38.46, 43.97, 32.23, 40.3,  52.33, 46.43, 42.79,
    46.62, 41.87, 42.26, 44.62, 44.43, 43.34, 47.9,  41.11, 40.66, 35.57, 44.39,
    40.52, 47.57, 46.98, 43.57, 42.56, 51.38, 46.07, 61.21, 40.26, 47.02, 45.8,
    41.08, 41.93, 39.31, 36.92, 30.59, 38.39, 54.49, 40.92, 36.1,  38.92, 32.23,
    37.25, 43.61, 40.36, 37.18, 55.84, 38.75, 38.62, 42.07, 32.46, 37.8,  35.8,
    35.84, 31.48, 35.18, 34.46, 37.54, 34.03, 36.33, 36.66, 41.28, 46.46, 47.08,
    40.2,  43.44, 39.67, 42.43, 32.69, 35.57, 33.41, 32.36, 45.7,  35.57, 29.8,
    29.51, 31.25, 29.87, 30.1,  31.44, 26.85, 32.13, 30.2,  44.69, 31.41, 29.54,
    30.52, 29.57, 31.21, 30.56, 25.9,  31.48, 32.56, 33.31, 36.69, 31.21, 31.25,
    32.43, 32.95, 26.62, 36.79, 36.1,  33.44, 39.28, 37.77, 42.79, 38.75, 37.8,
    42.92, 40.03, 35.64, 40.03, 40.13, 36.39, 34.0,  37.28, 38.66, 30.26, 32.03,
    30.56, 32.2,  38.85, 37.08, 34.0,  38.98, 38.75, 29.93, 41.77, 37.21, 42.36,
    48.07, 51.74, 46.16, 39.9,  39.28, 47.93, 45.84, 40.89, 39.41, 43.97, 34.69,
    40.95, 37.93, 38.62, 41.05, 37.97, 41.77, 38.46, 34.49, 37.31, 36.69, 37.08,
    40.43, 37.51, 42.43, 40.75, 50.23, 42.2,  47.31, 37.11, 36.2,  34.92, 44.03,
    40.56, 40.2,  32.26, 33.31, 30.3,  32.33, 42.03, 38.75, 38.39, 35.97, 33.74,
    36.33, 33.31, 36.3,  39.34, 47.41, 42.85, 36.49, 37.97, 38.49, 38.03, 45.31,
    40.43, 33.64, 35.31, 40.03, 37.21, 36.95, 36.03, 35.74, 36.72, 43.15, 40.95,
    36.72, 42.26, 35.15, 35.18, 36.62, 39.08, 32.59, 44.52, 33.15, 34.56, 36.2,
    44.2,  46.2,  47.84, 39.77, 46.26, 38.16, 39.8,  38.92, 38.92, 44.26, 38.1,
    37.64, 46.72, 41.44, 37.67, 39.64, 40.52, 41.08, 42.66, 44.3,  46.46, 40.36,
    41.38, 40.98, 46.56, 50.98, 48.0,  46.16, 42.49, 51.15, 50.66, 49.84, 40.98,
    37.28, 41.8,  41.25, 34.23, 42.52, 37.48, 28.92, 34.92, 40.26, 34.43, 33.97,
    33.84, 33.18, 34.82, 39.31, 37.31, 34.3,  33.25, 34.69, 39.41, 36.72, 38.89,
    37.44, 37.21, 32.85, 36.3,  40.95, 31.44, 40.82, 45.15, 50.95, 42.33, 35.93,
    37.34, 50.13, 40.23, 38.13, 41.87, 41.05, 30.72, 34.1,  33.64, 31.31, 29.7,
    30.43, 32.43, 34.89, 36.36, 42.72, 30.13, 40.52, 27.97, 29.11, 54.66, 28.26,
    38.16, 32.16, 32.23, 36.72, 42.36, 40.13, 36.85, 43.51, 38.1,  45.67, 40.07,
    45.11, 42.92, 44.07, 36.13, 42.72, 41.51, 40.16, 40.56, 36.92, 49.93, 40.07,
    39.11, 42.2,  47.25, 39.44, 41.18, 43.34, 41.67, 45.51, 45.41, 47.28, 52.75,
    46.56, 35.21, 37.54, 56.2,  52.23, 50.43, 43.38, 42.79, 43.08, 38.36, 44.16,
    39.93, 40.72, 40.43, 44.85, 47.54, 43.97, 42.36, 48.03, 44.16, 53.61, 44.1,
    49.38, 47.7,  48.98, 50.39, 45.15, 38.92, 44.23, 45.11, 35.31, 47.57, 40.66,
    39.84, 39.05, 26.89, 38.98, 31.41, 32.92, 32.13, 42.66, 33.64, 38.2,  32.0,
    27.05, 31.7,  35.41, 36.23, 48.23, 35.57, 36.26, 39.51, 41.48, 39.8,  49.51,
    40.36, 44.43, 38.46, 40.36, 44.23, 49.93, 42.89, 38.52, 41.9,  36.13, 43.31,
    48.39, 48.46, 43.02, 44.66, 44.52, 41.34, 41.77, 50.49, 53.7,  43.77, 39.21,
    36.43, 36.79, 39.31, 33.05, 35.77, 33.74, 29.18, 37.8,  37.51, 34.52, 39.11,
    35.64, 47.74, 42.75, 38.13, 37.11, 37.74, 48.23, 62.0,  44.72, 39.61, 51.41,
    44.85, 49.57, 38.23, 39.18, 37.18, 47.51, 46.49, 47.54, 49.34, 47.15, 45.84,
    44.39, 47.57, 43.21, 46.0,  35.84, 44.89, 43.28, 40.95, 40.92, 40.23, 42.33,
    48.82, 45.25, 36.85, 40.82, 40.07, 39.7,  39.28, 42.0,  40.95, 51.44, 52.79,
    44.79, 41.38, 41.48, 39.7,  31.05, 32.52, 36.23, 41.51, 35.74, 37.28, 36.07,
    61.25, 39.64, 36.36, 34.95, 39.05, 32.56, 36.59, 33.9,  40.69, 35.48, 34.46,
    40.03, 48.82, 54.13, 39.97, 40.69, 39.08, 46.26, 42.75, 34.92, 34.13, 32.59,
    36.49, 34.33, 32.43, 36.07, 33.87, 33.38, 31.74, 44.3,  37.64, 42.39, 48.3,
    38.26, 49.41, 42.72, 40.1,  41.02, 39.8,  33.51, 36.49, 36.66, 36.46, 40.56,
    44.85, 47.8,  42.62, 39.9,  39.7,  47.38, 56.98, 44.46, 48.82, 40.66, 45.93,
    49.05, 42.39, 47.9,  44.2,  37.44, 44.07, 43.38, 44.89, 45.67, 35.97, 37.25,
    42.79, 42.82, 30.23, 35.87, 33.8,  31.02, 28.07, 27.38, 29.67, 28.92, 28.89,
    47.41, 40.79, 36.16, 46.85, 42.72, 41.15, 42.92, 30.56, 32.46, 29.28, 38.85,
    29.57, 52.59, 35.84, 53.34, 37.38, 42.79, 47.25, 44.95, 52.59, 56.75, 43.48,
    46.79, 46.33, 36.2,  48.1,  38.59, 41.34, 50.85, 45.15, 44.13, 42.07, 33.38,
    40.66, 35.31, 35.54, 36.92, 45.41, 34.75, 32.75, 36.82, 36.89, 44.49, 34.79,
    34.75, 37.41, 36.56, 36.62, 37.51, 37.44, 35.25, 39.31, 34.33, 35.8,  39.31,
    50.2,  30.13, 36.92, 53.57, 35.48, 45.11, 34.43, 34.62, 36.52, 40.1,  35.28,
    39.64, 37.08, 37.41, 31.61, 37.48, 34.52, 32.46, 38.33, 32.13, 39.87, 39.87,
    38.75, 39.8,  38.1,  31.97, 38.46, 43.87, 39.44, 38.79, 29.61, 34.3,  38.56,
    37.74, 42.16, 40.3,  48.66, 60.52, 44.85, 53.11, 54.3,  47.18, 48.95, 63.05,
    46.56, 32.85, 34.13, 33.84, 43.54, 34.98, 36.95, 36.75, 33.11, 36.92, 38.72,
    36.79, 38.16, 27.25, 43.93, 35.44, 39.34, 36.43, 40.79, 40.69, 52.2,  45.15,
    38.79, 35.93, 38.39, 36.16, 33.93, 36.56, 37.21, 50.66, 44.56, 44.07, 48.52,
    49.11, 43.77, 31.41, 36.95, 41.05, 39.38, 56.92, 38.2,  39.02, 36.95, 32.98,
    38.2,  33.8,  26.92, 38.36, 35.28, 46.75, 42.2,  43.15, 44.1,  41.7,  42.23,
    39.02, 40.95, 32.3,  32.75, 36.69, 29.15, 30.82, 40.59, 40.43, 36.49, 40.3,
    34.2,  38.33, 41.28, 35.54, 39.38, 45.31, 42.52, 36.13, 43.87, 40.43, 41.8,
    44.03, 38.98, 44.1,  46.56, 42.33, 49.08, 47.15, 47.48, 45.41, 48.0,  40.72,
    40.49, 50.69, 38.98, 39.25, 41.11, 38.07, 37.97, 39.57, 46.07, 45.9,  40.1,
    41.25, 35.51, 36.49, 40.46, 37.28, 43.64, 48.75, 33.25, 32.03, 29.02, 36.36,
    36.85, 36.98, 33.15, 29.61, 40.95, 34.39, 39.64, 28.89, 31.11, 30.56, 44.0,
    35.57, 36.56, 45.67, 45.9,  39.57, 41.7,  41.51, 44.72, 38.75, 46.2,  38.0,
    30.43, 35.74, 38.85, 33.38, 42.72, 34.62, 44.59, 44.33, 38.26, 39.05, 39.7,
    33.34, 30.69, 68.46, 33.25, 48.62, 35.64, 36.36, 40.2,  37.41, 45.97, 35.34,
    38.33, 34.59, 32.46, 61.48, 31.11, 36.0,  36.49, 34.2,  34.75, 41.08, 30.03,
    43.21, 33.57, 37.7,  33.57, 31.87, 50.82, 42.33, 34.13, 34.72, 37.41, 38.03,
    39.08, 33.57, 40.07, 41.41, 40.85, 45.41, 38.92, 38.66, 38.75, 41.08, 37.67,
    34.69, 34.2,  37.41, 45.87, 32.72, 39.97, 38.46, 30.43, 40.3,  43.74, 46.95,
    49.44, 46.52, 56.85, 62.82, 38.52, 42.33, 42.59, 43.9,  54.1,  49.8,  44.95,
    46.0,  45.02, 39.02, 36.79, 42.39, 38.23, 42.85, 35.25, 36.98, 39.67, 33.02,
    34.2,  38.07, 55.77, 26.85, 32.89, 33.51, 34.69, 37.21, 44.89, 62.49, 44.89,
    41.02, 49.18, 33.05, 50.43, 43.02, 43.44, 36.75, 35.64, 31.57, 37.21, 43.31,
    36.62, 38.36, 42.82, 37.77, 39.51, 39.93, 36.75, 49.15, 43.31, 42.36, 37.84,
    41.28, 41.9,  37.05, 34.75, 34.33, 35.93, 37.38, 42.89, 38.72, 39.34, 34.92,
    35.54, 32.1,  37.38, 31.54, 30.49, 29.21, 37.34, 37.11, 34.43, 44.49, 33.9,
    33.77, 30.2,  38.26, 39.08, 47.18, 51.97, 41.7,  37.74, 40.46, 42.79, 54.13,
    34.36, 43.84, 41.28, 38.85, 53.44, 39.05, 46.13, 32.62, 36.95, 40.1,  37.08,
    35.9,  45.77, 50.98, 39.7,  43.28, 37.77, 49.44, 39.64, 45.8,  42.95, 43.28,
    42.33, 47.93, 42.43, 52.46, 43.61, 48.2,  41.74, 43.44, 40.79, 39.34, 39.18,
    36.98, 38.49, 43.8,  47.02, 38.07, 35.8,  40.23, 41.18, 30.85, 33.05, 34.07,
    34.43, 31.67, 36.85, 45.7,  47.61, 44.3,  44.03, 39.38, 47.31, 39.54, 45.64,
    42.98, 42.59, 41.15, 52.79, 41.15, 36.56, 42.82, 33.48, 33.34, 29.28, 42.0,
    40.46, 35.57, 44.72, 30.26, 34.23, 32.23, 32.98, 34.2,  30.66, 30.52, 31.77,
    34.26, 40.16, 32.69, 33.7,  32.72, 32.33, 37.25, 33.38, 37.9,  38.13, 34.39,
    48.39, 36.62, 49.02, 49.67, 39.38, 46.1,  47.57, 47.02, 47.18, 42.1,  46.82,
    42.66, 45.38, 47.61, 39.8,  39.05, 47.77, 41.84, 43.08, 44.95, 36.33, 38.95,
    39.08, 40.56, 45.67, 38.23, 37.15, 36.95, 34.56, 43.77, 44.66, 41.87, 41.38,
    42.39, 34.98, 39.57, 38.62, 38.16, 44.2,  46.62, 39.74, 41.61, 41.11, 47.87,
    39.08, 46.39, 37.31, 35.9,  38.26, 42.3,  47.67, 29.64, 47.31, 43.48, 34.2,
    34.66, 32.0,  32.07};


namespace {
struct ReportFileOpCounters {
  std::atomic<int> open_counter_;
  std::atomic<int> read_counter_;
  std::atomic<int> append_counter_;
  std::atomic<uint64_t> bytes_read_;
  std::atomic<uint64_t> bytes_written_;
};

// A special Env to records and report file operations in db_bench
class ReportFileOpEnv : public EnvWrapper {
 public:
  explicit ReportFileOpEnv(Env* base) : EnvWrapper(base) { reset(); }

  void reset() {
    counters_.open_counter_ = 0;
    counters_.read_counter_ = 0;
    counters_.append_counter_ = 0;
    counters_.bytes_read_ = 0;
    counters_.bytes_written_ = 0;
  }

  Status NewSequentialFile(const std::string& f, unique_ptr<SequentialFile>* r,
                           const EnvOptions& soptions) override {
    class CountingFile : public SequentialFile {
     private:
      unique_ptr<SequentialFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(unique_ptr<SequentialFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}

      virtual Status Read(size_t n, Slice* result, char* scratch) override {
        counters_->read_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Read(n, result, scratch);
        counters_->bytes_read_.fetch_add(result->size(),
                                         std::memory_order_relaxed);
        return rv;
      }

      virtual Status Skip(uint64_t n) override { return target_->Skip(n); }
    };

    Status s = target()->NewSequentialFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f,
                             unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& soptions) override {
    class CountingFile : public RandomAccessFile {
     private:
      unique_ptr<RandomAccessFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(unique_ptr<RandomAccessFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}
      virtual Status Read(uint64_t offset, size_t n, Slice* result,
                          char* scratch) const override {
        counters_->read_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Read(offset, n, result, scratch);
        counters_->bytes_read_.fetch_add(result->size(),
                                         std::memory_order_relaxed);
        return rv;
      }
    };

    Status s = target()->NewRandomAccessFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  Status NewWritableFile(const std::string& f, unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    class CountingFile : public WritableFile {
     private:
      unique_ptr<WritableFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(unique_ptr<WritableFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}

      Status Append(const Slice& data) override {
        counters_->append_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Append(data);
        counters_->bytes_written_.fetch_add(data.size(),
                                            std::memory_order_relaxed);
        return rv;
      }

      Status Truncate(uint64_t size) override { return target_->Truncate(size); }
      Status Close() override { return target_->Close(); }
      Status Flush() override { return target_->Flush(); }
      Status Sync() override { return target_->Sync(); }
    };

    Status s = target()->NewWritableFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  // getter
  ReportFileOpCounters* counters() { return &counters_; }

 private:
  ReportFileOpCounters counters_;
};

}  // namespace

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  unsigned int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < (unsigned)std::max(1048576, FLAGS_value_size)) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(unsigned int len) {
    assert(len <= data_.size());
    if (pos_ + len > data_.size()) {
      pos_ = 0;
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }

  Slice GenerateWithTTL(unsigned int len) {
    assert(len <= data_.size());
    if (pos_ + len > data_.size()) {
      pos_ = 0;
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

enum ThreadStallLevels : int {
  //  kLowFlush,
  kL0Stall,
  kPendingBytes,
  kGoodArea,
  kIdle,
  kBandwidthCongestion
};
enum BatchSizeStallLevels : int {
  kTinyMemtable,  // tiny memtable
  kStallFree,
  kOverFrequent
};

struct SystemScores {
  // Memory Component
  uint64_t memtable_speed;   // MB per sec
  double active_size_ratio;  // active size / total memtable size
  int immutable_number;      // NonFlush number
  // Flushing
  double flush_speed_avg;
  double flush_min;
  double flush_speed_var;
  // Compaction speed
  double l0_num;
  // LSM  size
  double l0_drop_ratio;
  double estimate_compaction_bytes;  // given by the system, divided by the soft
                                     // limit
  // System metrics
  double disk_bandwidth;  // avg
  double flush_idle_time;
  double flush_gap_time;
  double compaction_idle_time;  // calculate by idle calculating,flush and
                                // compaction stats separately
  int flush_numbers;

  SystemScores() {
    memtable_speed = 0.0;
    active_size_ratio = 0.0;
    immutable_number = 0;
    flush_speed_avg = 0.0;
    flush_min = 9999999;
    flush_speed_var = 0.0;
    l0_num = 0.0;
    l0_drop_ratio = 0.0;
    estimate_compaction_bytes = 0.0;
    disk_bandwidth = 0.0;
    compaction_idle_time = 0.0;
    flush_numbers = 0;
    flush_gap_time = 0;
  }
  void Reset() {
    memtable_speed = 0.0;
    active_size_ratio = 0.0;
    immutable_number = 0;
    flush_speed_avg = 0.0;
    flush_speed_var = 0.0;
    l0_num = 0.0;
    l0_drop_ratio = 0.0;
    estimate_compaction_bytes = 0.0;
    disk_bandwidth = 0.0;
    compaction_idle_time = 0.0;
    flush_numbers = 0;
    flush_gap_time = 0;
  }
  SystemScores operator-(const SystemScores& a);
  SystemScores operator+(const SystemScores& a);
  SystemScores operator/(const int& a);
};

typedef SystemScores ScoreGradient;

struct ChangePoint {
  std::string opt;
  std::string value;
  int change_timing;
  bool db_width;
};
enum OpType : int { kLinearIncrease, kHalf, kKeep };
struct TuningOP {
  OpType BatchOp;
  OpType ThreadOp;
};
class DOTA_Tuner {
 protected:
  const Options default_opts;
  uint64_t tuning_rounds;
  Options current_opt;
  Version* version;
  ColumnFamilyData* cfd;
  VersionStorageInfo* vfs;
  DBImpl* running_db_;
  int64_t* last_report_ptr;
  std::atomic<int64_t>* total_ops_done_ptr_;
  std::deque<SystemScores> scores;
  std::vector<ScoreGradient> gradients;
  int current_sec;
  uint64_t flush_list_accessed, compaction_list_accessed;
  ThreadStallLevels last_thread_states;
  BatchSizeStallLevels last_batch_stat;
  std::shared_ptr<std::vector<FlushMetrics>> flush_list_from_opt_ptr;
  std::shared_ptr<std::vector<QuicksandMetrics>> compaction_list_from_opt_ptr;
  SystemScores max_scores;
  SystemScores avg_scores;
  uint64_t last_flush_thread_len;
  uint64_t last_compaction_thread_len;
  Env* env_;
  double tuning_gap;
  int double_ratio = 2;
  uint64_t last_unflushed_bytes = 0;
  const int score_array_len = 600 / tuning_gap;
  double idle_threshold = 2.5;
  double FEA_gap_threshold = 1;
  double TEA_slow_flush = 0.5;
  uint64_t last_non_zero_flush = 0;
  void UpdateSystemStats() { UpdateSystemStats(running_db_); }

 public:
  DOTA_Tuner(const Options opt, DBImpl* running_db, int64_t* last_report_op_ptr,
             std::atomic<int64_t>* total_ops_done_ptr, Env* env,
             uint64_t gap_sec)
      : default_opts(opt),
        tuning_rounds(0),
        running_db_(running_db),
        scores(),
        gradients(0),
        current_sec(0),
        flush_list_accessed(0),
        compaction_list_accessed(0),
        last_thread_states(kL0Stall),
        last_batch_stat(kTinyMemtable),
        flush_list_from_opt_ptr(running_db->immutable_db_options().flush_stats),
        compaction_list_from_opt_ptr(
            running_db->immutable_db_options().job_stats),
        max_scores(),
        last_flush_thread_len(0),
        last_compaction_thread_len(0),
        env_(env),
        tuning_gap(gap_sec),
        core_num(running_db->immutable_db_options().core_number),
        max_memtable_size(
            running_db->immutable_db_options().max_memtable_size) {
    this->last_report_ptr = last_report_op_ptr;
    this->total_ops_done_ptr_ = total_ops_done_ptr;
  }
  void set_idle_ratio(double idle_ra) { idle_threshold = idle_ra; }
  void set_gap_threshold(double ng_threshold) {
    FEA_gap_threshold = ng_threshold;
  }
  void set_slow_flush_threshold(double sf_threshold) {
    this->TEA_slow_flush = sf_threshold;
  }
  virtual ~DOTA_Tuner();

  inline void UpdateMaxScore(SystemScores& current_score) {
    //    if (!scores.empty() &&
    //        current_score.memtable_speed > scores.front().memtable_speed * 2)
    //        {
    //      // this would be an error
    //      return;
    //    }

    if (current_score.memtable_speed > max_scores.memtable_speed) {
      max_scores.memtable_speed = current_score.memtable_speed;
    }
    if (current_score.active_size_ratio > max_scores.active_size_ratio) {
      max_scores.active_size_ratio = current_score.active_size_ratio;
    }
    if (current_score.immutable_number > max_scores.immutable_number) {
      max_scores.immutable_number = current_score.immutable_number;
    }

    if (current_score.flush_speed_avg > max_scores.flush_speed_avg) {
      max_scores.flush_speed_avg = current_score.flush_speed_avg;
    }
    if (current_score.flush_speed_var > max_scores.flush_speed_var) {
      max_scores.flush_speed_var = current_score.flush_speed_var;
    }
    if (current_score.l0_num > max_scores.l0_num) {
      max_scores.l0_num = current_score.l0_num;
    }
    if (current_score.l0_drop_ratio > max_scores.l0_drop_ratio) {
      max_scores.l0_drop_ratio = current_score.l0_drop_ratio;
    }
    if (current_score.estimate_compaction_bytes >
        max_scores.estimate_compaction_bytes) {
      max_scores.estimate_compaction_bytes =
          current_score.estimate_compaction_bytes;
    }
    if (current_score.disk_bandwidth > max_scores.disk_bandwidth) {
      max_scores.disk_bandwidth = current_score.disk_bandwidth;
    }
    if (current_score.flush_idle_time > max_scores.flush_idle_time) {
      max_scores.flush_idle_time = current_score.flush_idle_time;
    }
    if (current_score.compaction_idle_time > max_scores.compaction_idle_time) {
      max_scores.compaction_idle_time = current_score.compaction_idle_time;
    }
    if (current_score.flush_numbers > max_scores.flush_numbers) {
      max_scores.flush_numbers = current_score.flush_numbers;
    }
  }

  void ResetTuner() { tuning_rounds = 0; }
  void UpdateSystemStats(DBImpl* running_db) {
    current_opt = running_db->GetOptions();
    version = running_db->GetVersionSet()
                  ->GetColumnFamilySet()
                  ->GetDefault()
                  ->current();
    cfd = version->cfd();
    vfs = version->storage_info();
  }
  virtual void DetectTuningOperations(int secs_elapsed,
                                      std::vector<ChangePoint>* change_list);

  ScoreGradient CompareWithBefore() { return scores.back() - scores.front(); }
  ScoreGradient CompareWithBefore(SystemScores& past_score) {
    return scores.back() - past_score;
  }
  ScoreGradient CompareWithBefore(SystemScores& past_score,
                                  SystemScores& current_score) {
    return current_score - past_score;
  }
  ThreadStallLevels LocateThreadStates(SystemScores& score);
  BatchSizeStallLevels LocateBatchStates(SystemScores& score);

  const std::string memtable_size = "write_buffer_size";
  const std::string sst_size = "target_file_size_base";
  const std::string total_l1_size = "max_bytes_for_level_base";
  const std::string max_bg_jobs = "max_background_jobs";
  const std::string memtable_number = "max_write_buffer_number";

  const int core_num;
  int max_thread = core_num;
  const int min_thread = 2;
  uint64_t max_memtable_size;
  const uint64_t min_memtable_size = 64 << 20;

  SystemScores ScoreTheSystem();
  void AdjustmentTuning(std::vector<ChangePoint>* change_list,
                        SystemScores& score, ThreadStallLevels levels,
                        BatchSizeStallLevels stallLevels);
  TuningOP VoteForOP(SystemScores& current_score, ThreadStallLevels levels,
                     BatchSizeStallLevels stallLevels);
  void FillUpChangeList(std::vector<ChangePoint>* change_list, TuningOP op);
  void SetBatchSize(std::vector<ChangePoint>* change_list,
                    uint64_t target_value);
  void SetThreadNum(std::vector<ChangePoint>* change_list, int target_value);
};

enum Stage : int { kSlowStart, kStabilizing };
class FEAT_Tuner : public DOTA_Tuner {
 public:
  FEAT_Tuner(const Options opt, DBImpl* running_db, int64_t* last_report_op_ptr,
             std::atomic<int64_t>* total_ops_done_ptr, Env* env, int gap_sec,
             bool triggerTEA, bool triggerFEA)
      : DOTA_Tuner(opt, running_db, last_report_op_ptr, total_ops_done_ptr, env,
                   gap_sec),
        TEA_enable(triggerTEA),
        FEA_enable(triggerFEA),
        current_stage(kSlowStart) {
    flush_list_from_opt_ptr =
        this->running_db_->immutable_db_options().flush_stats;

    std::cout << "Using FEAT tuner.\n FEA is "
              << (FEA_enable ? "triggered" : "NOT triggered") << std::endl;
    std::cout << "TEA is " << (TEA_enable ? "triggered" : "NOT triggered")
              << std::endl;
  }
  void DetectTuningOperations(int secs_elapsed,
                              std::vector<ChangePoint>* change_list) override;
  ~FEAT_Tuner() override;

  TuningOP TuneByTEA();
  TuningOP TuneByFEA();

 private:
  bool TEA_enable;
  bool FEA_enable;
  SystemScores current_score_;
  SystemScores head_score_;
  std::deque<TuningOP> recent_ops;
  Stage current_stage;
  double bandwidth_congestion_threshold = 0.7;
  double slow_down_threshold = 0.75;
  double RO_threshold = 0.8;
  double LO_threshold = 0.7;
  double MO_threshold = 0.5;
  double batch_changing_frequency = 0.7;
  int congestion_threads = min_thread;
  //  int double_ratio = 4;
  SystemScores normalize(SystemScores& origin_score);

  inline const char* StageString(Stage v) {
    switch (v) {
      case kSlowStart:
        return "slow start";
        //      case kBoundaryDetection:
        //        return "Boundary Detection";
      case kStabilizing:
        return "Stabilizing";
    }
    return "unknown operation";
  }
  void CalculateAvgScore();
};
inline const char* OpString(OpType v) {
  switch (v) {
    case kLinearIncrease:
      return "Linear Increase";
    case kHalf:
      return "Half";
    case kKeep:
      return "Keep";
  }
  return "unknown operation";
}


DOTA_Tuner::~DOTA_Tuner() = default;
void DOTA_Tuner::DetectTuningOperations(
    int secs_elapsed, std::vector<ChangePoint> *change_list_ptr) {
  current_sec = secs_elapsed;
  //  UpdateSystemStats();
  SystemScores current_score = ScoreTheSystem();
  UpdateMaxScore(current_score);
  scores.push_back(current_score);
  gradients.push_back(current_score - scores.front());

  auto thread_stat = LocateThreadStates(current_score);
  auto batch_stat = LocateBatchStates(current_score);

  AdjustmentTuning(change_list_ptr, current_score, thread_stat, batch_stat);
  // decide the operation based on the best behavior and last behavior
  // update the histories
  last_thread_states = thread_stat;
  last_batch_stat = batch_stat;
  tuning_rounds++;
}
ThreadStallLevels DOTA_Tuner::LocateThreadStates(SystemScores &score) {
  if (score.memtable_speed < max_scores.memtable_speed * 0.7) {
    // speed is slower than before, performance is in the stall area
    if (score.immutable_number >= 1) {
      if (score.flush_speed_avg <= max_scores.flush_speed_avg * 0.5) {
        // it's not influenced by the flushing speed
        if (current_opt.max_background_jobs > 6) {
          return kBandwidthCongestion;
        }
        //        else {
        //          return kLowFlush;
        //        }
      } else if (score.l0_num > 0.5) {
        // it's in the l0 stall
        return kL0Stall;
      }
    } else if (score.l0_num > 0.7) {
      // it's in the l0 stall
      return kL0Stall;
    } else if (score.estimate_compaction_bytes > 0.5) {
      return kPendingBytes;
    }
  } else if (score.compaction_idle_time > 2.5) {
    return kIdle;
  }
  return kGoodArea;
}

BatchSizeStallLevels DOTA_Tuner::LocateBatchStates(SystemScores &score) {
  if (score.memtable_speed < max_scores.memtable_speed * 0.7) {
    if (score.flush_speed_avg < max_scores.flush_speed_avg * 0.5) {
      if (score.active_size_ratio > 0.5 && score.immutable_number >= 1) {
        return kTinyMemtable;
      } else if (current_opt.max_background_jobs > 6 || score.l0_num > 0.9) {
        return kTinyMemtable;
      }
    }
  } else if (score.flush_numbers < max_scores.flush_numbers * 0.3) {
    return kOverFrequent;
  }

  return kStallFree;
};

SystemScores DOTA_Tuner::ScoreTheSystem() {
  UpdateSystemStats();
  SystemScores current_score;

  uint64_t total_mem_size = 0;
  uint64_t active_mem = 0;
  running_db_->GetIntProperty("rocksdb.size-all-mem-tables", &total_mem_size);
  running_db_->GetIntProperty("rocksdb.cur-size-active-mem-table", &active_mem);

  current_score.active_size_ratio =
      (double)active_mem / (double)current_opt.write_buffer_size;
  current_score.immutable_number =
      cfd->imm() == nullptr ? 0 : cfd->imm()->NumNotFlushed();

  std::vector<FlushMetrics> flush_metric_list;

  auto flush_result_length =
      running_db_->immutable_db_options().flush_stats->size();


  auto compaction_result_length =
      running_db_->immutable_db_options().job_stats->size();

  for (uint64_t i = flush_list_accessed; i < flush_result_length; i++) {
    auto temp = flush_list_from_opt_ptr->at(i);
    current_score.flush_min =
        std::min(current_score.flush_speed_avg, current_score.flush_min);
    flush_metric_list.push_back(temp);
    current_score.flush_speed_avg += temp.write_out_bandwidth;
    current_score.disk_bandwidth += temp.total_bytes;
    last_non_zero_flush = temp.write_out_bandwidth;
    if (current_score.l0_num > temp.l0_files) {
      current_score.l0_num = temp.l0_files;
    }
  }
  int l0_compaction = 0;
  auto num_new_flushes = (flush_result_length - flush_list_accessed);
  current_score.flush_numbers = num_new_flushes;

  while (total_mem_size < last_unflushed_bytes) {
    total_mem_size += current_opt.write_buffer_size;
  }
  current_score.memtable_speed += (total_mem_size - last_unflushed_bytes);

  current_score.memtable_speed /= tuning_gap;
  current_score.memtable_speed /= kMicrosInSecond;  // we use MiB to calculate

  uint64_t max_pending_bytes = 0;

  last_unflushed_bytes = total_mem_size;
  for (uint64_t i = compaction_list_accessed; i < compaction_result_length;
       i++) {
    auto temp = compaction_list_from_opt_ptr->at(i);
    if (temp.input_level == 0) {
      current_score.l0_drop_ratio += temp.drop_ratio;
      l0_compaction++;
    }
    if (temp.current_pending_bytes > max_pending_bytes) {
      max_pending_bytes = temp.current_pending_bytes;
    }
    current_score.disk_bandwidth += temp.total_bytes;
  }

  // flush_speed_avg,flush_speed_var,l0_drop_ratio
  if (num_new_flushes != 0) {
    auto avg_flush = current_score.flush_speed_avg / num_new_flushes;
    current_score.flush_speed_avg /= num_new_flushes;
    for (auto item : flush_metric_list) {
      current_score.flush_speed_var += (item.write_out_bandwidth - avg_flush) *
                                       (item.write_out_bandwidth - avg_flush);
    }
    current_score.flush_speed_var /= num_new_flushes;
    current_score.flush_gap_time /= (kMicrosInSecond * num_new_flushes);
  }

  if (l0_compaction != 0) {
    current_score.l0_drop_ratio /= l0_compaction;
  }
  // l0_num

  current_score.l0_num = (double)(vfs->NumLevelFiles(vfs->base_level())) /
                         current_opt.level0_slowdown_writes_trigger;
  //std::cout << "currenct score, l0 number" << current_score.l0_num <<std::endl;
  //  current_score.l0_num = l0_compaction == 0 ? current_score.l0_num : 0;
  // disk bandwidth,estimate_pending_bytes ratio
  current_score.disk_bandwidth /= kMicrosInSecond;

  current_score.estimate_compaction_bytes =
      (double)vfs->estimated_compaction_needed_bytes() /
      current_opt.soft_pending_compaction_bytes_limit;

  // clean up
  flush_list_accessed = flush_result_length;
  compaction_list_accessed = compaction_result_length;
  return current_score;
}

void DOTA_Tuner::AdjustmentTuning(std::vector<ChangePoint> *change_list,
                                  SystemScores &score,
                                  ThreadStallLevels thread_levels,
                                  BatchSizeStallLevels batch_levels) {
  // tune for thread number
  auto tuning_op = VoteForOP(score, thread_levels, batch_levels);
  // tune for memtable
  FillUpChangeList(change_list, tuning_op);
}
TuningOP DOTA_Tuner::VoteForOP(SystemScores & /*current_score*/,
                               ThreadStallLevels thread_level,
                               BatchSizeStallLevels batch_level) {
  TuningOP op;
  switch (thread_level) {
      //    case kLowFlush:
      //      op.ThreadOp = kDouble;
      //      break;
    case kL0Stall:
      op.ThreadOp = kLinearIncrease;
      break;
    case kPendingBytes:
      op.ThreadOp = kLinearIncrease;
      break;
    case kGoodArea:
      op.ThreadOp = kKeep;
      break;
    case kIdle:
      op.ThreadOp = kHalf;
      break;
    case kBandwidthCongestion:
      op.ThreadOp = kHalf;
      break;
  }

  if (batch_level == kTinyMemtable) {
    op.BatchOp = kLinearIncrease;
  } else if (batch_level == kStallFree) {
    op.BatchOp = kKeep;
  } else {
    op.BatchOp = kHalf;
  }

  return op;
}

inline void DOTA_Tuner::SetThreadNum(std::vector<ChangePoint> *change_list,
                                     int target_value) {
  ChangePoint thread_num_cp;
  thread_num_cp.opt = max_bg_jobs;
  thread_num_cp.db_width = true;
  target_value = std::max(target_value, min_thread);
  target_value = std::min(target_value, max_thread);
  thread_num_cp.value = std::to_string(target_value);
  change_list->push_back(thread_num_cp);
}

inline void DOTA_Tuner::SetBatchSize(std::vector<ChangePoint> *change_list,
                                     uint64_t target_value) {
  ChangePoint memtable_size_cp;
  ChangePoint L1_total_size;
  ChangePoint sst_size_cp;
  //  ChangePoint write_buffer_number;

  sst_size_cp.opt = sst_size;
  L1_total_size.opt = total_l1_size;
  // adjust the memtable size
  memtable_size_cp.db_width = false;
  memtable_size_cp.opt = memtable_size;

  target_value = std::max(target_value, min_memtable_size);
  target_value = std::min(target_value, max_memtable_size);

  // SST sizes should be controlled to be the same as memtable size
  memtable_size_cp.value = std::to_string(target_value);
  sst_size_cp.value = std::to_string(target_value);

  // calculate the total size of L1
  uint64_t l1_size = current_opt.level0_file_num_compaction_trigger *
                     current_opt.min_write_buffer_number_to_merge *
                     target_value;

  L1_total_size.value = std::to_string(l1_size);
  sst_size_cp.db_width = false;
  L1_total_size.db_width = false;

  //  change_list->push_back(write_buffer_number);
  change_list->push_back(memtable_size_cp);
  change_list->push_back(L1_total_size);
  change_list->push_back(sst_size_cp);
}

void DOTA_Tuner::FillUpChangeList(std::vector<ChangePoint> *change_list,
                                  TuningOP op) {
  uint64_t current_thread_num = current_opt.max_background_jobs;
  uint64_t current_batch_size = current_opt.write_buffer_size;
  switch (op.BatchOp) {
    case kLinearIncrease:
      SetBatchSize(change_list,
                   current_batch_size += default_opts.write_buffer_size);
      break;
    case kHalf:
      SetBatchSize(change_list, current_batch_size /= 2);
      break;
    case kKeep:
      break;
  }
  switch (op.ThreadOp) {
    case kLinearIncrease:
      SetThreadNum(change_list, current_thread_num += 2);
      break;
    case kHalf:
      SetThreadNum(change_list, current_thread_num /= 2);
      break;
    case kKeep:
      break;
  }
}

SystemScores SystemScores::operator-(const SystemScores &a) {
  SystemScores temp;

  temp.memtable_speed = this->memtable_speed - a.memtable_speed;
  temp.active_size_ratio = this->active_size_ratio - a.active_size_ratio;
  temp.immutable_number = this->immutable_number - a.immutable_number;
  temp.flush_speed_avg = this->flush_speed_avg - a.flush_speed_avg;
  temp.flush_speed_var = this->flush_speed_var - a.flush_speed_var;
  temp.l0_num = this->l0_num - a.l0_num;
  temp.l0_drop_ratio = this->l0_drop_ratio - a.l0_drop_ratio;
  temp.estimate_compaction_bytes =
      this->estimate_compaction_bytes - a.estimate_compaction_bytes;
  temp.disk_bandwidth = this->disk_bandwidth - a.disk_bandwidth;
  temp.compaction_idle_time =
      this->compaction_idle_time - a.compaction_idle_time;
  temp.flush_idle_time = this->flush_idle_time - a.flush_idle_time;
  temp.flush_gap_time = this->flush_gap_time - a.flush_gap_time;
  temp.flush_numbers = this->flush_numbers - a.flush_numbers;

  return temp;
}

SystemScores SystemScores::operator+(const SystemScores &a) {
  SystemScores temp;
  temp.flush_numbers = this->flush_numbers + a.flush_numbers;
  temp.memtable_speed = this->memtable_speed + a.memtable_speed;
  temp.active_size_ratio = this->active_size_ratio + a.active_size_ratio;
  temp.immutable_number = this->immutable_number + a.immutable_number;
  temp.flush_speed_avg = this->flush_speed_avg + a.flush_speed_avg;
  temp.flush_speed_var = this->flush_speed_var + a.flush_speed_var;
  temp.l0_num = this->l0_num + a.l0_num;
  temp.l0_drop_ratio = this->l0_drop_ratio + a.l0_drop_ratio;
  temp.estimate_compaction_bytes =
      this->estimate_compaction_bytes + a.estimate_compaction_bytes;
  temp.disk_bandwidth = this->disk_bandwidth + a.disk_bandwidth;
  temp.compaction_idle_time =
      this->compaction_idle_time + a.compaction_idle_time;
  temp.flush_idle_time = this->flush_idle_time + a.flush_idle_time;
  temp.flush_gap_time = this->flush_gap_time + a.flush_gap_time;
  return temp;
}

SystemScores SystemScores::operator/(const int &a) {
  SystemScores temp;

  temp.memtable_speed = this->memtable_speed / a;
  temp.active_size_ratio = this->active_size_ratio / a;
  temp.immutable_number = this->immutable_number / a;
  temp.l0_num = this->l0_num / a;
  temp.l0_drop_ratio = this->l0_drop_ratio / a;
  temp.estimate_compaction_bytes = this->estimate_compaction_bytes / a;
  temp.disk_bandwidth = this->disk_bandwidth / a;
  temp.compaction_idle_time = this->compaction_idle_time / a;
  temp.flush_idle_time = this->flush_idle_time / a;

  temp.flush_speed_avg = this->flush_numbers == 0
                             ? 0
                             : this->flush_speed_avg / this->flush_numbers;
  temp.flush_speed_var = this->flush_numbers == 0
                             ? 0
                             : this->flush_speed_var / this->flush_numbers;
  temp.flush_gap_time =
      this->flush_numbers == 0 ? 0 : this->flush_gap_time / this->flush_numbers;

  return temp;
}

FEAT_Tuner::~FEAT_Tuner() = default;

void FEAT_Tuner::DetectTuningOperations(int /*secs_elapsed*/,
                                        std::vector<ChangePoint> *change_list) {
  //   first, we tune only when the flushing speed is slower than before
  auto current_score = this->ScoreTheSystem();
  if (current_score.flush_speed_avg == 0) return ;
  scores.push_back(current_score);
  if (scores.size() == 1) {
    return;
  }
  this->UpdateMaxScore(current_score);
  if (scores.size() >= (size_t)this->score_array_len) {
    // remove the first record
    scores.pop_front();
  }
  CalculateAvgScore();

  current_score_ = current_score;
  
//  std::cout << current_score_.flush_speed_avg<< std::endl;


  //  std::cout << current_score_.memtable_speed << "/" <<
  //  avg_scores.memtable_speed
  //            << std::endl;

   //<=avg_scores.memtable_speed * TEA_slow_flush) {

  if (current_score_.flush_speed_avg >0 ){
   TuningOP result{kKeep, kKeep};
   if (TEA_enable) {
      result = TuneByTEA();
   }
    if (FEA_enable) {
      TuningOP fea_result = TuneByFEA();
      result.BatchOp = fea_result.BatchOp;
    }
    FillUpChangeList(change_list, result);

  }
 }

SystemScores FEAT_Tuner::normalize(SystemScores &origin_score) {
  return origin_score;
}

TuningOP FEAT_Tuner::TuneByTEA() {
  // the flushing speed is low.
  TuningOP result{kKeep, kKeep};
 
  if (current_score_.immutable_number >= 1){
    result.ThreadOp = kLinearIncrease;
  }

  if (current_score_.flush_speed_avg < max_scores.flush_speed_avg * TEA_slow_flush && current_score_.flush_speed_avg > 0 ) {
    result.ThreadOp = kHalf;
    std::cout << "slow flush, decrease thread" << std::endl;
  }


  if (current_score_.estimate_compaction_bytes >= 1 || current_score_.l0_num >= 1) {
    result.ThreadOp = kLinearIncrease;
    std::cout << "lo/ro increase, thread" << std::endl;
  }

  return result;
}

TuningOP FEAT_Tuner::TuneByFEA() {
  TuningOP negative_protocol{kKeep, kKeep};

  if (current_score_.flush_speed_avg <
          max_scores.flush_speed_avg * TEA_slow_flush ||
      current_score_.immutable_number > 1) {
    negative_protocol.BatchOp = kLinearIncrease;
    std::cout << "slow flushing, increase batch" << std::endl;
  }

  if (current_score_.immutable_number == 0){
     negative_protocol.BatchOp = kLinearIncrease;
     std::cout << "no flushing, decrease batch" << std::endl;
  
  }

  if (current_score_.estimate_compaction_bytes >= 1) {
    negative_protocol.BatchOp = kHalf;
    std::cout << "ro, decrease batch" << std::endl;
  }
  return negative_protocol;
}
void FEAT_Tuner::CalculateAvgScore() {
  SystemScores result;
  for (auto score : scores) {
    result = result + score;
  }
  if (scores.size() > 0) result = result / scores.size();
  this->avg_scores = result;
}


struct DBWithColumnFamilies {
  std::vector<ColumnFamilyHandle*> cfh;
  DB* db;
#ifndef ROCKSDB_LITE
  OptimisticTransactionDB* opt_txn_db;
#endif  // ROCKSDB_LITE
  std::atomic<size_t> num_created;  // Need to be updated after all the
                                    // new entries in cfh are set.
  size_t num_hot;  // Number of column families to be queried at each moment.
                   // After each CreateNewCf(), another num_hot number of new
                   // Column families will be created and used to be queried.
  port::Mutex create_cf_mutex;  // Only one thread can execute CreateNewCf()

  DBWithColumnFamilies()
      : db(nullptr)
#ifndef ROCKSDB_LITE
        , opt_txn_db(nullptr)
#endif  // ROCKSDB_LITE
  {
    cfh.clear();
    num_created = 0;
    num_hot = 0;
  }

  DBWithColumnFamilies(const DBWithColumnFamilies& other)
      : cfh(other.cfh),
        db(other.db),
#ifndef ROCKSDB_LITE
        opt_txn_db(other.opt_txn_db),
#endif  // ROCKSDB_LITE
        num_created(other.num_created.load()),
        num_hot(other.num_hot) {}

  void DeleteDBs() {
    std::for_each(cfh.begin(), cfh.end(),
                  [](ColumnFamilyHandle* cfhi) { delete cfhi; });
    cfh.clear();
#ifndef ROCKSDB_LITE
    if (opt_txn_db) {
      delete opt_txn_db;
      opt_txn_db = nullptr;
    } else {
      delete db;
      db = nullptr;
    }
#else
    delete db;
    db = nullptr;
#endif  // ROCKSDB_LITE
  }

  ColumnFamilyHandle* GetCfh(int64_t rand_num) {
    assert(num_hot > 0);
    return cfh[num_created.load(std::memory_order_acquire) - num_hot +
               rand_num % num_hot];
  }

  // stage: assume CF from 0 to stage * num_hot has be created. Need to create
  //        stage * num_hot + 1 to stage * (num_hot + 1).
  void CreateNewCf(ColumnFamilyOptions options, int64_t stage) {
    MutexLock l(&create_cf_mutex);
    if ((stage + 1) * num_hot <= num_created) {
      // Already created.
      return;
    }
    auto new_num_created = num_created + num_hot;
    assert(new_num_created <= cfh.size());
    for (size_t i = num_created; i < new_num_created; i++) {
      Status s =
          db->CreateColumnFamily(options, ColumnFamilyName(i), &(cfh[i]));
      if (!s.ok()) {
        fprintf(stderr, "create column family error: %s\n",
                s.ToString().c_str());
        abort();
      }
    }
    num_created.store(new_num_created, std::memory_order_release);
  }
};


typedef std::vector<double> LSM_STATE;
class ReporterAgent {
 private:
  std::string header_string_;

 public:
  static std::string Header() { return "secs_elapsed,interval_qps"; }

  ReporterAgent(Env* env, const std::string& fname,
                uint64_t report_interval_secs,
                std::string header_string = Header())
      : header_string_(header_string),
        env_(env),
        total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    auto s = env_->NewWritableFile(fname, &report_file_, EnvOptions());

    if (s.ok()) {
      s = report_file_->Append(header_string_ + "\n");
      //      std::cout << "opened report file" << std::endl;
    }
    if (s.ok()) {
      s = report_file_->Flush();
    }
    if (!s.ok()) {
      fprintf(stderr, "Can't open %s: %s\n", fname.c_str(),
              s.ToString().c_str());
      abort();
    }
    reporting_thread_ = port::Thread([&]() { SleepAndReport(); });
  }
  virtual ~ReporterAgent();

  // thread safe
  void ReportFinishedOps(int64_t num_ops) {
    total_ops_done_.fetch_add(num_ops);
  }

  virtual void InsertNewTuningPoints(ChangePoint point);

 protected:
  virtual void DetectAndTuning(int secs_elapsed);
  virtual Status ReportLine(int secs_elapsed, int total_ops_done_snapshot);
  Env* env_;
  std::unique_ptr<WritableFile> report_file_;
  std::atomic<int64_t> total_ops_done_;
  int64_t last_report_;
  const uint64_t report_interval_secs_;
  port::Thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  std::condition_variable stop_cv_;
  bool stop_;
  uint64_t time_started;
  void SleepAndReport() {
    time_started = env_->NowMicros();
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_),
                              [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
      }
      auto total_ops_done_snapshot = total_ops_done_.load();
      // round the seconds elapsed
      //      auto secs_elapsed = env_->NowMicros();
      auto secs_elapsed =
          (env_->NowMicros() - time_started + kMicrosInSecond / 2) /
          kMicrosInSecond;
      DetectAndTuning(secs_elapsed);
      auto s = this->ReportLine(secs_elapsed, total_ops_done_snapshot);
      s = report_file_->Append("\n");
      if (s.ok()) {
        s = report_file_->Flush();
      }

      if (!s.ok()) {
        fprintf(stderr,
                "Can't write to report file (%s), stopping the reporting\n",
                s.ToString().c_str());
        break;
      }
      last_report_ = total_ops_done_snapshot;
    }
  }
};

class ReporterAgentWithSILK : public ReporterAgent {
 private:
  bool pausedcompaction = false;
  long prev_bandwidth_compaction_MBPS = 0;
  int FLAGS_value_size = 1000;
  int FLAGS_SILK_bandwidth_limitation = 350;
  DBImpl* running_db_;

 public:
  ReporterAgentWithSILK(DBImpl* running_db, Env* env, const std::string& fname,
                        uint64_t report_interval_secs, int32_t FLAGS_value_size,
                        int32_t bandwidth_limitation);
  Status ReportLine(int secs_elapsed, int total_ops_done_snapshot) override;
};

class ReporterAgentWithTuning : public ReporterAgent {
 private:
  std::vector<ChangePoint> tuning_points;
  DBImpl* running_db_;
  const Options options_when_boost;
  uint64_t last_metrics_collect_secs;
  uint64_t last_compaction_thread_len;
  uint64_t last_flush_thread_len;
  std::map<std::string, void*> string_to_attributes_map;
  std::unique_ptr<DOTA_Tuner> tuner;
  bool applying_changes;
  static std::string DOTAHeader() {
    return "secs_elapsed,interval_qps,batch_size,thread_num";
  }
  int tuning_gap_secs_;
  std::map<std::string, std::string> parameter_map;
  std::map<std::string, int> baseline_map;
  const int thread_num_upper_bound = 12;
  const int thread_num_lower_bound = 2;

 public:
  const static unsigned long history_lsm_shape =
      10;  // Recorded history lsm shape, here we record 10 secs
  std::deque<LSM_STATE> shape_list;
  const size_t default_memtable_size = 64 << 20;
  const float threashold = 0.5;
  ReporterAgentWithTuning(DBImpl* running_db, Env* env,
                          const std::string& fname,
                          uint64_t report_interval_secs,
                          uint64_t dota_tuning_gap_secs = 1);
  DOTA_Tuner* GetTuner() { return tuner.get(); }
  void ApplyChangePointsInstantly(std::vector<ChangePoint>* points);

  void DetectChangesPoints(int sec_elapsed);

  void PopChangePoints(int secs_elapsed);

  static bool thread_idle_cmp(std::pair<size_t, uint64_t> p1,
                              std::pair<size_t, uint64_t> p2) {
    return p1.second < p2.second;
  }

  Status ReportLine(int secs_elapsed, int total_ops_done_snapshot) override;
  void UseFEATTuner(bool TEA_enable, bool FEA_enable);
  //  void print_useless_thing(int secs_elapsed);
  void DetectAndTuning(int secs_elapsed) override;
  enum CongestionStatus {
    kCongestion,
    kReachThreshold,
    kUnderThreshold,
    kIgnore
  };

  struct BatchSizeScore {
    double l0_stall;
    double memtable_stall;
    double pending_bytes_stall;
    double flushing_congestion;
    double read_amp_score;
    std::string ToString() {
      std::stringstream ss;
      ss << "l0 stall: " << l0_stall << " memtable stall: " << memtable_stall
         << " pending bytes: " << pending_bytes_stall
         << " flushing congestion: " << flushing_congestion
         << " reading performance score: " << read_amp_score;
      return ss.str();
    }
    std::string Differences() { return "batch size"; }
  };
  struct ThreadNumScore {
    double l0_stall;
    double memtable_stall;
    double pending_bytes_stall;
    double flushing_congestion;
    double thread_idle;
    std::string ToString() {
      std::stringstream ss;
      ss << "l0 stall: " << l0_stall << " memtable stall: " << memtable_stall
         << " pending bytes: " << pending_bytes_stall
         << " flushing congestion: " << flushing_congestion
         << " thread_idle: " << thread_idle;
      return ss.str();
    }
  };

  TuningOP VoteForThread(ThreadNumScore& scores);
  TuningOP VoteForMemtable(BatchSizeScore& scores);

};  // end ReporterWithTuning
typedef ReporterAgent DOTAAgent;
class ReporterWithMoreDetails : public ReporterAgent {
 private:
  DBImpl* db_ptr;
  std::string detailed_header() {
    return ReporterAgent::Header() + ",immutables" + ",total_mem_size" +
           ",l0_files" + ",all_sst_size" + ",live_data_size" + ",pending_bytes";
  }

 public:
  ReporterWithMoreDetails(DBImpl* running_db, Env* env,
                          const std::string& fname,
                          uint64_t report_interval_secs)
      : ReporterAgent(env, fname, report_interval_secs, detailed_header()) {
    if (running_db == nullptr) {
      std::cout << "Missing parameter db_ to record more details" << std::endl;
      abort();

    } else {
      db_ptr = reinterpret_cast<DBImpl*>(running_db);
    }
  }

  void DetectAndTuning(int secs_elapsed) override;

  Status ReportLine(int secs_elapsed, int total_ops_done_snapshot) override {
    auto opt = this->db_ptr->GetOptions();

    //    current_opt = db_ptr->GetOptions();
    auto version =
        db_ptr->GetVersionSet()->GetColumnFamilySet()->GetDefault()->current();
    auto cfd = version->cfd();
    auto vfs = version->storage_info();
    int l0_files = vfs->NumLevelFiles(0);
    uint64_t total_mem_size = 0;
    //    uint64_t active_mem = 0;
    db_ptr->GetIntProperty("rocksdb.size-all-mem-tables", &total_mem_size);
    //    db_ptr->GetIntProperty("rocksdb.cur-size-active-mem-table",
    //    &active_mem);

    uint64_t compaction_pending_bytes =
        vfs->estimated_compaction_needed_bytes();
    uint64_t live_data_size = vfs->EstimateLiveDataSize();
    uint64_t all_sst_size = 0;
    int immutable_memtables = cfd->imm()->NumNotFlushed();
    for (int i = 0; i < vfs->num_levels(); i++) {
      all_sst_size += vfs->NumLevelBytes(i);
    }

    std::string report =
        std::to_string(secs_elapsed) + "," +
        std::to_string(total_ops_done_snapshot - last_report_) + "," +
        std::to_string(immutable_memtables) + "," + std::to_string(total_mem_size) + "," +
        std::to_string(l0_files) + "," + std::to_string(all_sst_size) + "," +
        std::to_string(live_data_size) + "," + std::to_string(compaction_pending_bytes);
    //    std::cout << report << std::endl;
    auto s = report_file_->Append(report);
    return s;
  }
};

ReporterAgent::~ReporterAgent() {
  {
    std::unique_lock<std::mutex> lk(mutex_);
    stop_ = true;
    stop_cv_.notify_all();
  }
  reporting_thread_.join();
}
void ReporterAgent::InsertNewTuningPoints(ChangePoint point) {
  std::cout << "can't use change point @ " << point.change_timing
            << " Due to using default reporter" << std::endl;
};
void ReporterAgent::DetectAndTuning(int /*secs_elapsed*/) {}
Status ReporterAgent::ReportLine(int secs_elapsed,
                                 int total_ops_done_snapshot) {
  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_);
  auto s = report_file_->Append(report);
  return s;
}

void ReporterAgentWithTuning::DetectChangesPoints(int sec_elapsed) {
  std::vector<ChangePoint> change_points;
  if (applying_changes) {
    return;
  }
  tuner->DetectTuningOperations(sec_elapsed, &change_points);
  ApplyChangePointsInstantly(&change_points);
}

void ReporterAgentWithTuning::DetectAndTuning(int secs_elapsed) {
  if (secs_elapsed % tuning_gap_secs_ == 0) {
    DetectChangesPoints(secs_elapsed);
    //    this->running_db_->immutable_db_options().job_stats->clear();
    last_metrics_collect_secs = secs_elapsed;
  }
  if (tuning_points.empty() ||
      tuning_points.front().change_timing < secs_elapsed) {
    return;
  } else {
    PopChangePoints(secs_elapsed);
  }
}

Status ReporterAgentWithTuning::ReportLine(int secs_elapsed,
                                           int total_ops_done_snapshot) {
  auto opt = this->running_db_->GetOptions();

  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_) + "," +
                       std::to_string(opt.write_buffer_size >> 20) + "," +
                       std::to_string(opt.max_background_jobs);
  auto s = report_file_->Append(report);
  return s;
}
void ReporterAgentWithTuning::UseFEATTuner(bool TEA_enable, bool FEA_enable) {
  tuner.release();
  tuner.reset(new FEAT_Tuner(options_when_boost, running_db_, &last_report_,
                             &total_ops_done_, env_, tuning_gap_secs_,
                             TEA_enable, FEA_enable));
};

Status update_db_options(
    DBImpl* running_db_,
    std::unordered_map<std::string, std::string>* new_db_options,
    bool* applying_changes, Env* /*env*/) {
  *applying_changes = true;
  Status s = running_db_->SetDBOptions(*new_db_options);
  free(new_db_options);
  *applying_changes = false;
  return s;
}

Status update_cf_options(
    DBImpl* running_db_,
    std::unordered_map<std::string, std::string>* new_cf_options,
    bool* applying_changes, Env* /*env*/) {
  *applying_changes = true;
  Status s = running_db_->SetOptions(*new_cf_options);
  free(new_cf_options);
  *applying_changes = false;
  return s;
}

Status SILK_pause_compaction(DBImpl* running_db_, bool* stopped) {
  Status s = running_db_->PauseBackgroundWork();
  *stopped = true;
  return s;
}

Status SILK_resume_compaction(DBImpl* running_db_, bool* stopped) {
  Status s = running_db_->ContinueBackgroundWork();
  *stopped = false;
  return s;
}

void ReporterAgentWithTuning::ApplyChangePointsInstantly(
    std::vector<ChangePoint>* points) {
  std::unordered_map<std::string, std::string>* new_cf_options;
  std::unordered_map<std::string, std::string>* new_db_options;

  new_cf_options = new std::unordered_map<std::string, std::string>();
  new_db_options = new std::unordered_map<std::string, std::string>();
  if (points->empty()) {
    return;
  }

  for (auto point : *points) {
    if (point.db_width) {
      new_db_options->emplace(point.opt, point.value);
    } else {
      new_cf_options->emplace(point.opt, point.value);
    }
  }
  points->clear();
  Status s;
  if (!new_db_options->empty()) {
    //    std::thread t();
    std::thread t(update_db_options, running_db_, new_db_options,
                  &applying_changes, env_);
    t.detach();
  }
  if (!new_cf_options->empty()) {
    std::thread t(update_cf_options, running_db_, new_cf_options,
                  &applying_changes, env_);
    t.detach();
  }
}

ReporterAgentWithTuning::ReporterAgentWithTuning(DBImpl* running_db, Env* env,
                                                 const std::string& fname,
                                                 uint64_t report_interval_secs,
                                                 uint64_t dota_tuning_gap_secs)
    : ReporterAgent(env, fname, report_interval_secs, DOTAHeader()),
      options_when_boost(running_db->GetOptions()) {
  tuning_points = std::vector<ChangePoint>();
  tuning_points.clear();
  std::cout << "using reporter agent with change points." << std::endl;
  if (running_db == nullptr) {
    std::cout << "Missing parameter db_ to apply changes" << std::endl;
    abort();
  } else {
    running_db_ = running_db;
  }
  this->tuning_gap_secs_ = std::max(dota_tuning_gap_secs, report_interval_secs);
  this->last_metrics_collect_secs = 0;
  this->last_compaction_thread_len = 0;
  this->last_flush_thread_len = 0;
  tuner.reset(new DOTA_Tuner(options_when_boost, running_db_, &last_report_,
                             &total_ops_done_, env_, tuning_gap_secs_));
  tuner->ResetTuner();
  this->applying_changes = false;
}

inline double average(std::vector<double>& v) {
  assert(!v.empty());
  return accumulate(v.begin(), v.end(), 0.0) / v.size();
}

void ReporterAgentWithTuning::PopChangePoints(int secs_elapsed) {
  std::vector<ChangePoint> valid_point;
  for (auto it = tuning_points.begin(); it != tuning_points.end(); it++) {
    if (it->change_timing <= secs_elapsed) {
      if (running_db_ != nullptr) {
        valid_point.push_back(*it);
      }
      tuning_points.erase(it--);
    }
  }
  ApplyChangePointsInstantly(&valid_point);
}

void ReporterWithMoreDetails::DetectAndTuning(int secs_elapsed) {
  //  report_file_->Append(",");
  //  ReportLine(secs_elapsed, total_ops_done_);
  secs_elapsed++;
}

Status ReporterAgentWithSILK::ReportLine(int secs_elapsed,
                                         int total_ops_done_snapshot) {
  // copy from SILK https://github.com/theoanab/SILK-USENIXATC2019
  // //check the current bandwidth for user operations
  long cur_throughput = (total_ops_done_snapshot - last_report_);
  long cur_bandwidth_user_ops_MBPS =
      cur_throughput * FLAGS_value_size / 1000000;

  // SILK TESTING the Pause compaction work functionality
  if (!pausedcompaction &&
      cur_bandwidth_user_ops_MBPS > FLAGS_SILK_bandwidth_limitation * 0.75) {
    // SILK Consider this a load peak
    //    running_db_->PauseCompactionWork();
    //    pausedcompaction = true;
    pausedcompaction = true;
    std::thread t(SILK_pause_compaction, running_db_, &pausedcompaction);
    t.detach();

  } else if (pausedcompaction && cur_bandwidth_user_ops_MBPS <=
                                     FLAGS_SILK_bandwidth_limitation * 0.75) {
    std::thread t(SILK_resume_compaction, running_db_, &pausedcompaction);
    t.detach();
  }

  long cur_bandiwdth_compaction_MBPS =
      FLAGS_SILK_bandwidth_limitation -
      cur_bandwidth_user_ops_MBPS;  // measured 200MB/s SSD bandwidth on XEON.
  if (cur_bandiwdth_compaction_MBPS < 10) {
    cur_bandiwdth_compaction_MBPS = 10;
  }
  if (abs(prev_bandwidth_compaction_MBPS - cur_bandiwdth_compaction_MBPS) >=
      10) {
    auto opt = running_db_->GetOptions();
    opt.rate_limiter.get()->SetBytesPerSecond(cur_bandiwdth_compaction_MBPS *
                                              1000 * 1000);
    prev_bandwidth_compaction_MBPS = cur_bandiwdth_compaction_MBPS;
  }
  // Adjust the tuner from SILK before reporting
  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_) + "," +
                       std::to_string(cur_bandwidth_user_ops_MBPS);
  auto s = report_file_->Append(report);
  return s;
}

ReporterAgentWithSILK::ReporterAgentWithSILK(DBImpl* running_db, Env* env,
                                             const std::string& fname,
                                             uint64_t report_interval_secs,
                                             int32_t value_size,
                                             int32_t bandwidth_limitation)
    : ReporterAgent(env, fname, report_interval_secs) {
  std::cout << "SILK enabled, Disk bandwidth has been set to: "
            << bandwidth_limitation << std::endl;
  running_db_ = running_db;
  this->FLAGS_value_size = value_size;
  this->FLAGS_SILK_bandwidth_limitation = bandwidth_limitation;
}


enum OperationType : unsigned char {
  kRead = 0,
  kReadDisk,
  kWrite,
  kWriteLarge,
  kDelete,
  kSeek,
  kMerge,
  kUpdate,
  kReadModifyWrite,
  kCompress,
  kUncompress,
  kCrc,
  kHash,
  kOthers
};

static std::unordered_map<OperationType, std::string, std::hash<unsigned char>>
                          OperationTypeString = {
  {kRead, "read"},
  {kReadDisk, "readDisk"},
  {kWrite, "write"},
  {kWriteLarge, "writeLarge"},
  {kDelete, "delete"},
  {kSeek, "seek"},
  {kMerge, "merge"},
  {kUpdate, "update"},
  {kReadModifyWrite,"read-modify-write"},
  {kCompress, "compress"},
  {kCompress, "uncompress"},
  {kCrc, "crc"},
  {kHash, "hash"},
  {kOthers, "op"}
};

class CombinedStats;
class Stats {
 private:
  int id_;
  uint64_t start_;
  uint64_t finish_;
  double seconds_;
  uint64_t done_;
  uint64_t last_report_done_;
  uint64_t next_report_;
  uint64_t bytes_;
  uint64_t last_op_finish_;
  uint64_t last_report_finish_;
  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  std::string message_;
  bool exclude_from_merge_;
  ReporterAgent* reporter_agent_;  // does not own
  friend class CombinedStats;

 public:
  Stats() { Start(-1); }

  void SetReporterAgent(ReporterAgent* reporter_agent) {
    reporter_agent_ = reporter_agent;
  }

  void Start(int id) {
    id_ = id;
    next_report_ = FLAGS_stats_interval ? FLAGS_stats_interval : 100;
    last_op_finish_ = start_;
    hist_.clear();
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = FLAGS_env->NowMicros();
    finish_ = start_;
    last_report_finish_ = start_;
    message_.clear();
    // When set, stats from this thread won't be merged with others.
    exclude_from_merge_ = false;
  }

  void Merge(const Stats& other) {
    if (other.exclude_from_merge_)
      return;

    for (auto it = other.hist_.begin(); it != other.hist_.end(); ++it) {
      auto this_it = hist_.find(it->first);
      if (this_it != hist_.end()) {
        this_it->second->Merge(*(other.hist_.at(it->first)));
      } else {
        hist_.insert({ it->first, it->second });
      }
    }

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = FLAGS_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void SetId(int id) { id_ = id; }
  void SetExcludeFromMerge() { exclude_from_merge_ = true; }

  void PrintThreadStatus() {
    std::vector<ThreadStatus> thread_list;
    FLAGS_env->GetThreadList(&thread_list);

    fprintf(stderr, "\n%18s %10s %12s %20s %13s %45s %12s %s\n",
        "ThreadID", "ThreadType", "cfName", "Operation",
        "ElapsedTime", "Stage", "State", "OperationProperties");

    int64_t current_time = 0;
    Env::Default()->GetCurrentTime(&current_time);
    for (auto ts : thread_list) {
      fprintf(stderr, "%18" PRIu64 " %10s %12s %20s %13s %45s %12s",
          ts.thread_id,
          ThreadStatus::GetThreadTypeName(ts.thread_type).c_str(),
          ts.cf_name.c_str(),
          ThreadStatus::GetOperationName(ts.operation_type).c_str(),
          ThreadStatus::MicrosToString(ts.op_elapsed_micros).c_str(),
          ThreadStatus::GetOperationStageName(ts.operation_stage).c_str(),
          ThreadStatus::GetStateName(ts.state_type).c_str());

      auto op_properties = ThreadStatus::InterpretOperationProperties(
          ts.operation_type, ts.op_properties);
      for (const auto& op_prop : op_properties) {
        fprintf(stderr, " %s %" PRIu64" |",
            op_prop.first.c_str(), op_prop.second);
      }
      fprintf(stderr, "\n");
    }
  }

  void ResetLastOpTime() {
    // Set to now to avoid latency from calls to SleepForMicroseconds
    last_op_finish_ = FLAGS_env->NowMicros();
  }


  long FinishedOpsQUEUES(DBWithColumnFamilies* db_with_cfh, DB* db, int64_t num_ops, 
                    uint64_t op_start_time, enum OperationType op_type = kOthers) { //num_ops is 1 
    
    long cur_ops_interval = 0;
    if (reporter_agent_) {
      reporter_agent_->ReportFinishedOps(num_ops);
    }
    if (FLAGS_histogram) {
      uint64_t now = FLAGS_env->NowMicros();
      uint64_t micros = now - op_start_time;

      if (hist_.find(op_type) == hist_.end())
      {
        auto hist_temp = std::make_shared<HistogramImpl>();
        hist_.insert({op_type, std::move(hist_temp)});
      }
      hist_[op_type]->Add(micros);

      if (micros > 20000 && !FLAGS_stats_interval) {
        fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
      if (!FLAGS_stats_interval) {
        if      (next_report_ < 1000)   next_report_ += 100;
        else if (next_report_ < 5000)   next_report_ += 500;
        else if (next_report_ < 10000)  next_report_ += 1000;
        else if (next_report_ < 50000)  next_report_ += 5000;
        else if (next_report_ < 100000) next_report_ += 10000;
        else if (next_report_ < 500000) next_report_ += 50000;
        else                            next_report_ += 100000;
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      } else {
        uint64_t now = FLAGS_env->NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.

        if (FLAGS_stats_interval_seconds &&
            usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations
          next_report_ += FLAGS_stats_interval;

        } else {

          fprintf(stderr,
                  "%s ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  FLAGS_env->TimeToString(now/1000000).c_str(),
                  id_,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) /
                  (usecs_since_last / 1000000.0),
                  done_ / ((now - start_) / 1000000.0),
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);

          cur_ops_interval = (done_ - last_report_done_) /
                  (usecs_since_last / 1000000.0);

          for (auto it = hist_.begin(); it != hist_.end(); ++it) {

                fprintf(stdout, "Microseconds per %s %d :\n%.200s\n",
                OperationTypeString[it->first].c_str(), id_,
                it->second->ToString().c_str());
           }
          
          //TODO: maybe reset the histogram?

          if (id_ == 1 && FLAGS_stats_per_interval) {
            std::string stats;

            if (db_with_cfh && db_with_cfh->num_created.load()) {
              for (size_t i = 0; i < db_with_cfh->num_created.load(); ++i) {
                if (db->GetProperty(db_with_cfh->cfh[i], "rocksdb.cfstats",
                                    &stats))
                  fprintf(stderr, "%s\n", stats.c_str());
                if (FLAGS_show_table_properties) {
                  for (int level = 0; level < FLAGS_num_levels; ++level) {
                    if (db->GetProperty(
                            db_with_cfh->cfh[i],
                            "rocksdb.aggregated-table-properties-at-level" +
                                ToString(level),
                            &stats)) {
                      if (stats.find("# entries=0") == std::string::npos) {
                        fprintf(stderr, "Level[%d]: %s\n", level,
                                stats.c_str());
                      }
                    }
                  }
                }
              }
            } else if (db) {
              if (db->GetProperty("rocksdb.stats", &stats)) {
                fprintf(stderr, "%s\n", stats.c_str());
              }
              if (FLAGS_show_table_properties) {
                for (int level = 0; level < FLAGS_num_levels; ++level) {
                  if (db->GetProperty(
                          "rocksdb.aggregated-table-properties-at-level" +
                              ToString(level),
                          &stats)) {
                    if (stats.find("# entries=0") == std::string::npos) {
                      fprintf(stderr, "Level[%d]: %s\n", level, stats.c_str());
                    }
                  }
                }
              }
            }
          }

          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }
      if (id_ ==1 && FLAGS_thread_status_per_interval) {
        PrintThreadStatus();
      }
      fflush(stderr);
    }

    return cur_ops_interval;
  }

  void FinishedOps(DBWithColumnFamilies* db_with_cfh, DB* db, int64_t num_ops,
                   enum OperationType op_type = kOthers) {
    if (reporter_agent_) {
      reporter_agent_->ReportFinishedOps(num_ops);
    }
    if (FLAGS_histogram) {
      uint64_t now = FLAGS_env->NowMicros();
      uint64_t micros = now - last_op_finish_;

      if (hist_.find(op_type) == hist_.end())
      {
        auto hist_temp = std::make_shared<HistogramImpl>();
        hist_.insert({op_type, std::move(hist_temp)});
      }
      hist_[op_type]->Add(micros);

      if (micros > 20000 && !FLAGS_stats_interval) {
        fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
      if (!FLAGS_stats_interval) {
        if      (next_report_ < 1000)   next_report_ += 100;
        else if (next_report_ < 5000)   next_report_ += 500;
        else if (next_report_ < 10000)  next_report_ += 1000;
        else if (next_report_ < 50000)  next_report_ += 5000;
        else if (next_report_ < 100000) next_report_ += 10000;
        else if (next_report_ < 500000) next_report_ += 50000;
        else                            next_report_ += 100000;
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      } else {
        uint64_t now = FLAGS_env->NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.

        if (FLAGS_stats_interval_seconds &&
            usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations
          next_report_ += FLAGS_stats_interval;

        } else {

          fprintf(stderr,
                  "%s ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  FLAGS_env->TimeToString(now/1000000).c_str(),
                  id_,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) /
                  (usecs_since_last / 1000000.0),
                  done_ / ((now - start_) / 1000000.0),
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);

          for (auto it = hist_.begin(); it != hist_.end(); ++it) {

                fprintf(stdout, "Microseconds per %s %d :\n%.200s\n",
                OperationTypeString[it->first].c_str(), id_,
                it->second->ToString().c_str());
           }
          
          //TODO: maybe reset the histogram?

          if (id_ == 1 && FLAGS_stats_per_interval) {
            std::string stats;

            if (db_with_cfh && db_with_cfh->num_created.load()) {
              for (size_t i = 0; i < db_with_cfh->num_created.load(); ++i) {
                if (db->GetProperty(db_with_cfh->cfh[i], "rocksdb.cfstats",
                                    &stats))
                  fprintf(stderr, "%s\n", stats.c_str());
                if (FLAGS_show_table_properties) {
                  for (int level = 0; level < FLAGS_num_levels; ++level) {
                    if (db->GetProperty(
                            db_with_cfh->cfh[i],
                            "rocksdb.aggregated-table-properties-at-level" +
                                ToString(level),
                            &stats)) {
                      if (stats.find("# entries=0") == std::string::npos) {
                        fprintf(stderr, "Level[%d]: %s\n", level,
                                stats.c_str());
                      }
                    }
                  }
                }
              }
            } else if (db) {
              if (db->GetProperty("rocksdb.stats", &stats)) {
                fprintf(stderr, "%s\n", stats.c_str());
              }
              if (FLAGS_show_table_properties) {
                for (int level = 0; level < FLAGS_num_levels; ++level) {
                  if (db->GetProperty(
                          "rocksdb.aggregated-table-properties-at-level" +
                              ToString(level),
                          &stats)) {
                    if (stats.find("# entries=0") == std::string::npos) {
                      fprintf(stderr, "Level[%d]: %s\n", level, stats.c_str());
                    }
                  }
                }
              }
            }
          }

          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }
      if (id_ ==1 && FLAGS_thread_status_per_interval) {
        PrintThreadStatus();
      }
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedOps().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);
    double elapsed = (finish_ - start_) * 1e-6;
    double throughput = (double)done_/elapsed;

    fprintf(stdout, "%-12s : %11.3f micros/op %ld ops/sec;%s%s\n",
            name.ToString().c_str(),
            elapsed * 1e6 / done_,
            (long)throughput,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      for (auto it = hist_.begin(); it != hist_.end(); ++it) {
        fprintf(stdout, "Microseconds per %s:\n%s\n",
                OperationTypeString[it->first].c_str(),
                it->second->ToString().c_str());
      }
    }
    if (FLAGS_report_file_operations) {
      ReportFileOpEnv* env = static_cast<ReportFileOpEnv*>(FLAGS_env);
      ReportFileOpCounters* counters = env->counters();
      fprintf(stdout, "Num files opened: %d\n",
              counters->open_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num Read(): %d\n",
              counters->read_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num Append(): %d\n",
              counters->append_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num bytes read: %" PRIu64 "\n",
              counters->bytes_read_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num bytes written: %" PRIu64 "\n",
              counters->bytes_written_.load(std::memory_order_relaxed));
      env->reset();
    }
    fflush(stdout);
  }
};

class CombinedStats {
 public:
  void AddStats(const Stats& stat) {
    uint64_t total_ops = stat.done_;
    uint64_t total_bytes_ = stat.bytes_;
    double elapsed;

    if (total_ops < 1) {
      total_ops = 1;
    }

    elapsed = (stat.finish_ - stat.start_) * 1e-6;
    throughput_ops_.emplace_back(total_ops / elapsed);

    if (total_bytes_ > 0) {
      double mbs = (total_bytes_ / 1048576.0);
      throughput_mbs_.emplace_back(mbs / elapsed);
    }
  }

  void Report(const std::string& bench_name) {
    const char* name = bench_name.c_str();
    int num_runs = static_cast<int>(throughput_ops_.size());

    if (throughput_mbs_.size() == throughput_ops_.size()) {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d ops/sec; %6.1f MB/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec; %6.1f MB/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)),
              CalcAvg(throughput_mbs_), name, num_runs,
              static_cast<int>(CalcMedian(throughput_ops_)),
              CalcMedian(throughput_mbs_));
    } else {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d ops/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)), name,
              num_runs, static_cast<int>(CalcMedian(throughput_ops_)));
    }
  }

 private:
  double CalcAvg(std::vector<double> data) {
    double avg = 0;
    for (double x : data) {
      avg += x;
    }
    avg = avg / data.size();
    return avg;
  }

  double CalcMedian(std::vector<double> data) {
    assert(data.size() > 0);
    std::sort(data.begin(), data.end());

    size_t mid = data.size() / 2;
    if (data.size() % 2 == 1) {
      // Odd number of entries
      return data[mid];
    } else {
      // Even number of entries
      return (data[mid] + data[mid - 1]) / 2;
    }
  }

  std::vector<double> throughput_ops_;
  std::vector<double> throughput_mbs_;
};

class TimestampEmulator {
 private:
  std::atomic<uint64_t> timestamp_;

 public:
  TimestampEmulator() : timestamp_(0) {}
  uint64_t Get() const { return timestamp_.load(); }
  void Inc() { timestamp_++; }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;
  int perf_level;
  std::shared_ptr<RateLimiter> write_rate_limiter;
  std::shared_ptr<RateLimiter> read_rate_limiter;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  long num_initialized;
  long num_done;
  bool start;

  std::queue<std::pair<int, std::chrono::microseconds>> op_queues[5][8]; //only used in QueuingMeasurements
  int send_low_workload; //only used in FluctuatingThroughput test for generating high and low throughput
  int peak_sleep_time; //only used in ProductionWorkloadTest test for generating high and low throughput
  int high_peak; // used in LongPeakTest test for generating a high peak
  long cur_ops_interval;

  SharedState() : cv(&mu), perf_level(FLAGS_perf_level) { }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Random64 rand;         // Has different seeds for different threads
  Stats stats;
  
  SharedState* shared;
  int last_thread_bucket1;//only used in QueuingMeasurements test for load generator thread
  int last_thread_bucket2;//only used in QueuingMeasurements test for load generator thread

  //int queue[1000000];
  bool init =false;
  /* implicit */ ThreadState(int index)
      : tid(index),
        rand((FLAGS_seed ? FLAGS_seed : 1000) + index) {
  }
};

class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_seconds_ = max_seconds;
    max_ops_= max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = FLAGS_env->NowMicros();
  }

  int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;    // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = FLAGS_ops_between_duration_checks;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = FLAGS_env->NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};

class Benchmark {
 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> compressed_cache_;
  std::shared_ptr<const FilterPolicy> filter_policy_;
  const SliceTransform* prefix_extractor_;
  DBWithColumnFamilies db_;
  std::vector<DBWithColumnFamilies> multi_dbs_;
  int64_t num_;
  int value_size_;
  int key_size_;
  int prefix_size_;
  int64_t keys_per_prefix_;
  int64_t entries_per_batch_;
  int64_t writes_per_range_tombstone_;
  int64_t range_tombstone_width_;
  int64_t max_num_range_tombstones_;
  WriteOptions write_options_;
  ColumnFamilyOptions col_fam_options_;
  Options open_options_;  // keep options around to properly destroy db later
  int64_t reads_;
  int64_t deletes_;
  double read_random_exp_range_;
  int64_t writes_;
  int64_t readwrites_;
  int64_t merge_keys_;
  bool report_file_operations_;
  bool use_blob_db_;

  bool SanityCheck() {
    if (FLAGS_compression_ratio > 1) {
      fprintf(stderr, "compression_ratio should be between 0 and 1\n");
      return false;
    }
    return true;
  }

  inline bool CompressSlice(const Slice& input, std::string* compressed) {
    bool ok = true;
    switch (FLAGS_compression_type_e) {
      case rocksdb::kSnappyCompression:
        ok = Snappy_Compress(Options().compression_opts, input.data(),
                             input.size(), compressed);
        break;
      case rocksdb::kZlibCompression:
        ok = Zlib_Compress(Options().compression_opts, 2, input.data(),
                           input.size(), compressed);
        break;
      case rocksdb::kBZip2Compression:
        ok = BZip2_Compress(Options().compression_opts, 2, input.data(),
                            input.size(), compressed);
        break;
      case rocksdb::kLZ4Compression:
        ok = LZ4_Compress(Options().compression_opts, 2, input.data(),
                          input.size(), compressed);
        break;
      case rocksdb::kLZ4HCCompression:
        ok = LZ4HC_Compress(Options().compression_opts, 2, input.data(),
                            input.size(), compressed);
        break;
      case rocksdb::kXpressCompression:
        ok = XPRESS_Compress(input.data(),
          input.size(), compressed);
        break;
      case rocksdb::kZSTD:
        ok = ZSTD_Compress(Options().compression_opts, input.data(),
                           input.size(), compressed);
        break;
      default:
        ok = false;
    }
    return ok;
  }

  void PrintHeader() {
    PrintEnvironment();
    fprintf(stdout, "Keys:       %d bytes each\n", FLAGS_key_size);
    fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
            FLAGS_value_size,
            static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %" PRIu64 "\n", num_);
    fprintf(stdout, "Prefix:    %d bytes\n", FLAGS_prefix_size);
    fprintf(stdout, "Keys per prefix:    %" PRIu64 "\n", keys_per_prefix_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(FLAGS_key_size + FLAGS_value_size) * num_)
             / 1048576.0));
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((FLAGS_key_size + FLAGS_value_size * FLAGS_compression_ratio)
              * num_)
             / 1048576.0));
    fprintf(stdout, "Write rate: %" PRIu64 " bytes/second\n",
            FLAGS_benchmark_write_rate_limit);
    fprintf(stdout, "Read rate: %" PRIu64 " ops/second\n",
            FLAGS_benchmark_read_rate_limit);
    if (FLAGS_enable_numa) {
      fprintf(stderr, "Running in NUMA enabled mode.\n");
#ifndef NUMA
      fprintf(stderr, "NUMA is not defined in the system.\n");
      exit(1);
#else
      if (numa_available() == -1) {
        fprintf(stderr, "NUMA is not supported by the system.\n");
        exit(1);
      }
#endif
    }

    auto compression = CompressionTypeToString(FLAGS_compression_type_e);
    fprintf(stdout, "Compression: %s\n", compression.c_str());

    switch (FLAGS_rep_factory) {
      case kPrefixHash:
        fprintf(stdout, "Memtablerep: prefix_hash\n");
        break;
      case kSkipList:
        fprintf(stdout, "Memtablerep: skip_list\n");
        break;
      case kVectorRep:
        fprintf(stdout, "Memtablerep: vector\n");
        break;
      case kHashLinkedList:
        fprintf(stdout, "Memtablerep: hash_linkedlist\n");
        break;
      case kCuckoo:
        fprintf(stdout, "Memtablerep: cuckoo\n");
        break;
    }
    fprintf(stdout, "Perf Level: %d\n", FLAGS_perf_level);

    PrintWarnings(compression.c_str());
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings(const char* compression) {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
    if (FLAGS_compression_type_e != rocksdb::kNoCompression) {
      // The test string should not be too small.
      const int len = FLAGS_block_size;
      std::string input_str(len, 'y');
      std::string compressed;
      bool result = CompressSlice(Slice(input_str), &compressed);

      if (!result) {
        fprintf(stdout, "WARNING: %s compression is not enabled\n",
                compression);
      } else if (compressed.size() >= input_str.size()) {
        fprintf(stdout, "WARNING: %s compression is not effective\n",
                compression);
      }
    }
  }

// Current the following isn't equivalent to OS_LINUX.
#if defined(__linux)
  static Slice TrimSpace(Slice s) {
    unsigned int start = 0;
    while (start < s.size() && isspace(s[start])) {
      start++;
    }
    unsigned int limit = static_cast<unsigned int>(s.size());
    while (limit > start && isspace(s[limit-1])) {
      limit--;
    }
    return Slice(s.data() + start, limit - start);
  }
#endif

  void PrintEnvironment() {
    fprintf(stderr, "RocksDB:    version %d.%d\n",
            kMajorVersion, kMinorVersion);

#if defined(__linux)
    time_t now = time(nullptr);
    char buf[52];
    // Lint complains about ctime() usage, so replace it with ctime_r(). The
    // requirement is to provide a buffer which is at least 26 bytes.
    fprintf(stderr, "Date:       %s",
            ctime_r(&now, buf));  // ctime_r() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char* sep = strchr(line, ':');
        if (sep == nullptr) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

  static bool KeyExpired(const TimestampEmulator* timestamp_emulator,
                         const Slice& key) {
    const char* pos = key.data();
    pos += 8;
    uint64_t timestamp = 0;
    if (port::kLittleEndian) {
      int bytes_to_fill = 8;
      for (int i = 0; i < bytes_to_fill; ++i) {
        timestamp |= (static_cast<uint64_t>(static_cast<unsigned char>(pos[i]))
                      << ((bytes_to_fill - i - 1) << 3));
      }
    } else {
      memcpy(&timestamp, pos, sizeof(timestamp));
    }
    return timestamp_emulator->Get() - timestamp > FLAGS_time_range;
  }

  class ExpiredTimeFilter : public CompactionFilter {
   public:
    explicit ExpiredTimeFilter(
        const std::shared_ptr<TimestampEmulator>& timestamp_emulator)
        : timestamp_emulator_(timestamp_emulator) {}
    bool Filter(int level, const Slice& key, const Slice& existing_value,
                std::string* new_value, bool* value_changed) const override {
      return KeyExpired(timestamp_emulator_.get(), key);
    }
    const char* Name() const override { return "ExpiredTimeFilter"; }

   private:
    std::shared_ptr<TimestampEmulator> timestamp_emulator_;
  };

  std::shared_ptr<Cache> NewCache(int64_t capacity) {
    if (capacity <= 0) {
      return nullptr;
    }
    if (FLAGS_use_clock_cache) {
      auto cache = NewClockCache((size_t)capacity, FLAGS_cache_numshardbits);
      if (!cache) {
        fprintf(stderr, "Clock cache not supported.");
        exit(1);
      }
      return cache;
    } else {
      return NewLRUCache((size_t)capacity, FLAGS_cache_numshardbits,
                         false /*strict_capacity_limit*/,
                         FLAGS_cache_high_pri_pool_ratio);
    }
  }

 public:
  Benchmark()
      : cache_(NewCache(FLAGS_cache_size)),
        compressed_cache_(NewCache(FLAGS_compressed_cache_size)),
        filter_policy_(FLAGS_bloom_bits >= 0
                           ? NewBloomFilterPolicy(FLAGS_bloom_bits,
                                                  FLAGS_use_block_based_filter)
                           : nullptr),
        prefix_extractor_(NewFixedPrefixTransform(FLAGS_prefix_size)),
        num_(FLAGS_num),
        value_size_(FLAGS_value_size),
        key_size_(FLAGS_key_size),
        prefix_size_(FLAGS_prefix_size),
        keys_per_prefix_(FLAGS_keys_per_prefix),
        entries_per_batch_(1),
        reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
        read_random_exp_range_(0.0),
        writes_(FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes),
        readwrites_(
            (FLAGS_writes < 0 && FLAGS_reads < 0)
                ? FLAGS_num
                : ((FLAGS_writes > FLAGS_reads) ? FLAGS_writes : FLAGS_reads)),
        merge_keys_(FLAGS_merge_keys < 0 ? FLAGS_num : FLAGS_merge_keys),
        report_file_operations_(FLAGS_report_file_operations),
#ifndef ROCKSDB_LITE
        use_blob_db_(FLAGS_use_blob_db) {
#else
        use_blob_db_(false) {
#endif  // !ROCKSDB_LITE
    // use simcache instead of cache
    if (FLAGS_simcache_size >= 0) {
      if (FLAGS_cache_numshardbits >= 1) {
        cache_ =
            NewSimCache(cache_, FLAGS_simcache_size, FLAGS_cache_numshardbits);
      } else {
        cache_ = NewSimCache(cache_, FLAGS_simcache_size, 0);
      }
    }

    if (report_file_operations_) {
      if (!FLAGS_hdfs.empty()) {
        fprintf(stderr,
                "--hdfs and --report_file_operations cannot be enabled "
                "at the same time");
        exit(1);
      }
      FLAGS_env = new ReportFileOpEnv(rocksdb::Env::Default());
    }

    if (FLAGS_prefix_size > FLAGS_key_size) {
      fprintf(stderr, "prefix size is larger than key size");
      exit(1);
    }

    std::vector<std::string> files;
    FLAGS_env->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        FLAGS_env->DeleteFile(FLAGS_db + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      Options options;
      if (!FLAGS_wal_dir.empty()) {
        options.wal_dir = FLAGS_wal_dir;
      }
#ifndef ROCKSDB_LITE
      if (use_blob_db_) {
        blob_db::DestroyBlobDB(FLAGS_db, options, blob_db::BlobDBOptions());
      }
#endif  // !ROCKSDB_LITE
      DestroyDB(FLAGS_db, options);
      if (!FLAGS_wal_dir.empty()) {
        FLAGS_env->DeleteDir(FLAGS_wal_dir);
      }

      if (FLAGS_num_multi_db > 1) {
        FLAGS_env->CreateDir(FLAGS_db);
        if (!FLAGS_wal_dir.empty()) {
          FLAGS_env->CreateDir(FLAGS_wal_dir);
        }
      }
    }
  }

  ~Benchmark() {
    db_.DeleteDBs();
    delete prefix_extractor_;
    if (cache_.get() != nullptr) {
      // this will leak, but we're shutting down so nobody cares
      cache_->DisownData();
    }
  }

  Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), key_size_);
  }

  // Generate key according to the given specification and random number.
  // The resulting key will have the following format (if keys_per_prefix_
  // is positive), extra trailing bytes are either cut off or padded with '0'.
  // The prefix value is derived from key value.
  //   ----------------------------
  //   | prefix 00000 | key 00000 |
  //   ----------------------------
  // If keys_per_prefix_ is 0, the key is simply a binary representation of
  // random number followed by trailing '0's
  //   ----------------------------
  //   |        key 00000         |
  //   ----------------------------
  void GenerateKeyFromInt(uint64_t v, int64_t num_keys, Slice* key) {
    char* start = const_cast<char*>(key->data());
    char* pos = start;
    if (keys_per_prefix_ > 0) {
      int64_t num_prefix = num_keys / keys_per_prefix_;
      int64_t prefix = v % num_prefix;
      int bytes_to_fill = std::min(prefix_size_, 8);
      if (port::kLittleEndian) {
        for (int i = 0; i < bytes_to_fill; ++i) {
          pos[i] = (prefix >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
        }
      } else {
        memcpy(pos, static_cast<void*>(&prefix), bytes_to_fill);
      }
      if (prefix_size_ > 8) {
        // fill the rest with 0s
        memset(pos + 8, '0', prefix_size_ - 8);
      }
      pos += prefix_size_;
    }

    int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
    if (port::kLittleEndian) {
      for (int i = 0; i < bytes_to_fill; ++i) {
        pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
      }
    } else {
      memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    }
    pos += bytes_to_fill;
    if (key_size_ > pos - start) {
      memset(pos, '0', key_size_ - (pos - start));
    }
  }

  std::string GetPathForMultiple(std::string base_name, size_t id) {
    if (!base_name.empty()) {
#ifndef OS_WIN
      if (base_name.back() != '/') {
        base_name += '/';
      }
#else
      if (base_name.back() != '\\') {
        base_name += '\\';
      }
#endif
    }
    return base_name + ToString(id);
  }

void VerifyDBFromDB(std::string& truth_db_name) {
  DBWithColumnFamilies truth_db;
  auto s = DB::OpenForReadOnly(open_options_, truth_db_name, &truth_db.db);
  if (!s.ok()) {
    fprintf(stderr, "open error: %s\n", s.ToString().c_str());
    exit(1);
  }
  ReadOptions ro;
  ro.total_order_seek = true;
  std::unique_ptr<Iterator> truth_iter(truth_db.db->NewIterator(ro));
  std::unique_ptr<Iterator> db_iter(db_.db->NewIterator(ro));
  // Verify that all the key/values in truth_db are retrivable in db with ::Get
  fprintf(stderr, "Verifying db >= truth_db with ::Get...\n");
  for (truth_iter->SeekToFirst(); truth_iter->Valid(); truth_iter->Next()) {
      std::string value;
      s = db_.db->Get(ro, truth_iter->key(), &value);
      assert(s.ok());
      // TODO(myabandeh): provide debugging hints
      assert(Slice(value) == truth_iter->value());
  }
  // Verify that the db iterator does not give any extra key/value
  fprintf(stderr, "Verifying db == truth_db...\n");
  for (db_iter->SeekToFirst(), truth_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next(), truth_iter->Next()) {
    assert(truth_iter->Valid());
    assert(truth_iter->value() == db_iter->value());
  }
  // No more key should be left unchecked in truth_db
  assert(!truth_iter->Valid());
  fprintf(stderr, "...Verified\n");
}

  void Run() {
    if (!SanityCheck()) {
      exit(1);
    }
    Open(&open_options_);

    PrintHeader();
    std::stringstream benchmark_stream(FLAGS_benchmarks);
    std::string name;
    std::unique_ptr<ExpiredTimeFilter> filter;
    while (std::getline(benchmark_stream, name, ',')) {
      // Sanitize parameters
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      writes_ = (FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes);
      deletes_ = (FLAGS_deletes < 0 ? FLAGS_num : FLAGS_deletes);
      value_size_ = FLAGS_value_size;
      key_size_ = FLAGS_key_size;
      entries_per_batch_ = FLAGS_batch_size;
      writes_per_range_tombstone_ = FLAGS_writes_per_range_tombstone;
      range_tombstone_width_ = FLAGS_range_tombstone_width;
      max_num_range_tombstones_ = FLAGS_max_num_range_tombstones;
      write_options_ = WriteOptions();
      
      col_fam_options_ = ColumnFamilyOptions();
      col_fam_options_.inplace_update_support = true;

      read_random_exp_range_ = FLAGS_read_random_exp_range;
      if (FLAGS_sync) {
        write_options_.sync = true;
      }
      write_options_.disableWAL = FLAGS_disable_wal;

      void (Benchmark::*method)(ThreadState*) = nullptr;
      void (Benchmark::*post_process_method)() = nullptr;

      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      int num_repeat = 1;
      int num_warmup = 0;
      if (!name.empty() && *name.rbegin() == ']') {
        auto it = name.find('[');
        if (it == std::string::npos) {
          fprintf(stderr, "unknown benchmark arguments '%s'\n", name.c_str());
          exit(1);
        }
        std::string args = name.substr(it + 1);
        args.resize(args.size() - 1);
        name.resize(it);

        std::string bench_arg;
        std::stringstream args_stream(args);
        while (std::getline(args_stream, bench_arg, '-')) {
          if (bench_arg.empty()) {
            continue;
          }
          if (bench_arg[0] == 'X') {
            // Repeat the benchmark n times
            std::string num_str = bench_arg.substr(1);
            num_repeat = std::stoi(num_str);
          } else if (bench_arg[0] == 'W') {
            // Warm up the benchmark for n times
            std::string num_str = bench_arg.substr(1);
            num_warmup = std::stoi(num_str);
          }
        }
      }

      // Both fillseqdeterministic and filluniquerandomdeterministic
      // fill the levels except the max level with UNIQUE_RANDOM
      // and fill the max level with fillseq and filluniquerandom, respectively
      if (name == "fillseqdeterministic" ||
          name == "filluniquerandomdeterministic") {
        if (!FLAGS_disable_auto_compactions) {
          fprintf(stderr,
                  "Please disable_auto_compactions in FillDeterministic "
                  "benchmark\n");
          exit(1);
        }
        if (num_threads > 1) {
          fprintf(stderr,
                  "filldeterministic multithreaded not supported"
                  ", use 1 thread\n");
          num_threads = 1;
        }
        fresh_db = true;
        if (name == "fillseqdeterministic") {
          method = &Benchmark::WriteSeqDeterministic;
        } else {
          method = &Benchmark::WriteUniqueRandomDeterministic;
        }
      } else if (name == "fillseq") {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == "fillbatch") {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == "fillrandom") {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == "filluniquerandom") {
        fresh_db = true;
        if (num_threads > 1) {
          fprintf(stderr,
                  "filluniquerandom multithreaded not supported"
                  ", use 1 thread");
          num_threads = 1;
        }
        method = &Benchmark::WriteUniqueRandom;
      } else if (name == "overwrite") {
        method = &Benchmark::WriteRandom;
      } else if (name == "fillsync") {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == "fill100K") {
        fresh_db = true;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == "readseq") {
        method = &Benchmark::ReadSequential;
      } else if (name == "readtocache") {
        method = &Benchmark::ReadSequential;
        num_threads = 1;
        reads_ = num_;
      } else if (name == "readreverse") {
        method = &Benchmark::ReadReverse;
      } else if (name == "readrandom") {
        method = &Benchmark::ReadRandom;
      } else if (name == "readrandomfast") {
        method = &Benchmark::ReadRandomFast;
      } else if (name == "multireadrandom") {
        fprintf(stderr, "entries_per_batch = %" PRIi64 "\n",
                entries_per_batch_);
        method = &Benchmark::MultiReadRandom;
      }
      else if (name == "readmissing") {
        ++key_size_;
        method = &Benchmark::ReadRandom;
      } else if (name == "newiterator") {
        method = &Benchmark::IteratorCreation;
      } else if (name == "newiteratorwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::IteratorCreationWhileWriting;
      } else if (name == "seekrandom") {
        method = &Benchmark::SeekRandom;
      } else if (name == "seekrandomwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::SeekRandomWhileWriting;
      } else if (name == "seekrandomwhilemerging") {
        num_threads++;  // Add extra thread for merging
        method = &Benchmark::SeekRandomWhileMerging;
      } else if (name == "readrandomsmall") {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == "deleteseq") {
        method = &Benchmark::DeleteSeq;
      } else if (name == "deleterandom") {
        method = &Benchmark::DeleteRandom;
      } else if (name == "readwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == "readwhilemerging") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileMerging;
      } else if (name == "readrandomwriterandom") {
        method = &Benchmark::ReadRandomWriteRandom;
      } else if (name == "readrandomwriterandomsplitrange") {
        method = &Benchmark::ReadRandomWriteRandomSplitRange;
      }else if (name == "readrandomwriterandomskewed") {
        method = &Benchmark::ReadRandomWriteRandomSkewed;
      } else if (name == "readrandomwriterandomsplitrangeskewed") {
        method = &Benchmark::ReadRandomWriteRandomSplitRangeSkewed;
      }else if (name == "differentvaluesizesperthread") {
        method = &Benchmark::ReadRandomWriteRandomDifferentValueSizesPerThread;
      }else if (name == "differentvaluesizesperthreadcomp") {
        method = &Benchmark::ReadRandomWriteRandomDifferentValueSizesPerThreadComparison;
      }else if (name == "longpeak") {
        method = &Benchmark::LongPeakTest;
      }else if (name == "testzipf") {
        method = &Benchmark::Zipf;
      }else if (name == "testlatestgenerator") {
        method = &Benchmark::LatestGenerator;
      }else if (name == "ycsb") {
        fresh_db = true;
        method = &Benchmark::YCSBIntegrate;
      } else if (name == "ycsb_load") {
        fresh_db = true;
        method = &Benchmark::YCSBLoader;
      } else if (name == "ycsb_run") {
          fresh_db = false;
          method = &Benchmark::YCSBRunner;
      }else if (name == "bgYCSBrun") {
        num_threads++;
        fresh_db = true;
        method = &Benchmark::BGYCSBRun;
      }else if (name == "ycsbwklda") {
        fresh_db=false;
        method = &Benchmark::YCSBWorkloadA;
      }else if (name == "ycsbwkldb") {
          fresh_db=false;
        method = &Benchmark::YCSBWorkloadB;
      }else if (name == "ycsbwkldc") {
          fresh_db=false;
        method = &Benchmark::YCSBWorkloadC;
      }else if (name == "ycsbwkldd") {
          fresh_db=false;
        method = &Benchmark::YCSBWorkloadD;
      }else if (name == "ycsbwklde") {
          fresh_db=false;
        method = &Benchmark::YCSBWorkloadE;
      }else if (name == "ycsbwkldf") {
          fresh_db=false;
        method = &Benchmark::YCSBWorkloadF;
      }else if (name == "readrandomwriterandomdifferentvaluesizes") {
          method = &Benchmark::ReadRandomWriteRandomDifferentValueSizes;
      } else if (name == "readrandomwriterandomsplitrangedifferentvaluesizes") {
          method = &Benchmark::ReadRandomWriteRandomSplitRangeDifferentValueSizes;
      }else if (name == "readrandommergerandom") {
          if (FLAGS_merge_operator.empty()) {
              fprintf(stdout, "%-12s : skipped (--merge_operator is unknown)\n",
                      name.c_str());
              exit(1);
          }
          method = &Benchmark::ReadRandomMergeRandom;
      } else if (name == "updaterandom") {
          method = &Benchmark::UpdateRandom;
      } else if (name == "appendrandom") {
          method = &Benchmark::AppendRandom;
      } else if (name == "mergerandom") {
          if (FLAGS_merge_operator.empty()) {
              fprintf(stdout, "%-12s : skipped (--merge_operator is unknown)\n",
                      name.c_str());
              exit(1);
          }
          method = &Benchmark::MergeRandom;
      } else if (name == "randomwithverify") {
          method = &Benchmark::RandomWithVerify;
      } else if (name == "readwriteskewedworkload") {
          method = &Benchmark::ReadWriteSkewedWorkload;
      } else if (name == "fillseekseq") {
          method = &Benchmark::WriteSeqSeekSeq;
      } else if (name == "compact") {
          method = &Benchmark::Compact;
      } else if (name == "compactall") {
          CompactAll();
      } else if (name == "crc32c") {
          method = &Benchmark::Crc32c;
      } else if (name == "xxhash") {
          method = &Benchmark::xxHash;
      } else if (name == "acquireload") {
          method = &Benchmark::AcquireLoad;
      } else if (name == "compress") {
          method = &Benchmark::Compress;
      } else if (name == "uncompress") {
          method = &Benchmark::Uncompress;
#ifndef ROCKSDB_LITE
      } else if (name == "randomtransaction") {
          method = &Benchmark::RandomTransaction;
          post_process_method = &Benchmark::RandomTransactionVerify;
#endif  // ROCKSDB_LITE
      } else if (name == "randomreplacekeys") {
          fresh_db = true;
          method = &Benchmark::RandomReplaceKeys;
      } else if (name == "timeseries") {
          timestamp_emulator_.reset(new TimestampEmulator());
          if (FLAGS_expire_style == "compaction_filter") {
              filter.reset(new ExpiredTimeFilter(timestamp_emulator_));
              fprintf(stdout, "Compaction filter is used to remove expired data");
              open_options_.compaction_filter = filter.get();
          }
          fresh_db = true;
          method = &Benchmark::TimeSeries;
      } else if (name == "stats") {
          PrintStats("rocksdb.stats");
      } else if (name == "resetstats") {
          ResetStats();
      } else if (name == "verify") {
          VerifyDBFromDB(FLAGS_truth_db);
      } else if (name == "levelstats") {
          PrintStats("rocksdb.levelstats");
      } else if (name == "sstables") {
          PrintStats("rocksdb.sstables");
      } else if (!name.empty()) {  // No error message for empty name
          fprintf(stderr, "unknown benchmark '%s'\n", name.c_str());
          exit(1);
      }

      if (fresh_db) {
          if (FLAGS_use_existing_db) {
              fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                      name.c_str());
              method = nullptr;
          } else {
              if (db_.db != nullptr) {
                  db_.DeleteDBs();
                  DestroyDB(FLAGS_db, open_options_);
              }
              Options options = open_options_;
              for (size_t i = 0; i < multi_dbs_.size(); i++) {
                  delete multi_dbs_[i].db;
                  if (!open_options_.wal_dir.empty()) {
                      options.wal_dir = GetPathForMultiple(open_options_.wal_dir, i);
                  }
                  DestroyDB(GetPathForMultiple(FLAGS_db, i), options);
              }
              multi_dbs_.clear();
          }
          Open(&open_options_);  // use open_options for the last accessed
      }

      if (method != nullptr) {
          fprintf(stdout, "DB path: [%s]\n", FLAGS_db.c_str());
          if (num_warmup > 0) {
              printf("Warming up benchmark by running %d times\n", num_warmup);
          }

          for (int i = 0; i < num_warmup; i++) {
              RunBenchmark(num_threads, name, method);
          }

          if (num_repeat > 1) {
              printf("Running benchmark for %d times\n", num_repeat);
          }

          CombinedStats combined_stats;
          for (int i = 0; i < num_repeat; i++) {
              Stats stats = RunBenchmark(num_threads, name, method);
              combined_stats.AddStats(stats);
          }
          if (num_repeat > 1) {
              combined_stats.Report(name);
          }
      }
      if (post_process_method != nullptr) {
          (this->*post_process_method)();
      }
    }
    if (FLAGS_statistics) {
        fprintf(stdout, "STATISTICS:\n%s\n", dbstats->ToString().c_str());
    }
    if (FLAGS_simcache_size >= 0) {
        fprintf(stdout, "SIMULATOR CACHE STATISTICS:\n%s\n",
                std::dynamic_pointer_cast<SimCache>(cache_)->ToString().c_str());
    }
  }

private:
std::shared_ptr<TimestampEmulator> timestamp_emulator_;

struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
};

static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
        MutexLock l(&shared->mu);
        shared->num_initialized++;
        if (shared->num_initialized >= shared->total) {
            shared->cv.SignalAll();
        }
        while (!shared->start) {
            shared->cv.Wait();
        }
    }

    SetPerfLevel(static_cast<PerfLevel> (shared->perf_level));
    thread->stats.Start(thread->tid);
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
        MutexLock l(&shared->mu);
        shared->num_done++;
        if (shared->num_done >= shared->total) {
            shared->cv.SignalAll();
        }
    }
}

Stats RunBenchmark(int n, Slice name,
        void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.send_low_workload = 0;
    shared.start = false;
    if (FLAGS_benchmark_write_rate_limit > 0) {
        printf(">>>> FLAGS_benchmark_write_rate_limit WRITE RATE LIMITER\n");  
        shared.write_rate_limiter.reset(
                NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }
    if (FLAGS_benchmark_read_rate_limit > 0) {
        printf(">>>> FLAGS_benchmark_read_rate_limit READ RATE LIMITER\n");    
        shared.read_rate_limiter.reset(NewGenericRateLimiter(
                    FLAGS_benchmark_read_rate_limit, 100000 /* refill_period_us */,
                    10 /* fairness */, RateLimiter::Mode::kReadsOnly));
    }
   
        std::unique_ptr<ReporterAgent> reporter_agent;
    if (FLAGS_report_interval_seconds > 0) {
      if ( FLAGS_DOTA_enable || FLAGS_TEA_enable ||
          FLAGS_FEA_enable) {
        // need to use another Report Agent
        if (FLAGS_DOTA_tuning_gap == 0) {
          reporter_agent.reset(new ReporterAgentWithTuning(
              reinterpret_cast<DBImpl*>(db_.db), FLAGS_env, FLAGS_report_file,
              FLAGS_report_interval_seconds, FLAGS_report_interval_seconds));
        } else {
          reporter_agent.reset(new ReporterAgentWithTuning(
              reinterpret_cast<DBImpl*>(db_.db), FLAGS_env, FLAGS_report_file,
              FLAGS_report_interval_seconds, FLAGS_DOTA_tuning_gap));
        }
        auto tuner_agent =
            reinterpret_cast<ReporterAgentWithTuning*>(reporter_agent.get());
        tuner_agent->UseFEATTuner(FLAGS_TEA_enable, FLAGS_FEA_enable);
        tuner_agent->GetTuner()->set_idle_ratio(FLAGS_idle_rate);
        tuner_agent->GetTuner()->set_gap_threshold(FLAGS_FEA_gap_threshold);
        tuner_agent->GetTuner()->set_slow_flush_threshold(FLAGS_TEA_slow_flush);
      } else if (FLAGS_detailed_running_stats) {
        reporter_agent.reset(new ReporterWithMoreDetails(
            reinterpret_cast<DBImpl*>(db_.db), FLAGS_env, FLAGS_report_file,
            FLAGS_report_interval_seconds));
      } else if (FLAGS_SILK_triggered) {
        reporter_agent.reset(new ReporterAgentWithSILK(
            reinterpret_cast<DBImpl*>(db_.db), FLAGS_env, FLAGS_report_file,
            FLAGS_report_interval_seconds, FLAGS_value_size,
            FLAGS_SILK_bandwidth_limitation));
      } else {
        reporter_agent.reset(new ReporterAgent(FLAGS_env, FLAGS_report_file,
                                               FLAGS_report_interval_seconds));
      }
    }


    ThreadArg* arg = new ThreadArg[n];

    for (int i = 0; i < n; i++) {
#ifdef NUMA
        if (FLAGS_enable_numa) {
            // Performs a local allocation of memory to threads in numa node.
            int n_nodes = numa_num_task_nodes();  // Number of nodes in NUMA.
            numa_exit_on_error = 1;
            int numa_node = i % n_nodes;
            bitmask* nodes = numa_allocate_nodemask();
            numa_bitmask_clearall(nodes);
            numa_bitmask_setbit(nodes, numa_node);
            // numa_bind() call binds the process to the node and these
            // properties are passed on to the thread that is created in
            // StartThread method called later in the loop.
            numa_bind(nodes);
            numa_set_strict(1);
            numa_free_nodemask(nodes);
        }
#endif
        arg[i].bm = this;
        arg[i].method = method;
        arg[i].shared = &shared;
        arg[i].thread = new ThreadState(i);
        arg[i].thread->stats.SetReporterAgent(reporter_agent.get());
        arg[i].thread->shared = &shared;
        FLAGS_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
        shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
        shared.cv.Wait();
    }
    shared.mu.Unlock();

    // Stats for some threads can be excluded.
    Stats merge_stats;
    for (int i = 0; i < n; i++) {
        merge_stats.Merge(arg[i].thread->stats);
    }
    merge_stats.Report(name);

    for (int i = 0; i < n; i++) {
        delete arg[i].thread;
    }
    delete[] arg;

    return merge_stats;
}

void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
        crc = crc32c::Value(data.data(), size);
        thread->stats.FinishedOps(nullptr, nullptr, 1, kCrc);
        bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
}

void xxHash(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    unsigned int xxh32 = 0;
    while (bytes < 500 * 1048576) {
        xxh32 = XXH32(data.data(), size, 0);
        thread->stats.FinishedOps(nullptr, nullptr, 1, kHash);
        bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... xxh32=0x%x\r", static_cast<unsigned int>(xxh32));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
}

void AcquireLoad(ThreadState* thread) {
    int dummy;
    std::atomic<void*> ap(&dummy);
    int count = 0;
    void *ptr = nullptr;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
        for (int i = 0; i < 1000; i++) {
            ptr = ap.load(std::memory_order_acquire);
        }
        count++;
        thread->stats.FinishedOps(nullptr, nullptr, 1, kOthers);
    }
    if (ptr == nullptr) exit(1);  // Disable unused variable warning.
}

void Compress(ThreadState *thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(FLAGS_block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;

    // Compress 1G
    while (ok && bytes < int64_t(1) << 30) {
        compressed.clear();
        ok = CompressSlice(input, &compressed);
        produced += compressed.size();
        bytes += input.size();
        thread->stats.FinishedOps(nullptr, nullptr, 1, kCompress);
    }

    if (!ok) {
        thread->stats.AddMessage("(compression failure)");
    } else {
        char buf[340];
        snprintf(buf, sizeof(buf), "(output: %.1f%%)",
                (produced * 100.0) / bytes);
        thread->stats.AddMessage(buf);
        thread->stats.AddBytes(bytes);
    }
}

void Uncompress(ThreadState *thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(FLAGS_block_size);
    std::string compressed;

    bool ok = CompressSlice(input, &compressed);
    int64_t bytes = 0;
    int decompress_size;
    while (ok && bytes < 1024 * 1048576) {
        char *uncompressed = nullptr;
        switch (FLAGS_compression_type_e) {
            case rocksdb::kSnappyCompression: {
                                                  // get size and allocate here to make comparison fair
                                                  size_t ulength = 0;
                                                  if (!Snappy_GetUncompressedLength(compressed.data(),
                                                              compressed.size(), &ulength)) {
                                                      ok = false;
                                                      break;
                                                  }
                                                  uncompressed = new char[ulength];
                                                  ok = Snappy_Uncompress(compressed.data(), compressed.size(),
                                                          uncompressed);
                                                  break;
                                              }
            case rocksdb::kZlibCompression:
                                              uncompressed = Zlib_Uncompress(compressed.data(), compressed.size(),
                                                      &decompress_size, 2);
                                              ok = uncompressed != nullptr;
                                              break;
            case rocksdb::kBZip2Compression:
                                              uncompressed = BZip2_Uncompress(compressed.data(), compressed.size(),
                                                      &decompress_size, 2);
                                              ok = uncompressed != nullptr;
                                              break;
            case rocksdb::kLZ4Compression:
                                              uncompressed = LZ4_Uncompress(compressed.data(), compressed.size(),
                                                      &decompress_size, 2);
                                              ok = uncompressed != nullptr;
                                              break;
            case rocksdb::kLZ4HCCompression:
                                              uncompressed = LZ4_Uncompress(compressed.data(), compressed.size(),
                                                      &decompress_size, 2);
                                              ok = uncompressed != nullptr;
                                              break;
            case rocksdb::kXpressCompression:
                                              uncompressed = XPRESS_Uncompress(compressed.data(), compressed.size(),
                                                      &decompress_size);
                                              ok = uncompressed != nullptr;
                                              break;
            case rocksdb::kZSTD:
                                              uncompressed = ZSTD_Uncompress(compressed.data(), compressed.size(),
                                                      &decompress_size);
                                              ok = uncompressed != nullptr;
                                              break;
            default:
                                              ok = false;
        }
        delete[] uncompressed;
        bytes += input.size();
        thread->stats.FinishedOps(nullptr, nullptr, 1, kUncompress);
    }

    if (!ok) {
        thread->stats.AddMessage("(compression failure)");
    } else {
        thread->stats.AddBytes(bytes);
    }
}

// Returns true if the options is initialized from the specified
// options file.
bool InitializeOptionsFromFile(Options* opts) {
#ifndef ROCKSDB_LITE
    printf("Initializing RocksDB Options from the specified file\n");
    DBOptions db_opts;
    std::vector<ColumnFamilyDescriptor> cf_descs;
    if (FLAGS_options_file != "") {
        auto s = LoadOptionsFromFile(FLAGS_options_file, Env::Default(), &db_opts,
                &cf_descs);
        if (s.ok()) {
            *opts = Options(db_opts, cf_descs[0].options);
            return true;
        }
        fprintf(stderr, "Unable to load options file %s --- %s\n",
                FLAGS_options_file.c_str(), s.ToString().c_str());
        exit(1);
    }
#endif
    return false;
}

void InitializeOptionsFromFlags(Options* opts) {
    printf("Initializing RocksDB Options from command-line flags\n");
    Options& options = *opts;

    assert(db_.db == nullptr);

    options.create_missing_column_families = FLAGS_num_column_families > 1;
    options.max_open_files = FLAGS_open_files;
    if (FLAGS_cost_write_buffer_to_cache || FLAGS_db_write_buffer_size != 0) {
        options.write_buffer_manager.reset(
                new WriteBufferManager(FLAGS_db_write_buffer_size, cache_));
    }
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_write_buffer_number = FLAGS_max_write_buffer_number;
    options.min_write_buffer_number_to_merge =
        FLAGS_min_write_buffer_number_to_merge;
    options.max_write_buffer_number_to_maintain =
        FLAGS_max_write_buffer_number_to_maintain;
    options.max_background_jobs = FLAGS_max_background_jobs;
    options.max_background_compactions = FLAGS_max_background_compactions;
    options.max_subcompactions = static_cast<uint32_t>(FLAGS_subcompactions);
    options.max_background_flushes = FLAGS_max_background_flushes;
    options.compaction_style = FLAGS_compaction_style_e;
    options.compaction_pri = FLAGS_compaction_pri_e;
    options.allow_mmap_reads = FLAGS_mmap_read;
    options.allow_mmap_writes = FLAGS_mmap_write;
    options.use_direct_reads = FLAGS_use_direct_reads;
    options.use_direct_io_for_flush_and_compaction =
        FLAGS_use_direct_io_for_flush_and_compaction;
#ifndef ROCKSDB_LITE
    options.compaction_options_fifo = CompactionOptionsFIFO(
            FLAGS_fifo_compaction_max_table_files_size_mb * 1024 * 1024,
            FLAGS_fifo_compaction_allow_compaction, FLAGS_fifo_compaction_ttl);
#endif  // ROCKSDB_LITE
    if (FLAGS_prefix_size != 0) {
        options.prefix_extractor.reset(
                NewFixedPrefixTransform(FLAGS_prefix_size));
    }
    if (FLAGS_use_uint64_comparator) {
        options.comparator = test::Uint64Comparator();
        if (FLAGS_key_size != 8) {
            fprintf(stderr, "Using Uint64 comparator but key size is not 8.\n");
            exit(1);
        }
    }
    if (FLAGS_use_stderr_info_logger) {
        options.info_log.reset(new StderrLogger());
    }
    options.memtable_huge_page_size = FLAGS_memtable_use_huge_page ? 2048 : 0;
    options.memtable_prefix_bloom_size_ratio = FLAGS_memtable_bloom_size_ratio;
    if (FLAGS_memtable_insert_with_hint_prefix_size > 0) {
        options.memtable_insert_with_hint_prefix_extractor.reset(
                NewCappedPrefixTransform(
                    FLAGS_memtable_insert_with_hint_prefix_size));
    }
    options.bloom_locality = FLAGS_bloom_locality;
    options.max_file_opening_threads = FLAGS_file_opening_threads;
    options.new_table_reader_for_compaction_inputs =
        FLAGS_new_table_reader_for_compaction_inputs;
    options.compaction_readahead_size = FLAGS_compaction_readahead_size;
    options.random_access_max_buffer_size = FLAGS_random_access_max_buffer_size;
    options.writable_file_max_buffer_size = FLAGS_writable_file_max_buffer_size;
    options.use_fsync = FLAGS_use_fsync;
    options.num_levels = FLAGS_num_levels;
    options.target_file_size_base = FLAGS_target_file_size_base;
    options.target_file_size_multiplier = FLAGS_target_file_size_multiplier;
    options.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;
    options.level_compaction_dynamic_level_bytes =
        FLAGS_level_compaction_dynamic_level_bytes;
    options.max_bytes_for_level_multiplier =
        FLAGS_max_bytes_for_level_multiplier;
    if ((FLAGS_prefix_size == 0) && (FLAGS_rep_factory == kPrefixHash ||
                FLAGS_rep_factory == kHashLinkedList)) {
        fprintf(stderr, "prefix_size should be non-zero if PrefixHash or "
                "HashLinkedList memtablerep is used\n");
        exit(1);
    }
    switch (FLAGS_rep_factory) {
        case kSkipList:
            options.memtable_factory.reset(new SkipListFactory(
                        FLAGS_skip_list_lookahead));
            break;
#ifndef ROCKSDB_LITE
        case kPrefixHash:
            options.memtable_factory.reset(
                    NewHashSkipListRepFactory(FLAGS_hash_bucket_count));
            break;
        case kHashLinkedList:
            options.memtable_factory.reset(NewHashLinkListRepFactory(
                        FLAGS_hash_bucket_count));
            break;
        case kVectorRep:
            options.memtable_factory.reset(
                    new VectorRepFactory
                    );
            break;
        case kCuckoo:
            options.memtable_factory.reset(NewHashCuckooRepFactory(
                        options.write_buffer_size, FLAGS_key_size + FLAGS_value_size));
            break;
#else
        default:
            fprintf(stderr, "Only skip list is supported in lite mode\n");
            exit(1);
#endif  // ROCKSDB_LITE
    }
    if (FLAGS_use_plain_table) {
#ifndef ROCKSDB_LITE
        if (FLAGS_rep_factory != kPrefixHash &&
                FLAGS_rep_factory != kHashLinkedList) {
            fprintf(stderr, "Waring: plain table is used with skipList\n");
        }

        int bloom_bits_per_key = FLAGS_bloom_bits;
        if (bloom_bits_per_key < 0) {
            bloom_bits_per_key = 0;
        }

        PlainTableOptions plain_table_options;
        plain_table_options.user_key_len = FLAGS_key_size;
        plain_table_options.bloom_bits_per_key = bloom_bits_per_key;
        plain_table_options.hash_table_ratio = 0.75;
        options.table_factory = std::shared_ptr<TableFactory>(
                NewPlainTableFactory(plain_table_options));
#else
        fprintf(stderr, "Plain table is not supported in lite mode\n");
        exit(1);
#endif  // ROCKSDB_LITE
    } else if (FLAGS_use_cuckoo_table) {
#ifndef ROCKSDB_LITE
        if (FLAGS_cuckoo_hash_ratio > 1 || FLAGS_cuckoo_hash_ratio < 0) {
            fprintf(stderr, "Invalid cuckoo_hash_ratio\n");
            exit(1);
        }
        rocksdb::CuckooTableOptions table_options;
        table_options.hash_table_ratio = FLAGS_cuckoo_hash_ratio;
        table_options.identity_as_first_hash = FLAGS_identity_as_first_hash;
        options.table_factory = std::shared_ptr<TableFactory>(
                NewCuckooTableFactory(table_options));
#else
        fprintf(stderr, "Cuckoo table is not supported in lite mode\n");
        exit(1);
#endif  // ROCKSDB_LITE
    } else {
        BlockBasedTableOptions block_based_options;
        if (FLAGS_use_hash_search) {
            if (FLAGS_prefix_size == 0) {
                fprintf(stderr,
                        "prefix_size not assigned when enable use_hash_search \n");
                exit(1);
            }
            block_based_options.index_type = BlockBasedTableOptions::kHashSearch;
        } else {
            block_based_options.index_type = BlockBasedTableOptions::kBinarySearch;
        }
        if (FLAGS_partition_index_and_filters) {
            if (FLAGS_use_hash_search) {
                fprintf(stderr,
                        "use_hash_search is incompatible with "
                        "partition_index_and_filters and is ignored");
            }
            block_based_options.index_type =
                BlockBasedTableOptions::kTwoLevelIndexSearch;
            block_based_options.partition_filters = true;
            block_based_options.metadata_block_size = FLAGS_metadata_block_size;
        }
        if (cache_ == nullptr) {
            block_based_options.no_block_cache = true;
        }
        block_based_options.cache_index_and_filter_blocks =
            FLAGS_cache_index_and_filter_blocks;
        block_based_options.pin_l0_filter_and_index_blocks_in_cache =
            FLAGS_pin_l0_filter_and_index_blocks_in_cache;
        if (FLAGS_cache_high_pri_pool_ratio > 1e-6) {  // > 0.0 + eps
            block_based_options.cache_index_and_filter_blocks_with_high_priority =
                true;
        }
        block_based_options.block_cache = cache_;
        block_based_options.block_cache_compressed = compressed_cache_;
        block_based_options.block_size = FLAGS_block_size;
        block_based_options.block_restart_interval = FLAGS_block_restart_interval;
        block_based_options.index_block_restart_interval =
            FLAGS_index_block_restart_interval;
        block_based_options.filter_policy = filter_policy_;
        block_based_options.format_version = 2;
        block_based_options.read_amp_bytes_per_bit = FLAGS_read_amp_bytes_per_bit;
        if (FLAGS_read_cache_path != "") {
#ifndef ROCKSDB_LITE
            Status rc_status;

            // Read cache need to be provided with a the Logger, we will put all
            // reac cache logs in the read cache path in a file named rc_LOG
            rc_status = FLAGS_env->CreateDirIfMissing(FLAGS_read_cache_path);
            std::shared_ptr<Logger> read_cache_logger;
            if (rc_status.ok()) {
                rc_status = FLAGS_env->NewLogger(FLAGS_read_cache_path + "/rc_LOG",
                        &read_cache_logger);
            }

            if (rc_status.ok()) {
                PersistentCacheConfig rc_cfg(FLAGS_env, FLAGS_read_cache_path,
                        FLAGS_read_cache_size,
                        read_cache_logger);

                rc_cfg.enable_direct_reads = FLAGS_read_cache_direct_read;
                rc_cfg.enable_direct_writes = FLAGS_read_cache_direct_write;
                rc_cfg.writer_qdepth = 4;
                rc_cfg.writer_dispatch_size = 4 * 1024;

                auto pcache = std::make_shared<BlockCacheTier>(rc_cfg);
                block_based_options.persistent_cache = pcache;
                rc_status = pcache->Open();
            }

            if (!rc_status.ok()) {
                fprintf(stderr, "Error initializing read cache, %s\n",
                        rc_status.ToString().c_str());
                exit(1);
            }
#else
            fprintf(stderr, "Read cache is not supported in LITE\n");
            exit(1);

#endif
        }
        options.table_factory.reset(
                NewBlockBasedTableFactory(block_based_options));
    }
    if (FLAGS_max_bytes_for_level_multiplier_additional_v.size() > 0) {
        if (FLAGS_max_bytes_for_level_multiplier_additional_v.size() !=
                (unsigned int)FLAGS_num_levels) {
            fprintf(stderr, "Insufficient number of fanouts specified %d\n",
                    (int)FLAGS_max_bytes_for_level_multiplier_additional_v.size());
            exit(1);
        }
        options.max_bytes_for_level_multiplier_additional =
            FLAGS_max_bytes_for_level_multiplier_additional_v;
    }
    options.level0_stop_writes_trigger = FLAGS_level0_stop_writes_trigger;
    options.level0_file_num_compaction_trigger =
        FLAGS_level0_file_num_compaction_trigger;
    options.level0_slowdown_writes_trigger =
        FLAGS_level0_slowdown_writes_trigger;
    options.compression = FLAGS_compression_type_e;
    options.compression_opts.level = FLAGS_compression_level;
    options.compression_opts.max_dict_bytes = FLAGS_compression_max_dict_bytes;
    options.WAL_ttl_seconds = FLAGS_wal_ttl_seconds;
    options.WAL_size_limit_MB = FLAGS_wal_size_limit_MB;
    options.max_total_wal_size = FLAGS_max_total_wal_size;

    if (FLAGS_min_level_to_compress >= 0) {
        assert(FLAGS_min_level_to_compress <= FLAGS_num_levels);
        options.compression_per_level.resize(FLAGS_num_levels);
        for (int i = 0; i < FLAGS_min_level_to_compress; i++) {
            options.compression_per_level[i] = kNoCompression;
        }
        for (int i = FLAGS_min_level_to_compress;
                i < FLAGS_num_levels; i++) {
            options.compression_per_level[i] = FLAGS_compression_type_e;
        }
    }
    options.soft_rate_limit = FLAGS_soft_rate_limit;
    options.hard_rate_limit = FLAGS_hard_rate_limit;
    options.soft_pending_compaction_bytes_limit =
        FLAGS_soft_pending_compaction_bytes_limit;
    options.hard_pending_compaction_bytes_limit =
        FLAGS_hard_pending_compaction_bytes_limit;
    options.delayed_write_rate = FLAGS_delayed_write_rate;
    options.allow_concurrent_memtable_write =
        FLAGS_allow_concurrent_memtable_write;
    options.enable_write_thread_adaptive_yield =
        FLAGS_enable_write_thread_adaptive_yield;
    options.enable_pipelined_write = FLAGS_enable_pipelined_write;
    options.write_thread_max_yield_usec = FLAGS_write_thread_max_yield_usec;
    options.write_thread_slow_yield_usec = FLAGS_write_thread_slow_yield_usec;
    options.rate_limit_delay_max_milliseconds =
        FLAGS_rate_limit_delay_max_milliseconds;
    options.table_cache_numshardbits = FLAGS_table_cache_numshardbits;
    options.max_compaction_bytes = FLAGS_max_compaction_bytes;
    options.disable_auto_compactions = FLAGS_disable_auto_compactions;
    options.optimize_filters_for_hits = FLAGS_optimize_filters_for_hits;

    // fill storage options
    options.advise_random_on_open = FLAGS_advise_random_on_open;
    options.access_hint_on_compaction_start = FLAGS_compaction_fadvice_e;
    options.use_adaptive_mutex = FLAGS_use_adaptive_mutex;
    options.bytes_per_sync = FLAGS_bytes_per_sync;
    options.wal_bytes_per_sync = FLAGS_wal_bytes_per_sync;

    // merge operator options
    options.merge_operator = MergeOperators::CreateFromStringId(
            FLAGS_merge_operator);
    if (options.merge_operator == nullptr && !FLAGS_merge_operator.empty()) {
        fprintf(stderr, "invalid merge operator: %s\n",
                FLAGS_merge_operator.c_str());
        exit(1);
    }
    options.max_successive_merges = FLAGS_max_successive_merges;
    options.report_bg_io_stats = FLAGS_report_bg_io_stats;

    // set universal style compaction configurations, if applicable
    if (FLAGS_universal_size_ratio != 0) {
        options.compaction_options_universal.size_ratio =
            FLAGS_universal_size_ratio;
    }
    if (FLAGS_universal_min_merge_width != 0) {
        options.compaction_options_universal.min_merge_width =
            FLAGS_universal_min_merge_width;
    }
    if (FLAGS_universal_max_merge_width != 0) {
        options.compaction_options_universal.max_merge_width =
            FLAGS_universal_max_merge_width;
    }
    if (FLAGS_universal_max_size_amplification_percent != 0) {
        options.compaction_options_universal.max_size_amplification_percent =
            FLAGS_universal_max_size_amplification_percent;
    }
    if (FLAGS_universal_compression_size_percent != -1) {
        options.compaction_options_universal.compression_size_percent =
            FLAGS_universal_compression_size_percent;
    }
    options.compaction_options_universal.allow_trivial_move =
        FLAGS_universal_allow_trivial_move;
    if (FLAGS_thread_status_per_interval > 0) {
        options.enable_thread_tracking = true;
    }
    if (FLAGS_rate_limiter_bytes_per_sec > 0) {
        if (FLAGS_rate_limit_bg_reads &&
                !FLAGS_new_table_reader_for_compaction_inputs) {
            fprintf(stderr,
                    "rate limit compaction reads must have "
                    "new_table_reader_for_compaction_inputs set\n");
            exit(1);
        }
        printf(">>>> FLAGS_rate_limiter_bytes_per_sec RATE LIMITER\n");

        if (FLAGS_autotuned_rate_limiter == true){
            options.rate_limiter.reset(NewGenericRateLimiter(
                        FLAGS_rate_limiter_bytes_per_sec, 100 * 1000 /* refill_period_us */,
                        10 /* fairness */,
                        FLAGS_rate_limit_bg_reads ? RateLimiter::Mode::kReadsOnly
                        : RateLimiter::Mode::kWritesOnly));
        } else{ 
            options.rate_limiter.reset(NewGenericRateLimiter(
                        FLAGS_rate_limiter_bytes_per_sec, 100 * 1000 /* refill_period_us */,
                        10 /* fairness */,
                        FLAGS_rate_limit_bg_reads ? RateLimiter::Mode::kReadsOnly
                        : RateLimiter::Mode::kWritesOnly));
        }
    }

#ifndef ROCKSDB_LITE
    if (FLAGS_readonly && FLAGS_transaction_db) {
        fprintf(stderr, "Cannot use readonly flag with transaction_db\n");
        exit(1);
    }
#endif  // ROCKSDB_LITE

}

void InitializeOptionsGeneral(Options* opts) {
    Options& options = *opts;

    options.statistics = dbstats;
    options.wal_dir = FLAGS_wal_dir;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.dump_malloc_stats = FLAGS_dump_malloc_stats;

    if (FLAGS_row_cache_size) {
        if (FLAGS_cache_numshardbits >= 1) {
            options.row_cache =
                NewLRUCache(FLAGS_row_cache_size, FLAGS_cache_numshardbits);
        } else {
            options.row_cache = NewLRUCache(FLAGS_row_cache_size);
        }
    }
    if (FLAGS_enable_io_prio) {
        FLAGS_env->LowerThreadPoolIOPriority(Env::LOW);
        FLAGS_env->LowerThreadPoolIOPriority(Env::HIGH);
    }
    options.env = FLAGS_env;

    if (FLAGS_num_multi_db <= 1) {
        OpenDb(options, FLAGS_db, &db_);
    } else {
        multi_dbs_.clear();
        multi_dbs_.resize(FLAGS_num_multi_db);
        auto wal_dir = options.wal_dir;
        for (int i = 0; i < FLAGS_num_multi_db; i++) {
            if (!wal_dir.empty()) {
                options.wal_dir = GetPathForMultiple(wal_dir, i);
            }
            OpenDb(options, GetPathForMultiple(FLAGS_db, i), &multi_dbs_[i]);
        }
        options.wal_dir = wal_dir;
    }
}

void Open(Options* opts) {
    if (!InitializeOptionsFromFile(opts)) {
        InitializeOptionsFromFlags(opts);
    }

    InitializeOptionsGeneral(opts);
}

void OpenDb(Options options, const std::string& db_name,
        DBWithColumnFamilies* db) {
    Status s;
    // Open with column families if necessary.
    if (FLAGS_num_column_families > 1) {
        size_t num_hot = FLAGS_num_column_families;
        if (FLAGS_num_hot_column_families > 0 &&
                FLAGS_num_hot_column_families < FLAGS_num_column_families) {
            num_hot = FLAGS_num_hot_column_families;
        } else {
            FLAGS_num_hot_column_families = FLAGS_num_column_families;
        }
        std::vector<ColumnFamilyDescriptor> column_families;
        for (size_t i = 0; i < num_hot; i++) {
            column_families.push_back(ColumnFamilyDescriptor(
                        ColumnFamilyName(i), ColumnFamilyOptions(options)));
        }
#ifndef ROCKSDB_LITE
        if (FLAGS_readonly) {
            s = DB::OpenForReadOnly(options, db_name, column_families,
                    &db->cfh, &db->db);
        } else if (FLAGS_optimistic_transaction_db) {
            s = OptimisticTransactionDB::Open(options, db_name, column_families,
                    &db->cfh, &db->opt_txn_db);
            if (s.ok()) {
                db->db = db->opt_txn_db->GetBaseDB();
            }
        } else if (FLAGS_transaction_db) {
            TransactionDB* ptr;
            TransactionDBOptions txn_db_options;
            s = TransactionDB::Open(options, txn_db_options, db_name,
                    column_families, &db->cfh, &ptr);
            if (s.ok()) {
                db->db = ptr;
            }
        } else {
            s = DB::Open(options, db_name, column_families, &db->cfh, &db->db);
        }
#else
        s = DB::Open(options, db_name, column_families, &db->cfh, &db->db);
#endif  // ROCKSDB_LITE
        db->cfh.resize(FLAGS_num_column_families);
        db->num_created = num_hot;
        db->num_hot = num_hot;
#ifndef ROCKSDB_LITE
    } else if (FLAGS_readonly) {
        s = DB::OpenForReadOnly(options, db_name, &db->db);
    } else if (FLAGS_optimistic_transaction_db) {
        s = OptimisticTransactionDB::Open(options, db_name, &db->opt_txn_db);
        if (s.ok()) {
            db->db = db->opt_txn_db->GetBaseDB();
        }
    } else if (FLAGS_transaction_db) {
        TransactionDB* ptr;
        TransactionDBOptions txn_db_options;
        s = CreateLoggerFromOptions(db_name, options, &options.info_log);
        if (s.ok()) {
            s = TransactionDB::Open(options, txn_db_options, db_name, &ptr);
        }
        if (s.ok()) {
            db->db = ptr;
        }
    } else if (FLAGS_use_blob_db) {
        blob_db::BlobDBOptions blob_db_options;
        blob_db::BlobDB* ptr;
        s = CreateLoggerFromOptions(db_name, options, &options.info_log);
        if (s.ok()) {
            s = blob_db::BlobDB::Open(options, blob_db_options, db_name, &ptr);
        }
        if (s.ok()) {
            db->db = ptr;
        }
#endif  // ROCKSDB_LITE
    } else {
        s = DB::Open(options, db_name, &db->db);
    }
    if (!s.ok()) {
        fprintf(stderr, "open error: %s\n", s.ToString().c_str());
        exit(1);
    }
}

enum WriteMode {
    RANDOM, SEQUENTIAL, UNIQUE_RANDOM
};

void WriteSeqDeterministic(ThreadState* thread) {
    DoDeterministicCompact(thread, open_options_.compaction_style, SEQUENTIAL);
}

void WriteUniqueRandomDeterministic(ThreadState* thread) {
    DoDeterministicCompact(thread, open_options_.compaction_style,
            UNIQUE_RANDOM);
}

void WriteSeq(ThreadState* thread) {
    DoWrite(thread, SEQUENTIAL);
}

void WriteRandom(ThreadState* thread) {
    DoWrite(thread, RANDOM);
}

void WriteUniqueRandom(ThreadState* thread) {
    DoWrite(thread, UNIQUE_RANDOM);
}

class KeyGenerator {
    public:
        KeyGenerator(Random64* rand, WriteMode mode,
                uint64_t num, uint64_t num_per_set = 64 * 1024)
            : rand_(rand),
            mode_(mode),
            num_(num),
            next_(0) {
                if (mode_ == UNIQUE_RANDOM) {
                    // NOTE: if memory consumption of this approach becomes a concern,
                    // we can either break it into pieces and only random shuffle a section
                    // each time. Alternatively, use a bit map implementation
                    // (https://reviews.facebook.net/differential/diff/54627/)
                    values_.resize(num_);
                    for (uint64_t i = 0; i < num_; ++i) {
                        values_[i] = i;
                    }
                    std::shuffle(
                            values_.begin(), values_.end(),
                            std::default_random_engine(static_cast<unsigned int>(FLAGS_seed)));
                }
            }

        uint64_t Next() {
            switch (mode_) {
                case SEQUENTIAL:
                    return next_++;
                case RANDOM:
                    return rand_->Next() % num_;
                case UNIQUE_RANDOM:
                    assert(next_ + 1 < num_);
                    return values_[next_++];
            }
            assert(false);
            return std::numeric_limits<uint64_t>::max();
        }

    private:
        Random64* rand_;
        WriteMode mode_;
        const uint64_t num_;
        uint64_t next_;
        std::vector<uint64_t> values_;
};

DB* SelectDB(ThreadState* thread) {
    return SelectDBWithCfh(thread)->db;
}

DBWithColumnFamilies* SelectDBWithCfh(ThreadState* thread) {
    return SelectDBWithCfh(thread->rand.Next());
}

DBWithColumnFamilies* SelectDBWithCfh(uint64_t rand_int) {
    if (db_.db != nullptr) {
        return &db_;
    } else  {
        return &multi_dbs_[rand_int % multi_dbs_.size()];
    }
}

void DoWrite(ThreadState* thread, WriteMode write_mode) {
    const int test_duration = write_mode == RANDOM ? FLAGS_duration : 0;
    const int64_t num_ops = writes_ == 0 ? num_ : writes_;

    size_t num_key_gens = 1;
    if (db_.db == nullptr) {
        num_key_gens = multi_dbs_.size();
    }
    std::vector<std::unique_ptr<KeyGenerator>> key_gens(num_key_gens);
    int64_t max_ops = num_ops * num_key_gens;
    int64_t ops_per_stage = max_ops;
    if (FLAGS_num_column_families > 1 && FLAGS_num_hot_column_families > 0) {
        ops_per_stage = (max_ops - 1) / (FLAGS_num_column_families /
                FLAGS_num_hot_column_families) +
            1;
    }

    Duration duration(test_duration, max_ops, ops_per_stage);
    for (size_t i = 0; i < num_key_gens; i++) {
        key_gens[i].reset(new KeyGenerator(&(thread->rand), write_mode, num_,
                    ops_per_stage));
    }

    if (num_ != FLAGS_num) {
        char msg[100];
        snprintf(msg, sizeof(msg), "(%" PRIu64 " ops)", num_);
        thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::unique_ptr<const char[]> begin_key_guard;
    Slice begin_key = AllocateKey(&begin_key_guard);
    std::unique_ptr<const char[]> end_key_guard;
    Slice end_key = AllocateKey(&end_key_guard);
    std::vector<std::unique_ptr<const char[]>> expanded_key_guards;
    std::vector<Slice> expanded_keys;
    if (FLAGS_expand_range_tombstones) {
        expanded_key_guards.resize(range_tombstone_width_);
        for (auto& expanded_key_guard : expanded_key_guards) {
            expanded_keys.emplace_back(AllocateKey(&expanded_key_guard));
        }
    }

    int64_t stage = 0;
    int64_t num_written = 0;
    while (!duration.Done(entries_per_batch_)) {
        if (duration.GetStage() != stage) {
            stage = duration.GetStage();
            if (db_.db != nullptr) {
                db_.CreateNewCf(open_options_, stage);
            } else {
                for (auto& db : multi_dbs_) {
                    db.CreateNewCf(open_options_, stage);
                }
            }
        }

        size_t id = thread->rand.Next() % num_key_gens;
        DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(id);
        batch.Clear();

        if (thread->shared->write_rate_limiter.get() != nullptr) {
            thread->shared->write_rate_limiter->Request(
                    entries_per_batch_ * (value_size_ + key_size_), Env::IO_HIGH,
                    nullptr /* stats */, RateLimiter::OpType::kWrite);
            // Set time at which last op finished to Now() to hide latency and
            // sleep from rate limiter. Also, do the check once per batch, not
            // once per write.
            thread->stats.ResetLastOpTime();
        }

        for (int64_t j = 0; j < entries_per_batch_; j++) {
            int64_t rand_num = key_gens[id]->Next();
            GenerateKeyFromInt(rand_num, FLAGS_num, &key);
            if (use_blob_db_) {
#ifndef ROCKSDB_LITE
                Slice val = gen.Generate(value_size_);
                int ttl = rand() % 86400;
                blob_db::BlobDB* blobdb =
                    static_cast<blob_db::BlobDB*>(db_with_cfh->db);
                s = blobdb->PutWithTTL(write_options_, key, val, ttl);
#endif  //  ROCKSDB_LITE
            } else if (FLAGS_num_column_families <= 1) {
                batch.Put(key, gen.Generate(value_size_));
            } else {
                // We use same rand_num as seed for key and column family so that we
                // can deterministically find the cfh corresponding to a particular
                // key while reading the key.
                batch.Put(db_with_cfh->GetCfh(rand_num), key,
                        gen.Generate(value_size_));
            }
            bytes += value_size_ + key_size_;
            ++num_written;
            if (writes_per_range_tombstone_ > 0 &&
                    num_written / writes_per_range_tombstone_ <=
                    max_num_range_tombstones_ &&
                    num_written % writes_per_range_tombstone_ == 0) {
                int64_t begin_num = key_gens[id]->Next();
                if (FLAGS_expand_range_tombstones) {
                    for (int64_t offset = 0; offset < range_tombstone_width_;
                            ++offset) {
                        GenerateKeyFromInt(begin_num + offset, FLAGS_num,
                                &expanded_keys[offset]);
                        if (use_blob_db_) {
#ifndef ROCKSDB_LITE
                            s = db_with_cfh->db->Delete(write_options_,
                                    expanded_keys[offset]);
#endif  //  ROCKSDB_LITE
                        } else if (FLAGS_num_column_families <= 1) {
                            batch.Delete(expanded_keys[offset]);
                        } else {
                            batch.Delete(db_with_cfh->GetCfh(rand_num),
                                    expanded_keys[offset]);
                        }
                    }
                } else {
                    GenerateKeyFromInt(begin_num, FLAGS_num, &begin_key);
                    GenerateKeyFromInt(begin_num + range_tombstone_width_, FLAGS_num,
                            &end_key);
                    if (use_blob_db_) {
#ifndef ROCKSDB_LITE
                        s = db_with_cfh->db->DeleteRange(
                                write_options_, db_with_cfh->db->DefaultColumnFamily(),
                                begin_key, end_key);
#endif  //  ROCKSDB_LITE
                    } else if (FLAGS_num_column_families <= 1) {
                        batch.DeleteRange(begin_key, end_key);
                    } else {
                        batch.DeleteRange(db_with_cfh->GetCfh(rand_num), begin_key,
                                end_key);
                    }
                }
            }
        }
        if (!use_blob_db_) {
#ifndef ROCKSDB_LITE
            s = db_with_cfh->db->Write(write_options_, &batch);
#endif  //  ROCKSDB_LITE
        }
        thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db,
                entries_per_batch_, kWrite);
        if (!s.ok()) {
            fprintf(stderr, "put error: %s\n", s.ToString().c_str());
            exit(1);
        }
    }
    thread->stats.AddBytes(bytes);
}

Status DoDeterministicCompact(ThreadState* thread,
        CompactionStyle compaction_style,
        WriteMode write_mode) {
#ifndef ROCKSDB_LITE
    ColumnFamilyMetaData meta;
    std::vector<DB*> db_list;
    if (db_.db != nullptr) {
        db_list.push_back(db_.db);
    } else {
        for (auto& db : multi_dbs_) {
            db_list.push_back(db.db);
        }
    }
    std::vector<Options> options_list;
    for (auto db : db_list) {
        options_list.push_back(db->GetOptions());
        if (compaction_style != kCompactionStyleFIFO) {
            db->SetOptions({{"disable_auto_compactions", "1"},
                    {"level0_slowdown_writes_trigger", "20"},
                    {"level0_stop_writes_trigger", "36"}});
        } else {
            db->SetOptions({{"disable_auto_compactions", "1"}});
        }
    }

    assert(!db_list.empty());
    auto num_db = db_list.size();
    size_t num_levels = static_cast<size_t>(open_options_.num_levels);
    size_t output_level = open_options_.num_levels - 1;
    std::vector<std::vector<std::vector<SstFileMetaData>>> sorted_runs(num_db);
    std::vector<size_t> num_files_at_level0(num_db, 0);
    if (compaction_style == kCompactionStyleLevel) {
        if (num_levels == 0) {
            return Status::InvalidArgument("num_levels should be larger than 1");
        }
        bool should_stop = false;
        while (!should_stop) {
            if (sorted_runs[0].empty()) {
                DoWrite(thread, write_mode);
            } else {
                DoWrite(thread, UNIQUE_RANDOM);
            }
            for (size_t i = 0; i < num_db; i++) {
                auto db = db_list[i];
                db->Flush(FlushOptions());
                db->GetColumnFamilyMetaData(&meta);
                if (num_files_at_level0[i] == meta.levels[0].files.size() ||
                        writes_ == 0) {
                    should_stop = true;
                    continue;
                }
                sorted_runs[i].emplace_back(
                        meta.levels[0].files.begin(),
                        meta.levels[0].files.end() - num_files_at_level0[i]);
                num_files_at_level0[i] = meta.levels[0].files.size();
                if (sorted_runs[i].back().size() == 1) {
                    should_stop = true;
                    continue;
                }
                if (sorted_runs[i].size() == output_level) {
                    auto& L1 = sorted_runs[i].back();
                    L1.erase(L1.begin(), L1.begin() + L1.size() / 3);
                    should_stop = true;
                    continue;
                }
            }
            writes_ /= static_cast<int64_t>(open_options_.max_bytes_for_level_multiplier);
        }
        for (size_t i = 0; i < num_db; i++) {
            if (sorted_runs[i].size() < num_levels - 1) {
                fprintf(stderr, "n is too small to fill %" ROCKSDB_PRIszt " levels\n", num_levels);
                exit(1);
            }
        }
        for (size_t i = 0; i < num_db; i++) {
            auto db = db_list[i];
            auto compactionOptions = CompactionOptions();
            auto options = db->GetOptions();
            MutableCFOptions mutable_cf_options(options);
            for (size_t j = 0; j < sorted_runs[i].size(); j++) {
                compactionOptions.output_file_size_limit =
                    mutable_cf_options.MaxFileSizeForLevel(
                            static_cast<int>(output_level));
                std::cout << sorted_runs[i][j].size() << std::endl;
                db->CompactFiles(compactionOptions, {sorted_runs[i][j].back().name,
                        sorted_runs[i][j].front().name},
                        static_cast<int>(output_level - j) /*level*/);
            }
        }
    } else if (compaction_style == kCompactionStyleUniversal) {
        auto ratio = open_options_.compaction_options_universal.size_ratio;
        bool should_stop = false;
        while (!should_stop) {
            if (sorted_runs[0].empty()) {
                DoWrite(thread, write_mode);
            } else {
                DoWrite(thread, UNIQUE_RANDOM);
            }
            for (size_t i = 0; i < num_db; i++) {
                auto db = db_list[i];
                db->Flush(FlushOptions());
                db->GetColumnFamilyMetaData(&meta);
                if (num_files_at_level0[i] == meta.levels[0].files.size() ||
                        writes_ == 0) {
                    should_stop = true;
                    continue;
                }
                sorted_runs[i].emplace_back(
                        meta.levels[0].files.begin(),
                        meta.levels[0].files.end() - num_files_at_level0[i]);
                num_files_at_level0[i] = meta.levels[0].files.size();
                if (sorted_runs[i].back().size() == 1) {
                    should_stop = true;
                    continue;
                }
                num_files_at_level0[i] = meta.levels[0].files.size();
            }
            writes_ =  static_cast<int64_t>(writes_* static_cast<double>(100) / (ratio + 200));
        }
        for (size_t i = 0; i < num_db; i++) {
            if (sorted_runs[i].size() < num_levels) {
                fprintf(stderr, "n is too small to fill %" ROCKSDB_PRIszt  " levels\n", num_levels);
                exit(1);
            }
        }
        for (size_t i = 0; i < num_db; i++) {
            auto db = db_list[i];
            auto compactionOptions = CompactionOptions();
            auto options = db->GetOptions();
            MutableCFOptions mutable_cf_options(options);
            for (size_t j = 0; j < sorted_runs[i].size(); j++) {
                compactionOptions.output_file_size_limit =
                    mutable_cf_options.MaxFileSizeForLevel(
                            static_cast<int>(output_level));
                db->CompactFiles(
                        compactionOptions,
                        {sorted_runs[i][j].back().name, sorted_runs[i][j].front().name},
                        (output_level > j ? static_cast<int>(output_level - j)
                         : 0) /*level*/);
            }
        }
    } else if (compaction_style == kCompactionStyleFIFO) {
        if (num_levels != 1) {
            return Status::InvalidArgument(
                    "num_levels should be 1 for FIFO compaction");
        }
        if (FLAGS_num_multi_db != 0) {
            return Status::InvalidArgument("Doesn't support multiDB");
        }
        auto db = db_list[0];
        std::vector<std::string> file_names;
        while (true) {
            if (sorted_runs[0].empty()) {
                DoWrite(thread, write_mode);
            } else {
                DoWrite(thread, UNIQUE_RANDOM);
            }
            db->Flush(FlushOptions());
            db->GetColumnFamilyMetaData(&meta);
            auto total_size = meta.levels[0].size;
            if (total_size >=
                    db->GetOptions().compaction_options_fifo.max_table_files_size) {
                for (auto file_meta : meta.levels[0].files) {
                    file_names.emplace_back(file_meta.name);
                }
                break;
            }
        }
        // TODO(shuzhang1989): Investigate why CompactFiles not working
        // auto compactionOptions = CompactionOptions();
        // db->CompactFiles(compactionOptions, file_names, 0);
        auto compactionOptions = CompactRangeOptions();
        db->CompactRange(compactionOptions, nullptr, nullptr);
    } else {
        fprintf(stdout,
                "%-12s : skipped (-compaction_stype=kCompactionStyleNone)\n",
                "filldeterministic");
        return Status::InvalidArgument("None compaction is not supported");
    }

    // Verify seqno and key range
    // Note: the seqno get changed at the max level by implementation
    // optimization, so skip the check of the max level.
#ifndef NDEBUG
    for (size_t k = 0; k < num_db; k++) {
        auto db = db_list[k];
        db->GetColumnFamilyMetaData(&meta);
        // verify the number of sorted runs
        if (compaction_style == kCompactionStyleLevel) {
            assert(num_levels - 1 == sorted_runs[k].size());
        } else if (compaction_style == kCompactionStyleUniversal) {
            assert(meta.levels[0].files.size() + num_levels - 1 ==
                    sorted_runs[k].size());
        } else if (compaction_style == kCompactionStyleFIFO) {
            // TODO(gzh): FIFO compaction
            db->GetColumnFamilyMetaData(&meta);
            auto total_size = meta.levels[0].size;
            assert(total_size <=
                    db->GetOptions().compaction_options_fifo.max_table_files_size);
            break;
        }

        // verify smallest/largest seqno and key range of each sorted run
        auto max_level = num_levels - 1;
        int level;
        for (size_t i = 0; i < sorted_runs[k].size(); i++) {
            level = static_cast<int>(max_level - i);
            SequenceNumber sorted_run_smallest_seqno = kMaxSequenceNumber;
            SequenceNumber sorted_run_largest_seqno = 0;
            std::string sorted_run_smallest_key, sorted_run_largest_key;
            bool first_key = true;
            for (auto fileMeta : sorted_runs[k][i]) {
                sorted_run_smallest_seqno =
                    std::min(sorted_run_smallest_seqno, fileMeta.smallest_seqno);
                sorted_run_largest_seqno =
                    std::max(sorted_run_largest_seqno, fileMeta.largest_seqno);
                if (first_key ||
                        db->DefaultColumnFamily()->GetComparator()->Compare(
                            fileMeta.smallestkey, sorted_run_smallest_key) < 0) {
                    sorted_run_smallest_key = fileMeta.smallestkey;
                }
                if (first_key ||
                        db->DefaultColumnFamily()->GetComparator()->Compare(
                            fileMeta.largestkey, sorted_run_largest_key) > 0) {
                    sorted_run_largest_key = fileMeta.largestkey;
                }
                first_key = false;
            }
            if (compaction_style == kCompactionStyleLevel ||
                    (compaction_style == kCompactionStyleUniversal && level > 0)) {
                SequenceNumber level_smallest_seqno = kMaxSequenceNumber;
                SequenceNumber level_largest_seqno = 0;
                for (auto fileMeta : meta.levels[level].files) {
                    level_smallest_seqno =
                        std::min(level_smallest_seqno, fileMeta.smallest_seqno);
                    level_largest_seqno =
                        std::max(level_largest_seqno, fileMeta.largest_seqno);
                }
                assert(sorted_run_smallest_key ==
                        meta.levels[level].files.front().smallestkey);
                assert(sorted_run_largest_key ==
                        meta.levels[level].files.back().largestkey);
                if (level != static_cast<int>(max_level)) {
                    // compaction at max_level would change sequence number
                    assert(sorted_run_smallest_seqno == level_smallest_seqno);
                    assert(sorted_run_largest_seqno == level_largest_seqno);
                }
            } else if (compaction_style == kCompactionStyleUniversal) {
                // level <= 0 means sorted runs on level 0
                auto level0_file =
                    meta.levels[0].files[sorted_runs[k].size() - 1 - i];
                assert(sorted_run_smallest_key == level0_file.smallestkey);
                assert(sorted_run_largest_key == level0_file.largestkey);
                if (level != static_cast<int>(max_level)) {
                    assert(sorted_run_smallest_seqno == level0_file.smallest_seqno);
                    assert(sorted_run_largest_seqno == level0_file.largest_seqno);
                }
            }
        }
    }
#endif
    // print the size of each sorted_run
    for (size_t k = 0; k < num_db; k++) {
        auto db = db_list[k];
        fprintf(stdout,
                "---------------------- DB %" ROCKSDB_PRIszt " LSM ---------------------\n", k);
        db->GetColumnFamilyMetaData(&meta);
        for (auto& levelMeta : meta.levels) {
            if (levelMeta.files.empty()) {
                continue;
            }
            if (levelMeta.level == 0) {
                for (auto& fileMeta : levelMeta.files) {
                    fprintf(stdout, "Level[%d]: %s(size: %" PRIu64 " bytes)\n",
                            levelMeta.level, fileMeta.name.c_str(), fileMeta.size);
                }
            } else {
                fprintf(stdout, "Level[%d]: %s - %s(total size: %" PRIi64 " bytes)\n",
                        levelMeta.level, levelMeta.files.front().name.c_str(),
                        levelMeta.files.back().name.c_str(), levelMeta.size);
            }
        }
    }
    for (size_t i = 0; i < num_db; i++) {
        db_list[i]->SetOptions(
                {{"disable_auto_compactions",
                std::to_string(options_list[i].disable_auto_compactions)},
                {"level0_slowdown_writes_trigger",
                std::to_string(options_list[i].level0_slowdown_writes_trigger)},
                {"level0_stop_writes_trigger",
                std::to_string(options_list[i].level0_stop_writes_trigger)}});
    }
    return Status::OK();
#else
    fprintf(stderr, "Rocksdb Lite doesn't support filldeterministic\n");
    return Status::NotSupported(
            "Rocksdb Lite doesn't support filldeterministic");
#endif  // ROCKSDB_LITE
}

void ReadSequential(ThreadState* thread) {
    if (db_.db != nullptr) {
        ReadSequential(thread, db_.db);
    } else {
        for (const auto& db_with_cfh : multi_dbs_) {
            ReadSequential(thread, db_with_cfh.db);
        }
    }
}

void ReadSequential(ThreadState* thread, DB* db) {
    ReadOptions options(FLAGS_verify_checksum, true);
    options.tailing = FLAGS_use_tailing_iterator;

    Iterator* iter = db->NewIterator(options);
    int64_t i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
        bytes += iter->key().size() + iter->value().size();
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
        ++i;

        if (thread->shared->read_rate_limiter.get() != nullptr &&
                i % 1024 == 1023) {
            thread->shared->read_rate_limiter->Request(1024, Env::IO_HIGH,
                    nullptr /* stats */,
                    RateLimiter::OpType::kRead);
        }
    }

    delete iter;
    thread->stats.AddBytes(bytes);
    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
        thread->stats.AddMessage(get_perf_context()->ToString());
    }
}

void ReadReverse(ThreadState* thread) {
    if (db_.db != nullptr) {
        ReadReverse(thread, db_.db);
    } else {
        for (const auto& db_with_cfh : multi_dbs_) {
            ReadReverse(thread, db_with_cfh.db);
        }
    }
}

void ReadReverse(ThreadState* thread, DB* db) {
    Iterator* iter = db->NewIterator(ReadOptions(FLAGS_verify_checksum, true));
    int64_t i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
        bytes += iter->key().size() + iter->value().size();
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
        ++i;
        if (thread->shared->read_rate_limiter.get() != nullptr &&
                i % 1024 == 1023) {
            thread->shared->read_rate_limiter->Request(1024, Env::IO_HIGH,
                    nullptr /* stats */,
                    RateLimiter::OpType::kRead);
        }
    }
    delete iter;
    thread->stats.AddBytes(bytes);
}

void ReadRandomFast(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t nonexist = 0;
    ReadOptions options(FLAGS_verify_checksum, true);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::string value;
    DB* db = SelectDBWithCfh(thread)->db;

    int64_t pot = 1;
    while (pot < FLAGS_num) {
        pot <<= 1;
    }

    Duration duration(FLAGS_duration, reads_);
    do {
        for (int i = 0; i < 100; ++i) {
            int64_t key_rand = thread->rand.Next() & (pot - 1);
            GenerateKeyFromInt(key_rand, FLAGS_num, &key);
            ++read;
            auto status = db->Get(options, key, &value);
            if (status.ok()) {
                ++found;
            } else if (!status.IsNotFound()) {
                fprintf(stderr, "Get returned an error: %s\n",
                        status.ToString().c_str());
                abort();
            }
            if (key_rand >= FLAGS_num) {
                ++nonexist;
            }
        }
        if (thread->shared->read_rate_limiter.get() != nullptr) {
            thread->shared->read_rate_limiter->Request(
                    100, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
        }

        thread->stats.FinishedOps(nullptr, db, 100, kRead);
    } while (!duration.Done(100));

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found, "
            "issued %" PRIu64 " non-exist keys)\n",
            found, read, nonexist);

    thread->stats.AddMessage(msg);

    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
        thread->stats.AddMessage(get_perf_context()->ToString());
    }
}

int64_t GetRandomKey(Random64* rand) {
    uint64_t rand_int = rand->Next();
    int64_t key_rand;
    if (read_random_exp_range_ == 0) {
        key_rand = rand_int % FLAGS_num;
    } else {
        const uint64_t kBigInt = static_cast<uint64_t>(1U) << 62;
        long double order = -static_cast<long double>(rand_int % kBigInt) /
            static_cast<long double>(kBigInt) *
            read_random_exp_range_;
        long double exp_ran = std::exp(order);
        uint64_t rand_num =
            static_cast<int64_t>(exp_ran * static_cast<long double>(FLAGS_num));
        // Map to a different number to avoid locality.
        const uint64_t kBigPrime = 0x5bd1e995;
        // Overflow is like %(2^64). Will have little impact of results.
        key_rand = static_cast<int64_t>((rand_num * kBigPrime) % FLAGS_num);
    }
    return key_rand;
}

void ReadRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    ReadOptions options(FLAGS_verify_checksum, true);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    PinnableSlice pinnable_val;

    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(1)) {
        DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(thread);
        // We use same key_rand as seed for key and column family so that we can
        // deterministically find the cfh corresponding to a particular key, as it
        // is done in DoWrite method.
        int64_t key_rand = GetRandomKey(&thread->rand);
        GenerateKeyFromInt(key_rand, FLAGS_num, &key);
        read++;
        Status s;
        if (FLAGS_num_column_families > 1) {
            s = db_with_cfh->db->Get(options, db_with_cfh->GetCfh(key_rand), key,
                    &pinnable_val);
        } else {
            pinnable_val.Reset();
            s = db_with_cfh->db->Get(options,
                    db_with_cfh->db->DefaultColumnFamily(), key,
                    &pinnable_val);
        }
        if (s.ok()) {
            found++;
            bytes += key.size() + pinnable_val.size();
        } else if (!s.IsNotFound()) {
            fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
            abort();
        }

        if (thread->shared->read_rate_limiter.get() != nullptr &&
                read % 256 == 255) {
            thread->shared->read_rate_limiter->Request(
                    256, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
        }

        thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)\n",
            found, read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);

    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
        thread->stats.AddMessage(get_perf_context()->ToString());
    }
}

// Calls MultiGet over a list of keys from a random distribution.
// Returns the total number of keys found.
void MultiReadRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t num_multireads = 0;
    int64_t found = 0;
    ReadOptions options(FLAGS_verify_checksum, true);
    std::vector<Slice> keys;
    std::vector<std::unique_ptr<const char[]> > key_guards;
    std::vector<std::string> values(entries_per_batch_);
    while (static_cast<int64_t>(keys.size()) < entries_per_batch_) {
        key_guards.push_back(std::unique_ptr<const char[]>());
        keys.push_back(AllocateKey(&key_guards.back()));
    }

    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(1)) {
        DB* db = SelectDB(thread);
        for (int64_t i = 0; i < entries_per_batch_; ++i) {
            GenerateKeyFromInt(GetRandomKey(&thread->rand), FLAGS_num, &keys[i]);
        }
        std::vector<Status> statuses = db->MultiGet(options, keys, &values);
        assert(static_cast<int64_t>(statuses.size()) == entries_per_batch_);

        read += entries_per_batch_;
        num_multireads++;
        for (int64_t i = 0; i < entries_per_batch_; ++i) {
            if (statuses[i].ok()) {
                ++found;
            } else if (!statuses[i].IsNotFound()) {
                fprintf(stderr, "MultiGet returned an error: %s\n",
                        statuses[i].ToString().c_str());
                abort();
            }
        }
        if (thread->shared->read_rate_limiter.get() != nullptr &&
                num_multireads % 256 == 255) {
            thread->shared->read_rate_limiter->Request(
                    256 * entries_per_batch_, Env::IO_HIGH, nullptr /* stats */,
                    RateLimiter::OpType::kRead);
        }
        thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)",
            found, read);
    thread->stats.AddMessage(msg);
}

void IteratorCreation(ThreadState* thread) {
    Duration duration(FLAGS_duration, reads_);
    ReadOptions options(FLAGS_verify_checksum, true);
    while (!duration.Done(1)) {
        DB* db = SelectDB(thread);
        Iterator* iter = db->NewIterator(options);
        delete iter;
        thread->stats.FinishedOps(nullptr, db, 1, kOthers);
    }
}

void IteratorCreationWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
        IteratorCreation(thread);
    } else {
        BGWriter(thread, kWrite);
    }
}

void SeekRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    ReadOptions options(FLAGS_verify_checksum, true);
    options.tailing = FLAGS_use_tailing_iterator;

    Iterator* single_iter = nullptr;
    std::vector<Iterator*> multi_iters;
    if (db_.db != nullptr) {
        single_iter = db_.db->NewIterator(options);
    } else {
        for (const auto& db_with_cfh : multi_dbs_) {
            multi_iters.push_back(db_with_cfh.db->NewIterator(options));
        }
    }

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    Duration duration(FLAGS_duration, reads_);
    char value_buffer[256];
    while (!duration.Done(1)) {
        if (!FLAGS_use_tailing_iterator) {
            if (db_.db != nullptr) {
                delete single_iter;
                single_iter = db_.db->NewIterator(options);
            } else {
                for (auto iter : multi_iters) {
                    delete iter;
                }
                multi_iters.clear();
                for (const auto& db_with_cfh : multi_dbs_) {
                    multi_iters.push_back(db_with_cfh.db->NewIterator(options));
                }
            }
        }
        // Pick a Iterator to use
        Iterator* iter_to_use = single_iter;
        if (single_iter == nullptr) {
            iter_to_use = multi_iters[thread->rand.Next() % multi_iters.size()];
        }

        GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
        iter_to_use->Seek(key);
        read++;
        if (iter_to_use->Valid() && iter_to_use->key().compare(key) == 0) {
            found++;
        }

        for (int j = 0; j < FLAGS_seek_nexts && iter_to_use->Valid(); ++j) {
            // Copy out iterator's value to make sure we read them.
            Slice value = iter_to_use->value();
            memcpy(value_buffer, value.data(),
                    std::min(value.size(), sizeof(value_buffer)));
            bytes += iter_to_use->key().size() + iter_to_use->value().size();

            if (!FLAGS_reverse_iterator) {
                iter_to_use->Next();
            } else {
                iter_to_use->Prev();
            }
            assert(iter_to_use->status().ok());
        }

        if (thread->shared->read_rate_limiter.get() != nullptr &&
                read % 256 == 255) {
            thread->shared->read_rate_limiter->Request(
                    256, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
        }

        thread->stats.FinishedOps(&db_, db_.db, 1, kSeek);
    }
    delete single_iter;
    for (auto iter : multi_iters) {
        delete iter;
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)\n",
            found, read);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
        thread->stats.AddMessage(get_perf_context()->ToString());
    }
}

void SeekRandomWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
        SeekRandom(thread);
    } else {
        BGWriter(thread, kWrite);
    }
}

void SeekRandomWhileMerging(ThreadState* thread) {
    if (thread->tid > 0) {
        SeekRandom(thread);
    } else {
        BGWriter(thread, kMerge);
    }
}

void DoDelete(ThreadState* thread, bool seq) {
    WriteBatch batch;
    Duration duration(seq ? 0 : FLAGS_duration, deletes_);
    int64_t i = 0;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    while (!duration.Done(entries_per_batch_)) {
        DB* db = SelectDB(thread);
        batch.Clear();
        for (int64_t j = 0; j < entries_per_batch_; ++j) {
            const int64_t k = seq ? i + j : (thread->rand.Next() % FLAGS_num);
            GenerateKeyFromInt(k, FLAGS_num, &key);
            batch.Delete(key);
        }
        auto s = db->Write(write_options_, &batch);
        thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kDelete);
        if (!s.ok()) {
            fprintf(stderr, "del error: %s\n", s.ToString().c_str());
            exit(1);
        }
        i += entries_per_batch_;
    }
}

void DeleteSeq(ThreadState* thread) {
    DoDelete(thread, true);
}

void DeleteRandom(ThreadState* thread) {
    DoDelete(thread, false);
}

void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
        ReadRandom(thread);
    } else {
        BGWriter(thread, kWrite);
    }
}

void ReadWhileMerging(ThreadState* thread) {
    if (thread->tid > 0) {
        ReadRandom(thread);
    } else {
        BGWriter(thread, kMerge);
    }
}

void BGWriter(ThreadState* thread, enum OperationType write_merge) {
    // Special thread that keeps writing until other threads are done.
    RandomGenerator gen;
    int64_t bytes = 0;

    std::unique_ptr<RateLimiter> write_rate_limiter;
    if (FLAGS_benchmark_write_rate_limit > 0) {

        printf (">>>> FLAGS_benchmark_write_rate_limit  BGWriter\n"); 
        write_rate_limiter.reset(
                NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }

    // Don't merge stats from this thread with the readers.
    thread->stats.SetExcludeFromMerge();

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    uint32_t written = 0;
    bool hint_printed = false;

    while (true) {
        DB* db = SelectDB(thread);
        {
            MutexLock l(&thread->shared->mu);
            if (FLAGS_finish_after_writes && written == writes_) {
                fprintf(stderr, "Exiting the writer after %u writes...\n", written);
                break;
            }
            if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
                // Other threads have finished
                if (FLAGS_finish_after_writes) {
                    // Wait for the writes to be finished
                    if (!hint_printed) {
                        fprintf(stderr, "Reads are finished. Have %d more writes to do\n",
                                (int)writes_ - written);
                        hint_printed = true;
                    }
                } else {
                    // Finish the write immediately
                    break;
                }
            }
        }

        GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
        Status s;

        if (write_merge == kWrite) {
            s = db->Put(write_options_, key, gen.Generate(value_size_));
        } else {
            s = db->Merge(write_options_, key, gen.Generate(value_size_));
        }
        written++;

        if (!s.ok()) {
            fprintf(stderr, "put or merge error: %s\n", s.ToString().c_str());
            exit(1);
        }
        bytes += key.size() + value_size_;
        thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);

        if (FLAGS_benchmark_write_rate_limit > 0) {
            write_rate_limiter->Request(
                    entries_per_batch_ * (value_size_ + key_size_), Env::IO_HIGH,
                    nullptr /* stats */, RateLimiter::OpType::kWrite);
        }
    }
    thread->stats.AddBytes(bytes);
}

// Given a key K and value V, this puts (K+"0", V), (K+"1", V), (K+"2", V)
// in DB atomically i.e in a single batch. Also refer GetMany.
Status PutMany(DB* db, const WriteOptions& writeoptions, const Slice& key,
        const Slice& value) {
    std::string suffixes[3] = {"2", "1", "0"};
    std::string keys[3];

    WriteBatch batch;
    Status s;
    for (int i = 0; i < 3; i++) {
        keys[i] = key.ToString() + suffixes[i];
        batch.Put(keys[i], value);
    }

    s = db->Write(writeoptions, &batch);
    return s;
}


// Given a key K, this deletes (K+"0", V), (K+"1", V), (K+"2", V)
// in DB atomically i.e in a single batch. Also refer GetMany.
Status DeleteMany(DB* db, const WriteOptions& writeoptions,
        const Slice& key) {
    std::string suffixes[3] = {"1", "2", "0"};
    std::string keys[3];

    WriteBatch batch;
    Status s;
    for (int i = 0; i < 3; i++) {
        keys[i] = key.ToString() + suffixes[i];
        batch.Delete(keys[i]);
    }

    s = db->Write(writeoptions, &batch);
    return s;
}

// Given a key K and value V, this gets values for K+"0", K+"1" and K+"2"
// in the same snapshot, and verifies that all the values are identical.
// ASSUMES that PutMany was used to put (K, V) into the DB.
Status GetMany(DB* db, const ReadOptions& readoptions, const Slice& key,
        std::string* value) {
    std::string suffixes[3] = {"0", "1", "2"};
    std::string keys[3];
    Slice key_slices[3];
    std::string values[3];
    ReadOptions readoptionscopy = readoptions;
    readoptionscopy.snapshot = db->GetSnapshot();
    Status s;
    for (int i = 0; i < 3; i++) {
        keys[i] = key.ToString() + suffixes[i];
        key_slices[i] = keys[i];
        s = db->Get(readoptionscopy, key_slices[i], value);
        if (!s.ok() && !s.IsNotFound()) {
            fprintf(stderr, "get error: %s\n", s.ToString().c_str());
            values[i] = "";
            // we continue after error rather than exiting so that we can
            // find more errors if any
        } else if (s.IsNotFound()) {
            values[i] = "";
        } else {
            values[i] = *value;
        }
    }
    db->ReleaseSnapshot(readoptionscopy.snapshot);

    if ((values[0] != values[1]) || (values[1] != values[2])) {
        fprintf(stderr, "inconsistent values for key %s: %s, %s, %s\n",
                key.ToString().c_str(), values[0].c_str(), values[1].c_str(),
                values[2].c_str());
        // we continue after error rather than exiting so that we can
        // find more errors if any
    }

    return s;
}

// Differs from readrandomwriterandom in the following ways:
// (a) Uses GetMany/PutMany to read/write key values. Refer to those funcs.
// (b) Does deletes as well (per FLAGS_deletepercent)
// (c) In order to achieve high % of 'found' during lookups, and to do
//     multiple writes (including puts and deletes) it uses upto
//     FLAGS_numdistinct distinct keys instead of FLAGS_num distinct keys.
// (d) Does not have a MultiGet option.
void RandomWithVerify(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int delete_weight = 0;
    int64_t gets_done = 0;
    int64_t puts_done = 0;
    int64_t deletes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_
    for (int64_t i = 0; i < readwrites_; i++) {
        DB* db = SelectDB(thread);
        if (get_weight == 0 && put_weight == 0 && delete_weight == 0) {
            // one batch completed, reinitialize for next batch
            get_weight = FLAGS_readwritepercent;
            delete_weight = FLAGS_deletepercent;
            put_weight = 100 - get_weight - delete_weight;
        }
        GenerateKeyFromInt(thread->rand.Next() % FLAGS_numdistinct,
                FLAGS_numdistinct, &key);
        if (get_weight > 0) {
            // do all the gets first
            Status s = GetMany(db, options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
                fprintf(stderr, "getmany error: %s\n", s.ToString().c_str());
                // we continue after error rather than exiting so that we can
                // find more errors if any
            } else if (!s.IsNotFound()) {
                found++;
            }
            get_weight--;
            gets_done++;
            thread->stats.FinishedOps(&db_, db_.db, 1, kRead);
        } else if (put_weight > 0) {
            // then do all the corresponding number of puts
            // for all the gets we have done earlier
            Status s = PutMany(db, write_options_, key, gen.Generate(value_size_));
            if (!s.ok()) {
                fprintf(stderr, "putmany error: %s\n", s.ToString().c_str());
                exit(1);
            }
            put_weight--;
            puts_done++;
            thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);
        } else if (delete_weight > 0) {
            Status s = DeleteMany(db, write_options_, key);
            if (!s.ok()) {
                fprintf(stderr, "deletemany error: %s\n", s.ToString().c_str());
                exit(1);
            }
            delete_weight--;
            deletes_done++;
            thread->stats.FinishedOps(&db_, db_.db, 1, kDelete);
        }
    }
    char msg[128];
    snprintf(msg, sizeof(msg),
            "( get:%" PRIu64 " put:%" PRIu64 " del:%" PRIu64 " total:%" \
            PRIu64 " found:%" PRIu64 ")",
            gets_done, puts_done, deletes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
}




// Differs from readrandomwriterandom in the following ways:
// (a) Uses GetMany/PutMany to read/write key values. Refer to those funcs.
// (b) Does deletes as well (per FLAGS_deletepercent)
// (c) In order to achieve high % of 'found' during lookups, and to do
//     multiple writes (including puts and deletes) it uses upto
//     FLAGS_numdistinct distinct keys instead of FLAGS_num distinct keys.
// (d) Does not have a MultiGet option.

void ReadWriteSkewedWorkload(ThreadState* thread) { // Workload FROM TRIAD 

    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int delete_weight = 0;
    int64_t gets_done = 0;
    int64_t puts_done = 0;
    int64_t deletes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_
    printf("Op count: %lu\n", readwrites_);
    for (int64_t i = 0; i < readwrites_; i++) {
        DB* db = SelectDB(thread);
        if (get_weight == 0 && put_weight == 0 && delete_weight == 0) {
            // one batch completed, reinitialize for next batch
            get_weight = FLAGS_readwritepercent;
            delete_weight = FLAGS_deletepercent;
            put_weight = 100 - get_weight - delete_weight;
        }

        // probability for skew; fix at 90%
        uint32_t prob = thread->rand.Next() % 100;
        uint32_t rand_key = thread->rand.Next();

        std::string a;
        if (prob <= 90){
            rand_key = rand_key % FLAGS_numdistinct;
            GenerateKeyFromInt(rand_key, FLAGS_numdistinct, &key);
        } else{
            rand_key = rand_key % FLAGS_datasize;
            GenerateKeyFromInt(rand_key, FLAGS_datasize, &key);
        }

        a = std::to_string(rand_key);
        a += '\0';
        key = Slice(a.c_str()); 

        if (get_weight > 0) {
            // do all the gets first
            Status s = db->Get(options, key, &value);

            if (!s.ok()) {
                //fprintf(stderr, "get error: %s\n", s.ToString().c_str());
                // we continue after error rather than exiting so that we can
                // find more errors if any
            } else if (!s.IsNotFound()) {
                found++;
            }
            get_weight--;
            gets_done++;
            thread->stats.FinishedOps(&db_, db_.db, 1, kRead);
        } else if (put_weight > 0) {
            // then do all the corresponding number of puts
            // for all the gets we have done earlier

            Status s = db->Put(write_options_, key, key);
            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
            }
            put_weight--;
            puts_done++;
            thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);
        } else if (delete_weight > 0) {
            Status s = db->Delete(write_options_, key);
            if (!s.ok()) {
                fprintf(stderr, "delete error: %s\n", s.ToString().c_str());
                exit(1);
            }
            delete_weight--;
            deletes_done++;
            thread->stats.FinishedOps(&db_, db_.db, 1, kDelete);
        }
    }
    char msg[100];
    snprintf(msg, sizeof(msg),
            "( get:%" PRIu64 " put:%" PRIu64 " del:%" PRIu64 " total:%" \
            PRIu64 " found:%" PRIu64 ")",
            gets_done, puts_done, deletes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
}



// This is different from ReadWhileWriting because it does not use
// an extra thread.
void ReadRandomWriteRandom(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
        DB* db = SelectDB(thread);
        GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
        if (get_weight == 0 && put_weight == 0) {
            // one batch completed, reinitialize for next batch
            get_weight = FLAGS_readwritepercent;
            put_weight = 100 - get_weight;
        }
        if (get_weight > 0) {
            // do all the gets first
            Status s = db->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
                fprintf(stderr, "get error: %s\n", s.ToString().c_str());
                // we continue after error rather than exiting so that we can
                // find more errors if any
            } else if (!s.IsNotFound()) {
                found++;
            }
            get_weight--;
            reads_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kRead);
        } else  if (put_weight > 0) {
            // then do all the corresponding number of puts
            // for all the gets we have done earlier
            Status s = db->Put(write_options_, key, gen.Generate(value_size_));
            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
            }
            put_weight--;
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
            " total:%" PRIu64 " found:%" PRIu64 ")",
            reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
}


// IMPORTANT: Need 15 threads for this to work: 
// Thread 0 is the SILK thread -- adjust compaction bandwidth accordin to user workload. 
// Threads 1 -- 8 are the worker threads who take stuff from queues
// Threads 9 -- 13 are the workload generator threads
// Thread 14 is the one that controls throughput fluctuations

void LongPeakTest(ThreadState* thread) {
    printf("Starting LongPeakTest test\n");

    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    Duration duration(FLAGS_duration, readwrites_);
    std::string value;
    long vals_generated = 0;
    long writes_done = 0;
    long reads_done = 0;
    int cur_queue = 0;
    int workerthread = 1;
    bool pausedcompaction = false;
    long prev_bandwidth_compaction_MBPS = 0;

    int steady_workload_time = 0;

    while (!duration.Done(1)) {
        DB* db = SelectDB(thread);
        if (!thread->init){
            if(thread->tid != 0 ){
                //
            } else {
                thread->shared->high_peak = 0;
                thread->shared->send_low_workload = 1;
                thread->last_thread_bucket1 = 0;
                thread->last_thread_bucket2 = 3;

            }  
            thread->init = true;
        }


        // thread tid=0 SILK thread. Responsible for dynamic compaction bandiwdth. 

        if (thread->tid == 0){

            // //check the current bandwidth for user operations 
            long cur_throughput = thread->shared->cur_ops_interval * 8 ; // 8 worker threads
            long cur_bandwidth_user_ops_MBPS = cur_throughput * FLAGS_value_size / 1000000;

            // SILK TESTING the Pause compaction work functionality
            if (!pausedcompaction && cur_bandwidth_user_ops_MBPS > 150){
                //SILK Consider this a load peak
                db->PauseCompactionWork();
                pausedcompaction = true;
            } else if (pausedcompaction && cur_bandwidth_user_ops_MBPS <= 150) {
                db->ContinueCompactionWork();
                pausedcompaction = false;
            }


            long cur_bandiwdth_compaction_MBPS = FLAGS_SILK_bandwidth_limitation - cur_bandwidth_user_ops_MBPS; //measured 200MB/s SSD bandwidth on XEON.
            if (cur_bandiwdth_compaction_MBPS < 10) {
                cur_bandiwdth_compaction_MBPS = 10;
            }

            if (FLAGS_dynamic_compaction_rate && 
                    abs(prev_bandwidth_compaction_MBPS - cur_bandiwdth_compaction_MBPS) >= 10 ){
                printf("Adjust-compaction-rate; current-client-bandwidth: %d ops/s; Bandwidth-taken: %d MB/s; Left-for-compaction: %d\n", 
                        cur_throughput, cur_bandwidth_user_ops_MBPS, cur_bandiwdth_compaction_MBPS);
                //DB* db = SelectDB(thread);
                db->GetOptions().rate_limiter->SetBytesPerSecond(cur_bandiwdth_compaction_MBPS * 1000 * 1000);
                prev_bandwidth_compaction_MBPS = cur_bandiwdth_compaction_MBPS;
            }

            usleep(FLAGS_SILK_bandwidth_check_interval); // sleep 10ms by default
                                                         // END COMMENT TO TOGGLE SILK TESTING 

        } else if (thread->tid > 8 && thread->tid < 14) {
            //printf("I'm the load generator!\n");
            //Generate a random number
            int val_size = FLAGS_value_size;;
            std::chrono::microseconds ms = std::chrono::duration_cast< std::chrono::milliseconds >
                (std::chrono::system_clock::now().time_since_epoch());

            std::pair<int, std::chrono::microseconds> val_timestamp_tuple(val_size, ms);
            thread->shared->op_queues[thread->tid - 9][workerthread].push(val_timestamp_tuple);        
            workerthread = (workerthread + 1) % 8;

            vals_generated++;

            //std::this_thread::sleep_for(std::chrono::microseconds(200)); // equivalent to 5k request/s per load generating thread

            if(thread->shared->send_low_workload == 1){
                usleep(700);
            } else{
                usleep(70);
            }

        } else if (thread->tid == 14){
            // throughput fluctuation thread.

            int sleep_time = 10000000;
            if (steady_workload_time > 200){
                if (thread->shared->send_low_workload == 1){
                    thread->shared->send_low_workload = 0;
                    printf("... thread 0 Switched to FAST workload\n");
                    sleep_time = 50000000; // 50s peak
                }
                else{
                    thread->shared->send_low_workload = 1;
                    printf("... thread 0 Switched to SLOW workload\n");
                    sleep_time = 10000000; // 10s time between peaks
                }
            }
            steady_workload_time += 10;

            std::this_thread::sleep_for(std::chrono::microseconds(sleep_time)); //sleep for 10s


        }else {
            //Worker thread
            //if my queue isn't empty, I pop the first element and I perform an operation on the DB 
            if (!thread->shared->op_queues[cur_queue][thread->tid - 1].empty()){
                std::pair<int, std::chrono::microseconds> pair_val_time = 
                    thread->shared->op_queues[cur_queue][thread->tid - 1].front();
                thread->shared->op_queues[cur_queue][thread->tid - 1].pop();
                std::chrono::microseconds out_of_queue_time = std::chrono::duration_cast< std::chrono::microseconds >
                    (std::chrono::system_clock::now().time_since_epoch());

                std::unique_ptr<const char[]> key_guard;
                Slice key = AllocateKey(&key_guard);

                std::string a;
                int rand_key = thread->rand.Next() % FLAGS_num;

                //GenerateKeyFromInt(rand_key, FLAGS_num, &key);
                a = std::to_string(rand_key);
                a += '\0';
                key = Slice(a.c_str()); 

                int op_prob = thread->rand.Next() % 100;

                if (op_prob < 50) {
                    Status s = db->Put(write_options_, key, gen.Generate(pair_val_time.first));
                    if (!s.ok()) {
                        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                        exit(1);
                    }
                    writes_done++;
                    long curops = thread->stats.FinishedOpsQUEUES(nullptr, db, 1, (pair_val_time.second).count(), kWrite);
                    if (curops != 0 && thread->tid == 1){
                        thread->shared->cur_ops_interval = curops;
                    }

                } else{
                    Status s = db->Get(options, key, &value);
                    reads_done++;
                    long curops =  thread->stats.FinishedOpsQUEUES(nullptr, db, 1, (pair_val_time.second).count(), kRead);
                    if (curops != 0 && thread->tid == 1){
                        thread->shared->cur_ops_interval = curops;
                    }

                }

            }
            cur_queue = (cur_queue + 1)%5;

        }
    }

}



//The idea of this test is that each thread has a different key/value size assigned
//The range isn't split, so the reads aren't split by key/value size among threads. 
// THIS TEST CAN ONLY BE RUN WITH 8 THREADS (for now).
void ReadRandomWriteRandomDifferentValueSizesPerThread(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    int64_t ops_done = 0;
    Duration duration(FLAGS_duration, readwrites_);

    int value_sizes_per_thread[8];
    value_sizes_per_thread[0] = 1;
    value_sizes_per_thread[1] = 10;
    value_sizes_per_thread[2] = 50;
    value_sizes_per_thread[3] = 100;
    value_sizes_per_thread[4] = 200;
    value_sizes_per_thread[5] = 500;
    value_sizes_per_thread[6] = 1000;
    value_sizes_per_thread[7] = 1000000;


    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {

        if (ops_done % 10000 == 0){
            printf("Ops done : %lu, thread: %d\n", ops_done, thread->tid);
        }
        DB* db = SelectDB(thread);
        GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
        if (get_weight == 0 && put_weight == 0) {
            // one batch completed, reinitialize for next batch
            get_weight = FLAGS_readwritepercent;
            put_weight = 100 - get_weight;
        }
        if (get_weight > 0) {
            // do all the gets first
            Status s = db->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
                fprintf(stderr, "get error: %s\n", s.ToString().c_str());
                // we continue after error rather than exiting so that we can
                // find more errors if any
            } else if (!s.IsNotFound()) {
                found++;
            }
            get_weight--;
            reads_done++;
            ops_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kRead);
        } else  if (put_weight > 0) {
            // then do all the corresponding number of puts
            // for all the gets we have done earlier

            //The generate function takes the value size in bytes.
            //In this test have values randomly fluctuate between 255B and 32000B
            // In this test, the value size is different depending on what thread is writing. 

            //printf("Size %d, thread %d\n", value_size_random, thread->tid);
            Status s = db->Put(write_options_, key, gen.Generate(value_sizes_per_thread[thread->tid]));
            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
            }
            put_weight--;
            writes_done++;
            ops_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
            " total:%" PRIu64 " found:%" PRIu64 ")",
            reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
}

//The idea of this test is that each thread has a different key/value size assigned
//The range isn't split, so the reads aren't split by key/value size among threads. 
void ReadRandomWriteRandomDifferentValueSizesPerThreadComparison(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    int64_t ops_done = 0;
    Duration duration(FLAGS_duration, readwrites_);

    int value_sizes_per_thread[8];
    value_sizes_per_thread[0] = 1;
    value_sizes_per_thread[1] = 10;
    value_sizes_per_thread[2] = 50;
    value_sizes_per_thread[3] = 100;
    value_sizes_per_thread[4] = 200;
    value_sizes_per_thread[5] = 500;
    value_sizes_per_thread[6] = 1000;
    value_sizes_per_thread[7] = 1000000;


    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {

        if (ops_done % 10000 == 0){
            printf("Thread %d, ops %lu", thread->tid, ops_done);
        }

        DB* db = SelectDB(thread);
        GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
        if (get_weight == 0 && put_weight == 0) {
            // one batch completed, reinitialize for next batch
            get_weight = FLAGS_readwritepercent;
            put_weight = 100 - get_weight;
        }
        if (get_weight > 0) {
            // do all the gets first
            Status s = db->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
                fprintf(stderr, "get error: %s\n", s.ToString().c_str());
                // we continue after error rather than exiting so that we can
                // find more errors if any
            } else if (!s.IsNotFound()) {
                found++;
            }
            get_weight--;
            reads_done++;
            ops_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kRead);
        } else  if (put_weight > 0) {
            // then do all the corresponding number of puts
            // for all the gets we have done earlier

            //The generate function takes the value size in bytes.
            //In this test have values randomly fluctuate between 255B and 32000B
            // In this test, the value size is different depending on what thread is writing. 

            //printf("Size %d, thread %d\n", value_size_random, thread->tid);
            Status s = db->Put(write_options_, key, gen.Generate(value_sizes_per_thread[thread->rand.Next()%8]));
            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
            }
            put_weight--;
            writes_done++;
            ops_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
            " total:%" PRIu64 " found:%" PRIu64 ")",
            reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
}



void ReadRandomWriteRandomDifferentValueSizes(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    int64_t ops_done = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {

        if (ops_done % 5000 == 0){
            printf("Ops done : %lu, thread: %d\n", ops_done, thread->tid);
        }
        DB* db = SelectDB(thread);
        GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
        if (get_weight == 0 && put_weight == 0) {
            // one batch completed, reinitialize for next batch
            get_weight = FLAGS_readwritepercent;
            put_weight = 100 - get_weight;
        }
        if (get_weight > 0) {
            // do all the gets first
            Status s = db->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
                fprintf(stderr, "get error: %s\n", s.ToString().c_str());
                // we continue after error rather than exiting so that we can
                // find more errors if any
            } else if (!s.IsNotFound()) {
                found++;
            }
            get_weight--;
            reads_done++;
            ops_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kRead);
        } else  if (put_weight > 0) {
            // then do all the corresponding number of puts
            // for all the gets we have done earlier

            //The generate function takes the value size in bytes.
            //In this test have values randomly fluctuate between 255B and 255000B
            int value_size_random = 255 + (thread->rand.Next() % (200000 - 255));
            //printf("Size %d, thread %d\n", value_size_random, thread->tid);
            Status s = db->Put(write_options_, key, gen.Generate(value_size_random));
            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
            }
            put_weight--;
            writes_done++;
            ops_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
            " total:%" PRIu64 " found:%" PRIu64 ")",
            reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
}

// This is different from ReadWhileWriting because it does not use
// an extra thread.
void ReadRandomWriteRandomSkewed(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
        DB* db = SelectDB(thread);


        //Allocate disjoint key ranges to threads
        int hot_range = FLAGS_num / 10;
        int prob_skewed = thread->rand.Next() % 100;
        if (prob_skewed > 10) { 
            // pick from hot range
            GenerateKeyFromInt(thread->rand.Next() % hot_range, FLAGS_num, &key);

        } else {
            // pick from cold range
            GenerateKeyFromInt( hot_range +
                    (thread->rand.Next() % (FLAGS_num - hot_range)), FLAGS_num, &key);

        }

        if (get_weight == 0 && put_weight == 0) {
            // one batch completed, reinitialize for next batch
            get_weight = FLAGS_readwritepercent;
            put_weight = 100 - get_weight;
        }
        if (get_weight > 0) {
            // do all the gets first
            Status s = db->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
                fprintf(stderr, "get error: %s\n", s.ToString().c_str());
                // we continue after error rather than exiting so that we can
                // find more errors if any
            } else if (!s.IsNotFound()) {
                found++;
            }
            get_weight--;
            reads_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kRead);
        } else  if (put_weight > 0) {
            // then do all the corresponding number of puts
            // for all the gets we have done earlier
            Status s = db->Put(write_options_, key, gen.Generate(value_size_));
            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
            }
            put_weight--;
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
            " total:%" PRIu64 " found:%" PRIu64 ")",
            reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
}

//Like ReadRandomWriteRandom, but where the key range is split per thread
void ReadRandomWriteRandomSplitRange(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);


    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
        DB* db = SelectDB(thread);

        //Allocate disjoint key ranges to threads
        int key_range = FLAGS_num/FLAGS_threads;
        int lower_bound = thread->tid * key_range;
        GenerateKeyFromInt( lower_bound + (thread->rand.Next() % key_range), FLAGS_num, &key);
        if (get_weight == 0 && put_weight == 0) {
            // one batch completed, reinitialize for next batch
            get_weight = FLAGS_readwritepercent;
            put_weight = 100 - get_weight;
        }
        if (get_weight > 0) {
            // do all the gets first
            Status s = db->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
                fprintf(stderr, "get error: %s\n", s.ToString().c_str());
                // we continue after error rather than exiting so that we can
                // find more errors if any
            } else if (!s.IsNotFound()) {
                found++;
            }
            get_weight--;
            reads_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kRead);
        } else  if (put_weight > 0) {
            // then do all the corresponding number of puts
            // for all the gets we have done earlier
            Status s = db->Put(write_options_, key, gen.Generate(value_size_));
            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
            }
            put_weight--;
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
            " total:%" PRIu64 " found:%" PRIu64 ")",
            reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
}


void ReadRandomWriteRandomSplitRangeDifferentValueSizes(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    int64_t ops_done = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);


    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
        if (ops_done % 5000 == 0){
            printf("Ops done : %lu, thread: %d\n", ops_done, thread->tid);
        }
        DB* db = SelectDB(thread);

        //Allocate disjoint key ranges to threads
        int key_range = FLAGS_num/FLAGS_threads;
        int lower_bound = thread->tid * key_range;
        GenerateKeyFromInt( lower_bound + (thread->rand.Next() % key_range), FLAGS_num, &key);
        if (get_weight == 0 && put_weight == 0) {
            // one batch completed, reinitialize for next batch
            get_weight = FLAGS_readwritepercent;
            put_weight = 100 - get_weight;
        }
        if (get_weight > 0) {
            // do all the gets first
            Status s = db->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
                fprintf(stderr, "get error: %s\n", s.ToString().c_str());
                // we continue after error rather than exiting so that we can
                // find more errors if any
            } else if (!s.IsNotFound()) {
                found++;
                ops_done++;
            }
            get_weight--;
            reads_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kRead);
        } else  if (put_weight > 0) {
            // then do all the corresponding number of puts
            // for all the gets we have done earlier
            //The generate function takes the value size in bytes.
            //In this test have values randomly fluctuate between 255B and 255000B
            int value_size_random = 255 + (thread->rand.Next() % (255000 - 255));
            Status s = db->Put(write_options_, key, gen.Generate(value_size_random));
            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
            }
            put_weight--;
            writes_done++;
            ops_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
            " total:%" PRIu64 " found:%" PRIu64 ")",
            reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
}

void YCSBFillDB(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_

    //write in order
    for (long k = 1; k <= FLAGS_num; k++){
        DB* db = SelectDB(thread);
        GenerateKeyFromInt(k, FLAGS_num, &key);

        //write
        Status s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
            //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
            //exit(1);
        }
        writes_done++;
        //printf("K= %d\n", k);
        thread->stats.FinishedOps(nullptr, db, 1, kWrite);
    }    

    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
            " total:%" PRIu64 " found:%" PRIu64 ")",
            reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
}

  void YCSBWorking(ThreadState* thread, ycsbc::CoreWorkload* workload, int load,
                   int run) {
    int remain_loading = FLAGS_load_num;
    int remain_running = FLAGS_running_num;
    const int test_duration = FLAGS_duration;
    int64_t ops_per_stage = 1;

    // load first, then run
    Duration loading_duration(FLAGS_load_duration, remain_loading, ops_per_stage);
    Duration running_duration(test_duration, remain_running, ops_per_stage);
    int stage = 0;
    //    WriteBatch batch;
    Status s;
    RandomGenerator gen;
    int64_t bytes = 0;
    Duration duration = loading_duration;
    rocksdb::ReadOptions r_op;
    rocksdb::WriteOptions w_op;

    if (load) {
      while (!duration.Done(entries_per_batch_)) {
        DB* db = SelectDB(thread);
        if (duration.GetStage() != stage) {
          stage = duration.GetStage();
          if (db_.db != nullptr) {
            db_.CreateNewCf(open_options_, stage);
          } else {
            for (auto& input_db : multi_dbs_) {
              input_db.CreateNewCf(open_options_, stage);
            }
          }
        }
        std::string key = workload->BuildKeyName();
        Slice val = gen.Generate(FLAGS_value_size);
        db_.db->Put(w_op, key, val);

        int64_t batch_bytes = 0;
        for (int64_t j = 0; j < entries_per_batch_; j++) {
          batch_bytes += val.size() + key.size();
          bytes += val.size() + key.size();
        }
        if (thread->shared->write_rate_limiter.get() != nullptr) {
          thread->shared->write_rate_limiter->Request(
              batch_bytes, Env::IO_HIGH, nullptr /* stats */,
              RateLimiter::OpType::kWrite);
          thread->stats.ResetLastOpTime();
        }
        thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kWrite);
      }
      thread->stats.AddBytes(bytes);
    }

    if (run) {
      duration = running_duration;

      Status op_status;
      int read_count = 0;
      int found_count = 0;
      int blind_updates = 0;
      while (!duration.Done(1)) {
        if (duration.GetStage() != stage) {
          stage = duration.GetStage();
          if (db_.db != nullptr) {
            db_.CreateNewCf(open_options_, stage);
          } else {
            for (auto& db : multi_dbs_) {
              db.CreateNewCf(open_options_, stage);
            }
          }
        }
        std::string data;
        uint64_t key_num = workload->NextTransactionKeyNum();
        const std::string key = workload->BuildKeyName(key_num);
        DB* db = SelectDB(thread);
        switch (workload->NextOp()) {
          case ycsbc::READ: {
            op_status = db_.db->Get(r_op, key, &data);
            if (op_status.ok()) {
              found_count++;
              bytes += key.size() + data.size();
            }
            read_count++;
            thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kRead);
          } break;
          case ycsbc::UPDATE: {

            Slice val = gen.Generate(FLAGS_value_size);
            // In rocksdb, update is just another put operation.
            op_status = db_.db->Put(w_op, key, val);
            thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kUpdate);
          } break;
          case ycsbc::INSERT: {
            Slice val = gen.Generate(FLAGS_value_size);
            // In rocksdb, update is just another put operation.
            op_status = db_.db->Put(w_op, key, val);
            thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kWrite);
          } break;
          case ycsbc::SCAN: {
            Iterator* db_iter = db_.db->NewIterator(rocksdb::ReadOptions());
            db_iter->Seek(key);
            int len = workload->scan_len_chooser_->Next();
            for (int i = 0; db_iter->Valid() && i < len; i++) {
              data = db_iter->value().ToString();
              db_iter->Next();
            }
            thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kSeek);
            delete db_iter;
          } break;
          case ycsbc::READMODIFYWRITE: {
            op_status = db_.db->Get(r_op, key, &data);
            read_count++;
            if (op_status.IsNotFound()) {
              blind_updates++;
            }
            Slice val = gen.Generate(FLAGS_value_size);
            op_status = db_.db->Put(w_op, key, val);
            thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kUpdate);
          } break;
          case ycsbc::DELETE: {
            op_status = db_.db->Delete(w_op, key);
            thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kDelete);
          } break;
          case ycsbc::MAXOPTYPE:
            throw ycsbc::utils::Exception(
                "Operation request is not recognized!");
        }
      }
      thread->stats.AddBytes(bytes);
    }
  }
 
void InitWorkload(ycsbc::CoreWorkload& wl, ycsbc::utils::Properties& props) {
    if (!FLAGS_ycsb_workload.empty()) {
        std::ifstream input(FLAGS_ycsb_workload);
        props.Load(input);
    }
    props.SetProperty(ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY,
                            std::to_string(FLAGS_load_num));
    props.SetProperty(ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY,
                            std::to_string(FLAGS_running_num)); 
    wl.Init(props);
}
void YCSBLoader(ThreadState* thread) {
    ycsbc::CoreWorkload wl;
    ycsbc::utils::Properties props;
    InitWorkload(wl, props);
    YCSBWorking(thread, &wl, true, false);
}
void YCSBRunner(ThreadState* thread) {
    ycsbc::CoreWorkload wl;
    ycsbc::utils::Properties props;
    InitWorkload(wl, props);
    YCSBWorking(thread, &wl, false, true);
}

void YCSBIntegrate(ThreadState* thread) {
    ycsbc::CoreWorkload wl;
    ycsbc::utils::Properties props;
    InitWorkload(wl, props);
    YCSBWorking(thread, &wl, true, true);
}

  void BurstWritten(ThreadState* thread, enum OperationType write_merge) {
    // Special thread that keeps writing until other threads are done.
    RandomGenerator gen;
    int64_t bytes = 0;

    std::unique_ptr<RateLimiter> write_rate_limiter;

    write_rate_limiter.reset(
        NewGenericRateLimiter(FLAGS_random_fill_average * 10000));

    // Don't merge stats from this thread with the readers.
    thread->stats.SetExcludeFromMerge();

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    uint32_t written = 0;
    bool hint_printed = false;
    uint64_t start = FLAGS_env->NowMicros();
    uint64_t last_report = FLAGS_env->NowMicros();
    while (true) {
      uint64_t now = FLAGS_env->NowMicros();
      if (now - last_report >1000000) {
        uint64_t sec = (now - start) /1000000;
        uint64_t target_speed = FLAGS_random_fill_average * 10000 *
                                bandwidth_in_one_hour[sec % 3600];
        write_rate_limiter.reset(
            NewGenericRateLimiter(target_speed + 1024));  // MB to bytes,
        last_report = FLAGS_env->NowMicros();
      }

      DB* db = SelectDB(thread);
      {
        MutexLock l(&thread->shared->mu);
        if (FLAGS_finish_after_writes && written == writes_) {
          fprintf(stderr, "Exiting the writer after %u writes...\n", written);
          break;
        }
        if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
          // Other threads have finished
          if (FLAGS_finish_after_writes) {
            // Wait for the writes to be finished
            if (!hint_printed) {
              fprintf(stderr, "Reads are finished. Have %d more writes to do\n",
                      static_cast<int>(writes_) - written);
              hint_printed = true;
            }
          } else {
            // Finish the write immediately
            break;
          }
        }
      }
      // write in random
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
      Status s;
      
      Slice val = gen.Generate(FLAGS_value_size);

      if (write_merge == kWrite) {
        s = db->Put(write_options_, key, val);
      } else {
        s = db->Merge(write_options_, key, val);
      }
      written++;

      if (!s.ok()) {
        fprintf(stderr, "put or merge error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + val.size();
      thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);

      write_rate_limiter->Request(key.size() + val.size(), Env::IO_HIGH,
                                  nullptr /* stats */,
                                  RateLimiter::OpType::kWrite);
    }
    thread->stats.AddBytes(bytes);
  }

  void BGYCSBRun(ThreadState* thread) {
    if (thread->tid > 0) {
      YCSBRunner(thread);
    } else {
      BurstWritten(thread, kWrite);
    }
  }




   // Workload A: Update heavy workload
  // This workload has a mix of 50/50 reads and writes. 
  // An application example is a session store recording recent actions.
  // Read/update ratio: 50/50
  // Default data size: 1 KB records 
  // Request distribution: zipfian
  void YCSBWorkloadA(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num);
    
    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    if (FLAGS_benchmark_write_rate_limit > 0) {
       printf(">>>> FLAGS_benchmark_write_rate_limit YCSBA \n");
      thread->shared->write_rate_limiter.reset(
          NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
       
          long k;
//default
            //Generate number from zipf distribution
            k = nextValue() % FLAGS_num;            
          GenerateKeyFromInt(k, FLAGS_num, &key);

          int next_op = thread->rand.Next() % 100;
          if (next_op < 50){
            //read
            Status s = db->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
              //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
              //exit(1);
              // we continue after error rather than exiting so that we can
              // find more errors if any
            } else if (!s.IsNotFound()) {
              found++;
              thread->stats.FinishedOps(nullptr, db, 1, kRead);
            }
            reads_done++;
            
          } else{
            //write
            if (FLAGS_benchmark_write_rate_limit > 0) {
                
                thread->shared->write_rate_limiter->Request(
                    value_size_ + key_size_, Env::IO_HIGH,
                    nullptr /* stats */, RateLimiter::OpType::kWrite);
                thread->stats.ResetLastOpTime();
            }
            Status s = db->Put(write_options_, key, gen.Generate(value_size_));
            if (!s.ok()) {
              //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
              //exit(1);
            } else{
             writes_done++;
             thread->stats.FinishedOps(nullptr, db, 1, kWrite);
            }                
      }



    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }


  // Workload B: Read mostly workload
  // This workload has a 95/5 reads/write mix. 
  // Application example: photo tagging; add a tag is an update, 
  // but most operations are to read tags.

  // Read/update ratio: 95/5
  // Default data size: 1 KB records 
  // Request distribution: zipfian
  void YCSBWorkloadB(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num);

    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);


    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);

      long k;

        //Generate number from zipf distribution
        k = nextValue() % FLAGS_num;            
      GenerateKeyFromInt(k, FLAGS_num, &key);

      int next_op = thread->rand.Next() % 100;
      if (next_op < 95){
        //read
        Status s = db->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats.FinishedOps(nullptr, db, 1, kRead);
        }
        reads_done++;
        
      } else{
        //write
        Status s = db->Get(options, key, &value);
        s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
      }

    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }

  // Workload C: Read only
  // This workload is 100% read. Application example: user profile cache, 
  // where profiles are constructed elsewhere (e.g., Hadoop).
  // Read/update ratio: 100/0
  // Default data size: 1 KB records 
  // Request distribution: zipfian
  void YCSBWorkloadC(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num);

    std::string value;
    int64_t found = 0;

    std::cout << "start" << std::endl;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);


    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);

      long k;
      k = nextValue() % FLAGS_num;            
      GenerateKeyFromInt(k, FLAGS_num, &key);
      //read
      Status s = db->Get(options, key, &value);
    std::cout << key.ToString() << std::endl;
      if (!s.ok() && !s.IsNotFound()) {
        fprintf(stderr, "get error: %s\n", s.ToString().c_str());
        // we continue after error rather than exiting so that we can
        // find more errors if any
      } else if (!s.IsNotFound()) {
        found++;
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      }
      reads_done++;
      
      //std::cout << k << "\n";

    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }


  // Workload D: Read latest workload
  // In this workload, new records are inserted, and the most recently 
  // inserted records are the most popular. Application example: 
  // user status updates; people want to read the latest.
  
  // Read/update/insert ratio: 95/0/5
  // Default data size: 1 KB records 
  // Request distribution: latest

  // The insert order for this is hashed, not ordered. The "latest" items may be 
  // scattered around the keyspace if they are keyed by userid.timestamp. A workload
  // which orders items purely by time, and demands the latest, is very different than 
  // workload here (which we believe is more typical of how people build systems.)
  void YCSBWorkloadD(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num);

    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);


    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);


      long k;
      if (FLAGS_YCSB_uniform_distribution){
        //Generate number from uniform distribution            
        k = thread->rand.Next() % FLAGS_num;
      } else { //default
        //Generate number from latest distribution
        k = next_value_latestgen() % FLAGS_num;           
      }
      GenerateKeyFromInt(k, FLAGS_num, &key);

      int next_op = thread->rand.Next() % 100;
      if (next_op < 95){
        //read
        Status s = db->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats.FinishedOps(nullptr, db, 1, kRead);
        }
        reads_done++;
        
      } else{
        //write
        Status s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
      }

    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
    
  }

  // Workload E: Short ranges. 
  // In this workload, short ranges of records are queried,
  // instead of individual records. Application example: 
  // threaded conversations, where each scan is for the posts 
  // in a given thread (assumed to be clustered by thread id).
  
  // Scan/insert ratio: 95/5
  // Default data size: 1 KB records 
  // Request distribution: latest
  // Scan Length Distribution=uniform
  // Max scan length = 100

  // The insert order is hashed, not ordered. Although the scans are ordered, it does not necessarily
  // follow that the data is inserted in order. For example, posts for thread 
  // 342 may not be inserted contiguously, but
  // instead interspersed with posts from lots of other threads. The way the YCSB 
  // client works is that it will pick a start
  // key, and then request a number of records; this works fine even for hashed insertion.
  void YCSBWorkloadE(ThreadState* thread) {

    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num);
    
    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);


    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);

      long k;
      if (FLAGS_YCSB_uniform_distribution){
        //Generate number from uniform distribution            
        k = thread->rand.Next() % FLAGS_num;
      } else { //default
        //Generate number from zipf distribution
        k = nextValue() % FLAGS_num;            
      }
      GenerateKeyFromInt(k, FLAGS_num, &key);


      int next_op = thread->rand.Next() % 100;
      if (next_op < 95){
        //scan
        
        //TODO need to draw a random number for the scan length
        //for now, scan lenght constant
        int scan_length = thread->rand.Next() % 100;

        Iterator* iter = db->NewIterator(options);
        int64_t i = 0;
        int64_t bytes = 0;
        for (iter->Seek(key); i < 100 && iter->Valid(); iter->Next()) {
          bytes += iter->key().size() + iter->value().size();
          //thread->stats.FinishedOps(nullptr, db, 1, kRead);
          ++i;

        }

        delete iter;

        reads_done++;
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      } else{
        //write
        Status s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
      }

    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);

    
  }

  
  // Workload F: Read-modify-write workload
  // In this workload, the client will read a record, 
  // modify it, and write back the changes. Application 
  // example: user database, where user records are read 
  // and modified by the user or to record user activity.

  // Read/read-modify-write ratio: 50/50
  // Default data size: 1 KB records 
  // Request distribution: zipfian

  void YCSBWorkloadF(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num);

    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);


    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);

      long k;
      if (FLAGS_YCSB_uniform_distribution){
        //Generate number from uniform distribution            
        k = thread->rand.Next() % FLAGS_num;
      } else { //default
        //Generate number from zipf distribution
        k = nextValue() % FLAGS_num;            
      }
      GenerateKeyFromInt(k, FLAGS_num, &key);

      int next_op = thread->rand.Next() % 100;
      if (next_op < 50){
        //read
        Status s = db->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats.FinishedOps(nullptr, db, 1, kRead);
        }
        reads_done++;
        
      } else{
        //read-modify-write.
        Status s = db->Get(options, key, &value);
        s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
      }

    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }

  void LatestGenerator(ThreadState* thread){
    printf("Latest Generator distribution test\n");
    // Need a zipf generator
    init_zipf_generator(0, 50);
    init_latestgen(50);

    for (int i=0; i<100; i++){
        printf("%ld\n", next_value_latestgen() );
    }

  }

  void Zipf(ThreadState* thread) {
    printf("ZIPF distribution test\n");
    init_zipf_generator(0, 1000);
    long vect[1000];
    for (int i = 0; i<1000; i++){
        vect[i] = 0;
    }
    for (int i = 0; i <= 10000; i++){
        vect[nextValue()] += 1;
    }
    for (int i = 0; i < 1000; i++){
        printf ("%ld\n", vect[i]);
    }
  }

  //Like ReadRandomWriteRandom, but where the key range is split per thread
  //There is also a 90-10 skew (10% of data is accessed 90% of the time)
  void ReadRandomWriteRandomSplitRangeSkewed(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);


    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      
      //Allocate disjoint key ranges to threads
      int key_range = FLAGS_num/FLAGS_threads;
      int hot_range = key_range / 10;
      int lower_bound = thread->tid * key_range;

      int prob_skewed = thread->rand.Next() % 100;
      if (prob_skewed > 10) { 
        // pick from hot range
        GenerateKeyFromInt( lower_bound + (thread->rand.Next() % hot_range), FLAGS_num, &key);

      } else {
        // pick from cold range
        GenerateKeyFromInt( lower_bound + hot_range + 
            (thread->rand.Next() % (key_range - hot_range)), FLAGS_num, &key);

      }

      if (get_weight == 0 && put_weight == 0) {
        // one batch completed, reinitialize for next batch
        get_weight = FLAGS_readwritepercent;
        put_weight = 100 - get_weight;
      }
      if (get_weight > 0) {
        // do all the gets first
        Status s = db->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
        }
        get_weight--;
        reads_done++;
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      } else  if (put_weight > 0) {
        // then do all the corresponding number of puts
        // for all the gets we have done earlier
        Status s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        put_weight--;
        writes_done++;
        thread->stats.FinishedOps(nullptr, db, 1, kWrite);
      }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }



  //
  // Read-modify-write for random keys
  void UpdateRandom(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int64_t bytes = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);

      auto status = db->Get(options, key, &value);
      if (status.ok()) {
        ++found;
        bytes += key.size() + value.size();
      } else if (!status.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n",
                status.ToString().c_str());
        abort();
      }

      if (thread->shared->write_rate_limiter) {
        thread->shared->write_rate_limiter->Request(
            key.size() + value_size_, Env::IO_HIGH, nullptr /*stats*/,
            RateLimiter::OpType::kWrite);
      }

      Status s = db->Put(write_options_, key, gen.Generate(value_size_));
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + value_size_;
      thread->stats.FinishedOps(nullptr, db, 1, kUpdate);
    }
    char msg[100];
    snprintf(msg, sizeof(msg),
             "( updates:%" PRIu64 " found:%" PRIu64 ")", readwrites_, found);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // Read-modify-write for random keys.
  // Each operation causes the key grow by value_size (simulating an append).
  // Generally used for benchmarking against merges of similar type
  void AppendRandom(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int64_t bytes = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // The number of iterations is the larger of read_ or write_
    Duration duration(FLAGS_duration, readwrites_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);

      auto status = db->Get(options, key, &value);
      if (status.ok()) {
        ++found;
        bytes += key.size() + value.size();
      } else if (!status.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n",
                status.ToString().c_str());
        abort();
      } else {
        // If not existing, then just assume an empty string of data
        value.clear();
      }

      // Update the value (by appending data)
      Slice operand = gen.Generate(value_size_);
      if (value.size() > 0) {
        // Use a delimiter to match the semantics for StringAppendOperator
        value.append(1,',');
      }
      value.append(operand.data(), operand.size());

      // Write back to the database
      Status s = db->Put(write_options_, key, value);
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + value.size();
      thread->stats.FinishedOps(nullptr, db, 1, kUpdate);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "( updates:%" PRIu64 " found:%" PRIu64 ")",
            readwrites_, found);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // Read-modify-write for random keys (using MergeOperator)
  // The merge operator to use should be defined by FLAGS_merge_operator
  // Adjust FLAGS_value_size so that the keys are reasonable for this operator
  // Assumes that the merge operator is non-null (i.e.: is well-defined)
  //
  // For example, use FLAGS_merge_operator="uint64add" and FLAGS_value_size=8
  // to simulate random additions over 64-bit integers using merge.
  //
  // The number of merges on the same key can be controlled by adjusting
  // FLAGS_merge_keys.
  void MergeRandom(ThreadState* thread) {
    RandomGenerator gen;
    int64_t bytes = 0;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // The number of iterations is the larger of read_ or write_
    Duration duration(FLAGS_duration, readwrites_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % merge_keys_, merge_keys_, &key);

      Status s = db->Merge(write_options_, key, gen.Generate(value_size_));

      if (!s.ok()) {
        fprintf(stderr, "merge error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + value_size_;
      thread->stats.FinishedOps(nullptr, db, 1, kMerge);
    }

    // Print some statistics
    char msg[100];
    snprintf(msg, sizeof(msg), "( updates:%" PRIu64 ")", readwrites_);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // Read and merge random keys. The amount of reads and merges are controlled
  // by adjusting FLAGS_num and FLAGS_mergereadpercent. The number of distinct
  // keys (and thus also the number of reads and merges on the same key) can be
  // adjusted with FLAGS_merge_keys.
  //
  // As with MergeRandom, the merge operator to use should be defined by
  // FLAGS_merge_operator.
  void ReadRandomMergeRandom(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t num_hits = 0;
    int64_t num_gets = 0;
    int64_t num_merges = 0;
    size_t max_length = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    Duration duration(FLAGS_duration, readwrites_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % merge_keys_, merge_keys_, &key);

      bool do_merge = int(thread->rand.Next() % 100) < FLAGS_mergereadpercent;

      if (do_merge) {
        Status s = db->Merge(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "merge error: %s\n", s.ToString().c_str());
          exit(1);
        }
        num_merges++;
        thread->stats.FinishedOps(nullptr, db, 1, kMerge);
      } else {
        Status s = db->Get(options, key, &value);
        if (value.length() > max_length)
          max_length = value.length();

        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          num_hits++;
        }
        num_gets++;
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      }
    }

    char msg[100];
    snprintf(msg, sizeof(msg),
             "(reads:%" PRIu64 " merges:%" PRIu64 " total:%" PRIu64
             " hits:%" PRIu64 " maxlength:%" ROCKSDB_PRIszt ")",
             num_gets, num_merges, readwrites_, num_hits, max_length);
    thread->stats.AddMessage(msg);
  }

  void WriteSeqSeekSeq(ThreadState* thread) {
    writes_ = FLAGS_num;
    DoWrite(thread, SEQUENTIAL);
    // exclude writes from the ops/sec calculation
    thread->stats.Start(thread->tid);

    DB* db = SelectDB(thread);
    std::unique_ptr<Iterator> iter(
      db->NewIterator(ReadOptions(FLAGS_verify_checksum, true)));

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    for (int64_t i = 0; i < FLAGS_num; ++i) {
      GenerateKeyFromInt(i, FLAGS_num, &key);
      iter->Seek(key);
      assert(iter->Valid() && iter->key() == key);
      thread->stats.FinishedOps(nullptr, db, 1, kSeek);

      for (int j = 0; j < FLAGS_seek_nexts && i + 1 < FLAGS_num; ++j) {
        if (!FLAGS_reverse_iterator) {
          iter->Next();
        } else {
          iter->Prev();
        }
        GenerateKeyFromInt(++i, FLAGS_num, &key);
        assert(iter->Valid() && iter->key() == key);
        thread->stats.FinishedOps(nullptr, db, 1, kSeek);
      }

      iter->Seek(key);
      assert(iter->Valid() && iter->key() == key);
      thread->stats.FinishedOps(nullptr, db, 1, kSeek);
    }
  }

#ifndef ROCKSDB_LITE
  // This benchmark stress tests Transactions.  For a given --duration (or
  // total number of --writes, a Transaction will perform a read-modify-write
  // to increment the value of a key in each of N(--transaction-sets) sets of
  // keys (where each set has --num keys).  If --threads is set, this will be
  // done in parallel.
  //
  // To test transactions, use --transaction_db=true.  Not setting this
  // parameter
  // will run the same benchmark without transactions.
  //
  // RandomTransactionVerify() will then validate the correctness of the results
  // by checking if the sum of all keys in each set is the same.
  void RandomTransaction(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    Duration duration(FLAGS_duration, readwrites_);
    ReadOptions read_options(FLAGS_verify_checksum, true);
    uint16_t num_prefix_ranges = static_cast<uint16_t>(FLAGS_transaction_sets);
    uint64_t transactions_done = 0;

    if (num_prefix_ranges == 0 || num_prefix_ranges > 9999) {
      fprintf(stderr, "invalid value for transaction_sets\n");
      abort();
    }

    TransactionOptions txn_options;
    txn_options.lock_timeout = FLAGS_transaction_lock_timeout;
    txn_options.set_snapshot = FLAGS_transaction_set_snapshot;

    RandomTransactionInserter inserter(&thread->rand, write_options_,
                                       read_options, FLAGS_num,
                                       num_prefix_ranges);

    if (FLAGS_num_multi_db > 1) {
      fprintf(stderr,
              "Cannot run RandomTransaction benchmark with "
              "FLAGS_multi_db > 1.");
      abort();
    }

    while (!duration.Done(1)) {
      bool success;

      // RandomTransactionInserter will attempt to insert a key for each
      // # of FLAGS_transaction_sets
      if (FLAGS_optimistic_transaction_db) {
        success = inserter.OptimisticTransactionDBInsert(db_.opt_txn_db);
      } else if (FLAGS_transaction_db) {
        TransactionDB* txn_db = reinterpret_cast<TransactionDB*>(db_.db);
        success = inserter.TransactionDBInsert(txn_db, txn_options);
      } else {
        success = inserter.DBInsert(db_.db);
      }

      if (!success) {
        fprintf(stderr, "Unexpected error: %s\n",
                inserter.GetLastStatus().ToString().c_str());
        abort();
      }

      thread->stats.FinishedOps(nullptr, db_.db, 1, kOthers);
      transactions_done++;
    }

    char msg[100];
    if (FLAGS_optimistic_transaction_db || FLAGS_transaction_db) {
      snprintf(msg, sizeof(msg),
               "( transactions:%" PRIu64 " aborts:%" PRIu64 ")",
               transactions_done, inserter.GetFailureCount());
    } else {
      snprintf(msg, sizeof(msg), "( batches:%" PRIu64 " )", transactions_done);
    }
    thread->stats.AddMessage(msg);

    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
      thread->stats.AddMessage(get_perf_context()->ToString());
    }
  }

  // Verifies consistency of data after RandomTransaction() has been run.
  // Since each iteration of RandomTransaction() incremented a key in each set
  // by the same value, the sum of the keys in each set should be the same.
  void RandomTransactionVerify() {
    if (!FLAGS_transaction_db && !FLAGS_optimistic_transaction_db) {
      // transactions not used, nothing to verify.
      return;
    }

    Status s =
        RandomTransactionInserter::Verify(db_.db,
                            static_cast<uint16_t>(FLAGS_transaction_sets));

    if (s.ok()) {
      fprintf(stdout, "RandomTransactionVerify Success.\n");
    } else {
      fprintf(stdout, "RandomTransactionVerify FAILED!!\n");
    }
  }
#endif  // ROCKSDB_LITE

  // Writes and deletes random keys without overwriting keys.
  //
  // This benchmark is intended to partially replicate the behavior of MyRocks
  // secondary indices: All data is stored in keys and updates happen by
  // deleting the old version of the key and inserting the new version.
  void RandomReplaceKeys(ThreadState* thread) {
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::vector<uint32_t> counters(FLAGS_numdistinct, 0);
    size_t max_counter = 50;
    RandomGenerator gen;

    Status s;
    DB* db = SelectDB(thread);
    for (int64_t i = 0; i < FLAGS_numdistinct; i++) {
      GenerateKeyFromInt(i * max_counter, FLAGS_num, &key);
      s = db->Put(write_options_, key, gen.Generate(value_size_));
      if (!s.ok()) {
        fprintf(stderr, "Operation failed: %s\n", s.ToString().c_str());
        exit(1);
      }
    }

    db->GetSnapshot();

    std::default_random_engine generator;
    std::normal_distribution<double> distribution(FLAGS_numdistinct / 2.0,
                                                  FLAGS_stddev);
    Duration duration(FLAGS_duration, FLAGS_num);
    while (!duration.Done(1)) {
      int64_t rnd_id = static_cast<int64_t>(distribution(generator));
      int64_t key_id = std::max(std::min(FLAGS_numdistinct - 1, rnd_id),
                                static_cast<int64_t>(0));
      GenerateKeyFromInt(key_id * max_counter + counters[key_id], FLAGS_num,
                         &key);
      s = FLAGS_use_single_deletes ? db->SingleDelete(write_options_, key)
                                   : db->Delete(write_options_, key);
      if (s.ok()) {
        counters[key_id] = (counters[key_id] + 1) % max_counter;
        GenerateKeyFromInt(key_id * max_counter + counters[key_id], FLAGS_num,
                           &key);
        s = db->Put(write_options_, key, Slice());
      }

      if (!s.ok()) {
        fprintf(stderr, "Operation failed: %s\n", s.ToString().c_str());
        exit(1);
      }

      thread->stats.FinishedOps(nullptr, db, 1, kOthers);
    }

    char msg[200];
    snprintf(msg, sizeof(msg),
             "use single deletes: %d, "
             "standard deviation: %lf\n",
             FLAGS_use_single_deletes, FLAGS_stddev);
    thread->stats.AddMessage(msg);
  }

  void TimeSeriesReadOrDelete(ThreadState* thread, bool do_deletion) {
    ReadOptions options(FLAGS_verify_checksum, true);
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;

    Iterator* iter = nullptr;
    // Only work on single database
    assert(db_.db != nullptr);
    iter = db_.db->NewIterator(options);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    char value_buffer[256];
    while (true) {
      {
        MutexLock l(&thread->shared->mu);
        if (thread->shared->num_done >= 1) {
          // Write thread have finished
          break;
        }
      }
      if (!FLAGS_use_tailing_iterator) {
        delete iter;
        iter = db_.db->NewIterator(options);
      }
      // Pick a Iterator to use

      int64_t key_id = thread->rand.Next() % FLAGS_key_id_range;
      GenerateKeyFromInt(key_id, FLAGS_num, &key);
      // Reset last 8 bytes to 0
      char* start = const_cast<char*>(key.data());
      start += key.size() - 8;
      memset(start, 0, 8);
      ++read;

      bool key_found = false;
      // Seek the prefix
      for (iter->Seek(key); iter->Valid() && iter->key().starts_with(key);
           iter->Next()) {
        key_found = true;
        // Copy out iterator's value to make sure we read them.
        if (do_deletion) {
          bytes += iter->key().size();
          if (KeyExpired(timestamp_emulator_.get(), iter->key())) {
            thread->stats.FinishedOps(&db_, db_.db, 1, kDelete);
            db_.db->Delete(write_options_, iter->key());
          } else {
            break;
          }
        } else {
          bytes += iter->key().size() + iter->value().size();
          thread->stats.FinishedOps(&db_, db_.db, 1, kRead);
          Slice value = iter->value();
          memcpy(value_buffer, value.data(),
                 std::min(value.size(), sizeof(value_buffer)));

          assert(iter->status().ok());
        }
      }
      found += key_found;

      if (thread->shared->read_rate_limiter.get() != nullptr) {
        thread->shared->read_rate_limiter->Request(
            1, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
      }
    }
    delete iter;

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)", found,
             read);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
      thread->stats.AddMessage(get_perf_context()->ToString());
    }
  }

  void TimeSeriesWrite(ThreadState* thread) {
    // Special thread that keeps writing until other threads are done.
    RandomGenerator gen;
    int64_t bytes = 0;

    // Don't merge stats from this thread with the readers.
    thread->stats.SetExcludeFromMerge();

    std::unique_ptr<RateLimiter> write_rate_limiter;
    if (FLAGS_benchmark_write_rate_limit > 0) {
      printf(">>>> FLAGS_benchmark_write_rate_limit TimeSeriesWrite\n");
      write_rate_limiter.reset(
          NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    Duration duration(FLAGS_duration, writes_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);

      uint64_t key_id = thread->rand.Next() % FLAGS_key_id_range;
      // Write key id
      GenerateKeyFromInt(key_id, FLAGS_num, &key);
      // Write timestamp

      char* start = const_cast<char*>(key.data());
      char* pos = start + 8;
      int bytes_to_fill =
          std::min(key_size_ - static_cast<int>(pos - start), 8);
      uint64_t timestamp_value = timestamp_emulator_->Get();
      if (port::kLittleEndian) {
        for (int i = 0; i < bytes_to_fill; ++i) {
          pos[i] = (timestamp_value >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
        }
      } else {
        memcpy(pos, static_cast<void*>(&timestamp_value), bytes_to_fill);
      }

      timestamp_emulator_->Inc();

      Status s;

      s = db->Put(write_options_, key, gen.Generate(value_size_));

      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes = key.size() + value_size_;
      thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);
      thread->stats.AddBytes(bytes);

      if (FLAGS_benchmark_write_rate_limit > 0) {
        write_rate_limiter->Request(
            entries_per_batch_ * (value_size_ + key_size_), Env::IO_HIGH,
            nullptr /* stats */, RateLimiter::OpType::kWrite);
      }
    }
  }

  void TimeSeries(ThreadState* thread) {
    if (thread->tid > 0) {
      bool do_deletion = FLAGS_expire_style == "delete" &&
                         thread->tid <= FLAGS_num_deletion_threads;
      TimeSeriesReadOrDelete(thread, do_deletion);
    } else {
      TimeSeriesWrite(thread);
      thread->stats.Stop();
      thread->stats.Report("timeseries write");
    }
  }

  void Compact(ThreadState* thread) {
    DB* db = SelectDB(thread);
    db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  }

  void CompactAll() {
    if (db_.db != nullptr) {
      db_.db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      db_with_cfh.db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    }
  }

  void ResetStats() {
    if (db_.db != nullptr) {
      db_.db->ResetStats();
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      db_with_cfh.db->ResetStats();
    }
  }

  void PrintStats(const char* key) {
    if (db_.db != nullptr) {
      PrintStats(db_.db, key, false);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      PrintStats(db_with_cfh.db, key, true);
    }
  }

  void PrintStats(DB* db, const char* key, bool print_header = false) {
    if (print_header) {
      fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
    }
    std::string stats;
    if (!db->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());
    exit(0);
  }
};

int db_bench_tool(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  static bool initialized = false;
  if (!initialized) {
    SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                    " [OPTIONS]...");
    initialized = true;
  }
  ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_compaction_style_e = (rocksdb::CompactionStyle) FLAGS_compaction_style;
#ifndef ROCKSDB_LITE
  if (FLAGS_statistics && !FLAGS_statistics_string.empty()) {
    fprintf(stderr,
            "Cannot provide both --statistics and --statistics_string.\n");
    exit(1);
  }
  if (!FLAGS_statistics_string.empty()) {
    std::unique_ptr<Statistics> custom_stats_guard;
    dbstats.reset(NewCustomObject<Statistics>(FLAGS_statistics_string,
                                              &custom_stats_guard));
    custom_stats_guard.release();
    if (dbstats == nullptr) {
      fprintf(stderr, "No Statistics registered matching string: %s\n",
              FLAGS_statistics_string.c_str());
      exit(1);
    }
  }
#endif  // ROCKSDB_LITE
  if (FLAGS_statistics) {
    dbstats = rocksdb::CreateDBStatistics();
  }
  FLAGS_compaction_pri_e = (rocksdb::CompactionPri)FLAGS_compaction_pri;

  std::vector<std::string> fanout = rocksdb::StringSplit(
      FLAGS_max_bytes_for_level_multiplier_additional, ',');
  for (size_t j = 0; j < fanout.size(); j++) {
    FLAGS_max_bytes_for_level_multiplier_additional_v.push_back(
#ifndef CYGWIN
        std::stoi(fanout[j]));
#else
        stoi(fanout[j]));
#endif
  }

  FLAGS_compression_type_e =
    StringToCompressionType(FLAGS_compression_type.c_str());

#ifndef ROCKSDB_LITE
  std::unique_ptr<Env> custom_env_guard;
  if (!FLAGS_hdfs.empty() && !FLAGS_env_uri.empty()) {
    fprintf(stderr, "Cannot provide both --hdfs and --env_uri.\n");
    exit(1);
  } else if (!FLAGS_env_uri.empty()) {
    FLAGS_env = NewCustomObject<Env>(FLAGS_env_uri, &custom_env_guard);
    if (FLAGS_env == nullptr) {
      fprintf(stderr, "No Env registered for URI: %s\n", FLAGS_env_uri.c_str());
      exit(1);
    }
  }
#endif  // ROCKSDB_LITE
  if (!FLAGS_hdfs.empty()) {
    FLAGS_env  = new rocksdb::HdfsEnv(FLAGS_hdfs);
  }

  if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "NONE"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::NONE;
  else if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "NORMAL"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::NORMAL;
  else if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "SEQUENTIAL"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::SEQUENTIAL;
  else if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "WILLNEED"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::WILLNEED;
  else {
    fprintf(stdout, "Unknown compaction fadvice:%s\n",
            FLAGS_compaction_fadvice.c_str());
  }

  FLAGS_rep_factory = StringToRepFactory(FLAGS_memtablerep.c_str());

  // The number of background threads should be at least as much the
  // max number of concurrent compactions.
  FLAGS_env->SetBackgroundThreads(FLAGS_max_background_compactions);
  FLAGS_env->SetBackgroundThreads(FLAGS_max_background_flushes,
                                  rocksdb::Env::Priority::HIGH);

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db.empty()) {
    std::string default_db_path;
    rocksdb::Env::Default()->GetTestDirectory(&default_db_path);
    default_db_path += "/dbbench";
    FLAGS_db = default_db_path;
  }

  if (FLAGS_stats_interval_seconds > 0) {
    // When both are set then FLAGS_stats_interval determines the frequency
    // at which the timer is checked for FLAGS_stats_interval_seconds
    FLAGS_stats_interval = 1000;
  }

  printf("HLL-%d", FLAGS_prob);

  rocksdb::Benchmark benchmark;
  // Initialize the zipf distribution for YCSB
  init_zipf_generator(0, FLAGS_num);
  init_latestgen(FLAGS_num);

  benchmark.Run();
  return 0;
}
}  // namespace rocksdb
#endif
