/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/types.hh"
#include "schema/schema.hh"

class frozen_mutation;
class mutation;

namespace replica::logstor {

struct single_row_partition;

// The functions that know what the bytes of a record_value mean. Everything else in logstor
// treats a value as bytes, so changing the encoding is a change to this file alone.
//
// The value holds the partition of the mutation and nothing the record_header already
// carries, which is why decoding takes the header: the key and the table of the record come
// from it. The layout of the value is described in docs/dev/logstor.md, "On-Disk Format".

// Returns the timestamp of the record that stores m: the timestamp of the row marker, or of the
// partition tombstone if there is no marker. Throws if m has neither. The two overloads must give
// the same result.
api::timestamp_type record_timestamp(const mutation& m);
api::timestamp_type record_timestamp(const frozen_mutation& m);

// Encodes the partition of m as the value of a record whose header is built from m.
record_value encode_record_value(const mutation& m);

// Encodes the partition of m as the value of a record whose header is built from m. s is the
// schema that m was frozen with.
record_value encode_record_value(const frozen_mutation& m, const schema& s);

// Decodes the value of a record with header h as a partition of the schema s.
single_row_partition decode_record_value(const record_value& v, const schema& s, const record_header& h);

// The schema version whose schema frame must be in the chunk of a record with this value. The
// value is at least ondisk::schema_version_size long.
table_schema_version record_schema_version(const record_value& v);
table_schema_version record_schema_version(bytes_view value);

// Per-shard map of schema version to column mapping, so reads never look for a schema frame.
// Filled by encode_record_value() and by segment scans of schema frames. Never pruned: a restart
// rebuilds it from the schema frames on disk.
struct known_schema {
    table_schema_version version;
    table_id table;
    column_mapping mapping;
    // The schema frame of the version as a chunk holds it: frame header, value and padding.
    bytes schema_frame;
    // The largest stamp of a chunk the frame was appended to. It only grows, so a stamp below a
    // chunk's was never its own.
    mutable uint64_t chunk_stamp = 0;
    // Set when the entry was made, or checked, against the live schema of a write. A scan can add
    // an entry for a version the shard never wrote, with whatever its frame claims.
    bool verified = false;
};

// Adds the version, table and column mapping of a schema frame value, unless the version is known.
// Returns the entry of the version. Throws if the value does not parse, or if a known version
// comes with another table or column mapping: a version stands for one column set forever.
const known_schema* remember_schema_frame(bytes_view value);

// The entry of the version, or nullptr if the shard does not know it.
const known_schema* find_known_schema(table_schema_version v) noexcept;

// Per-shard counts of schema frames, for metrics and for tests to check what they wrote.
struct schema_frame_stats {
    // Appended to a chunk.
    uint64_t written = 0;
    // The bytes of those frames, padding included.
    uint64_t bytes_written = 0;
    // Found by a scan and skipped, as they did not parse or conflict with a known version.
    uint64_t skipped = 0;
    // Records a scan found without the schema frame of their version in their chunk (I1).
    uint64_t records_without_frame = 0;
};
schema_frame_stats& get_schema_frame_stats() noexcept;

// For tests. forget_column_mappings() keeps the entries alive, as buffers may still point to them.
size_t known_column_mapping_count() noexcept;
void forget_column_mappings();

} // namespace replica::logstor
