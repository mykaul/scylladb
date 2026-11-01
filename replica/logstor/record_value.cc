/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/record_value.hh"

#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

#include <absl/hash/hash.h>
#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/static_visitor.hpp>

#include "mutation/converting_mutation_partition_applier.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition_serializer.hh"
#include "mutation/mutation_partition_view.hh"
#include "partition_builder.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/single_row_partition.hh"
#include "schema/schema.hh"
#include "serializer_impl.hh"
#include "idl/mutation.dist.hh"
#include "idl/mutation.dist.impl.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

namespace {

// Writes the partition field of a record value. Both overloads write the same bytes: the first
// encodes a partition from memory, the second copies one that is already encoded.
template <typename Output>
void write_partition(Output& out, const schema& s, const mutation_partition& p) {
    mutation_partition_serializer(s, p).write(ser::writer_of_mutation_partition<Output>(out));
}

template <typename Output>
void write_partition(Output& out, const schema&, const ser::mutation_partition_view& p) {
    ser::serialize(out, p);
}

template <typename Output>
void write_schema_version(Output& out, table_schema_version v) {
    ser::serializer<int64_t>::write(out, v.uuid().get_most_significant_bits());
    ser::serializer<int64_t>::write(out, v.uuid().get_least_significant_bits());
}

template <typename Input>
table_schema_version read_schema_version(Input& in) {
    const auto msb = ser::serializer<int64_t>::read(in);
    const auto lsb = ser::serializer<int64_t>::read(in);
    return table_schema_version(utils::UUID(msb, lsb));
}

// The layout of a record value and of a schema frame's value is in docs/dev/logstor.md,
// "On-Disk Format", "Record Frames".
template <typename Output, typename Partition>
void write_record_value(Output& out, const schema& s, const Partition& p) {
    write_schema_version(out, s.version());
    write_partition(out, s, p);
}

// Reads a record value field by field, in the order above. Construction reads the schema
// version; the caller then reads the partition.
template <typename Input>
class record_value_reader {
    Input& _in;
    table_schema_version _schema_version;

public:
    explicit record_value_reader(Input& in)
        : _in(in)
        , _schema_version(read_schema_version(in))
    { }

    table_schema_version schema_version() const noexcept { return _schema_version; }

    ser::mutation_partition_view read_partition() {
        return ser::deserialize(_in, std::type_identity<ser::mutation_partition_view>{});
    }
};

// Not absl::flat_hash_map: release builds keep its debug checks, which cost on every lookup.
// Node based, so entries keep their address.
using known_schema_map = std::unordered_map<table_schema_version, known_schema, schema_version_hash>;

known_schema_map& known_schemas() noexcept {
    static thread_local known_schema_map schemas;
    return schemas;
}

// Counts the erasures of entries, so a scan that was suspended can tell its pointers may be stale.
uint64_t& erase_count() noexcept {
    static thread_local uint64_t count = 0;
    return count;
}

// The schema frame of a value, padded, so that an append copies it as is.
bytes make_schema_frame(bytes_view value) {
    bytes frame(bytes::initialized_later(), align_up(ondisk::schema_frame_size(value.size()), ondisk::record_alignment));
    auto out = simple_memory_output_stream(reinterpret_cast<char*>(frame.data()), frame.size());
    ondisk::write_schema_frame(out, value);
    out.fill('\0', out.size());
    return frame;
}

bytes make_schema_frame(table_schema_version v, table_id table, const column_mapping& cm) {
    bytes_ostream out;
    write_schema_version(out, v);
    ser::serialize(out, table);
    ser::serialize(out, cm);
    return make_schema_frame(out.linearize());
}

// The value of an entry's schema frame, without the frame header and the padding.
bytes_view schema_frame_value(const known_schema& k) {
    auto in = simple_memory_input_stream(reinterpret_cast<const char*>(k.schema_frame.data()), ondisk::record_frame_header_size);
    const auto fh = ser::deserialize(in, std::type_identity<ondisk::record_frame_header>{});
    return bytes_view(k.schema_frame).substr(ondisk::record_frame_header_size, fh.value_size);
}

// The entry the last encoded record remembered: the record's writer looks it up next, and the
// next record is most often of the same version, so both skip the map.
const known_schema*& last_remembered() noexcept {
    static thread_local const known_schema* last = nullptr;
    return last;
}

void remember_column_mapping(const schema& s) {
    auto& last = last_remembered();
    if (last && last->version == s.version()) {
        return;
    }
    auto& schemas = known_schemas();
    auto it = schemas.find(s.version());
    const auto& cm = s.get_column_mapping();
    if (it == schemas.end()) {
        it = schemas.emplace(s.version(), known_schema{s.version(), s.id(), cm, make_schema_frame(s.version(), s.id(), cm), 0, true}).first;
    } else if (!it->second.verified) [[unlikely]] {
        auto& k = it->second;
        if (k.table != s.id() || k.mapping != cm) {
            logstor_logger.warn("Schema version {} was remembered from a schema frame of table {} that differs from the live schema of table {}; replacing it",
                    s.version(), k.table, s.id());
            // In place: buffers and writers hold pointers to the entry.
            k.table = s.id();
            k.mapping = cm;
            k.schema_frame = make_schema_frame(s.version(), s.id(), cm);
            // No chunk holds this frame yet, so buffers that wrote the old one write it again.
            k.chunk_stamp = 0;
        }
        k.verified = true;
    }
    last = &it->second;
}

// Returns the serialized partition of a frozen mutation.
ser::mutation_partition_view frozen_partition(const frozen_mutation& m) {
    auto in = ser::as_input_stream(m.representation());
    return ser::deserialize(in, std::type_identity<ser::mutation_view>{}).partition();
}

// Returns the timestamp of a serialized row marker, or nullopt if the row has no marker. Follows
// read_row_marker() in mutation/mutation_partition_view.cc, but reads only the timestamp.
std::optional<api::timestamp_type> marker_timestamp(
        boost::variant<ser::live_marker_view, ser::expiring_marker_view, ser::dead_marker_view,
                ser::no_marker_view, ser::unknown_variant_type> marker) {
    struct visitor : boost::static_visitor<std::optional<api::timestamp_type>> {
        std::optional<api::timestamp_type> operator()(ser::live_marker_view& v) const {
            return v.created_at();
        }
        std::optional<api::timestamp_type> operator()(ser::expiring_marker_view& v) const {
            return v.lm().created_at();
        }
        std::optional<api::timestamp_type> operator()(ser::dead_marker_view& v) const {
            return v.tomb().timestamp();
        }
        std::optional<api::timestamp_type> operator()(ser::no_marker_view&) const {
            return std::nullopt;
        }
        std::optional<api::timestamp_type> operator()(ser::unknown_variant_type&) const {
            throw std::runtime_error("logstor: record has a row marker of an unknown type");
        }
    };
    return boost::apply_visitor(visitor(), marker);
}

} // anonymous namespace

size_t schema_version_hash::operator()(table_schema_version v) const noexcept {
    return absl::HashOf(v.uuid().get_most_significant_bits(), v.uuid().get_least_significant_bits());
}

api::timestamp_type record_timestamp(const mutation& m) {
    const auto& partition = m.partition();

    for (const auto& row_entry : partition.clustered_rows()) {
        // mutation_partition_serializer does not write dummy rows, so the frozen overload has no
        // dummy rows to skip.
        if (row_entry.dummy()) {
            continue;
        }
        if (!row_entry.row().marker().is_missing()) {
            return row_entry.row().marker().timestamp();
        }
    }

    if (const auto partition_tombstone = partition.partition_tombstone(); partition_tombstone) {
        return partition_tombstone.timestamp;
    }

    throw std::runtime_error("logstor mutation has no row marker or partition tombstone timestamp");
}

api::timestamp_type record_timestamp(const frozen_mutation& m) {
    auto partition = frozen_partition(m);

    for (auto row : partition.rows()) {
        if (auto ts = marker_timestamp(row.marker())) {
            return *ts;
        }
    }

    if (const auto partition_tombstone = tombstone(partition.tomb()); partition_tombstone) {
        return partition_tombstone.timestamp;
    }

    throw std::runtime_error("logstor mutation has no row marker or partition tombstone timestamp");
}

record_value encode_record_value(const mutation& m) {
    remember_column_mapping(*m.schema());
    bytes_ostream out;
    write_record_value(out, *m.schema(), m.partition());
    return record_value(std::move(out));
}

record_value encode_record_value(const frozen_mutation& m, const schema& s) {
    remember_column_mapping(s);
    bytes_ostream out;
    write_record_value(out, s, frozen_partition(m));
    return record_value(std::move(out));
}

single_row_partition decode_record_value(const record_value& v, const schema& s, const record_header& h) {
    if (s.id() != h.table) {
        throw std::runtime_error(format("logstor: record of table {} decoded with the schema of table {} ({}.{})",
                h.table, s.id(), s.ks_name(), s.cf_name()));
    }
    auto in = ser::as_input_stream(v.representation());
    record_value_reader reader(in);

    // Decode into a mutation_partition, then move the row out of it.
    mutation_partition p(s);
    if (reader.schema_version() == s.version()) {
        auto partition_view = mutation_partition_view::from_view(reader.read_partition());
        partition_builder b(s, p);
        partition_view.accept(s, b);
    } else {
        const known_schema* known = find_known_schema(reader.schema_version());
        if (!known || known->table != h.table) [[unlikely]] {
            throw std::runtime_error(format("logstor: no column mapping for schema version {} of a record of table {}.{}{}",
                    reader.schema_version(), s.ks_name(), s.cf_name(),
                    known ? format(": the version belongs to table {}", known->table) : ""));
        }
        converting_mutation_partition_applier applier(known->mapping, s, p);
        auto partition_view = mutation_partition_view::from_view(reader.read_partition());
        partition_view.accept(known->mapping, applier);
    }
    return to_single_row_partition(std::move(p), s, h.key.key());
}

table_schema_version record_schema_version(bytes_view value) {
    auto in = simple_memory_input_stream(reinterpret_cast<const char*>(value.data()), value.size());
    return read_schema_version(in);
}

table_schema_version record_schema_version(const record_value& v) {
    auto in = ser::as_input_stream(v.representation());
    return read_schema_version(in);
}

const known_schema* remember_schema_frame(bytes_view value) {
    auto in = simple_memory_input_stream(reinterpret_cast<const char*>(value.data()), value.size());
    const auto v = read_schema_version(in);
    const auto table = ser::deserialize(in, std::type_identity<table_id>{});
    auto& schemas = known_schemas();
    auto it = schemas.find(v);
    if (it != schemas.end() && schema_frame_value(it->second) == value) [[likely]] {
        return &it->second;
    }
    auto cm = ser::deserialize(in, std::type_identity<column_mapping>{});
    if (in.size() != 0) {
        throw std::runtime_error(format("logstor: schema frame of version {} has {} bytes past its column mapping", v, in.size()));
    }
    if (it == schemas.end()) {
        it = schemas.emplace(v, known_schema{v, table, std::move(cm), make_schema_frame(value)}).first;
    } else if (table != it->second.table || cm != it->second.mapping) {
        // Equal mappings can still differ in bytes, e.g. in how type names are written.
        throw std::runtime_error(format("logstor: schema version {} has two different column mappings, or tables {} and {}",
                v, it->second.table, table));
    }
    return &it->second;
}

const known_schema* find_known_schema(table_schema_version v) noexcept {
    if (const auto* last = last_remembered(); last && last->version == v) {
        return last;
    }
    auto& schemas = known_schemas();
    auto it = schemas.find(v);
    return it != schemas.end() ? &it->second : nullptr;
}

size_t forget_schemas(const std::function<bool(const known_schema&)>& erase) {
    const auto erased = std::erase_if(known_schemas(), [&erase] (const auto& e) {
        return erase(e.second);
    });
    last_remembered() = nullptr;
    erase_count() += erased;
    return erased;
}

void forget_table_schemas(table_id table) {
    forget_schemas([table] (const known_schema& k) { return k.table == table; });
}

schema_frame_stats& get_schema_frame_stats() noexcept {
    static thread_local schema_frame_stats stats;
    return stats;
}

uint64_t known_schemas_erased() noexcept {
    return erase_count();
}

size_t known_column_mapping_count() noexcept {
    return known_schemas().size();
}

void forget_column_mappings() {
    // Retired, not freed: buffers and writers may still point to the entries.
    static thread_local std::vector<known_schema_map> retired;
    retired.push_back(std::move(known_schemas()));
    known_schemas().clear();
    last_remembered() = nullptr;
    ++erase_count();
}

} // namespace replica::logstor
