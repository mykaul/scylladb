/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/segment_io.hh"

#include <seastar/core/align.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/util/log.hh>

#include "replica/logstor/record_value.hh"
#include "utils/error_injection.hh"
#include "utils/small_vector.hh"
#include "serializer_impl.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

namespace {

// Reads a segment through its input stream, and keeps the position in the segment of what it
// reads next.
class segment_reader {
    seastar::input_stream<char>& _in;
    size_t _position = 0;
public:
    explicit segment_reader(seastar::input_stream<char>& in) noexcept : _in(in) {}

    size_t position() const noexcept {
        return _position;
    }

    future<temporary_buffer<char>> read_exactly(size_t n) {
        _position += n;
        return _in.read_exactly(n);
    }

    // Reads an on-disk T, or returns nullopt if the stream ends first.
    template <typename T>
    future<std::optional<T>> read() {
        constexpr size_t size = ser::serializer<T>::serialized_size;
        return read_exactly(size).then([] (temporary_buffer<char> buf) -> std::optional<T> {
            if (buf.size() < size) {
                return std::nullopt;
            }
            return ser::deserialize_from_buffer(buf, std::type_identity<T>{});
        });
    }

    // Does nothing if the reader is at or past the position already.
    future<> skip_to(size_t position) {
        if (_position >= position) {
            return make_ready_future<>();
        }
        auto n = position - _position;
        _position = position;
        return _in.skip(n);
    }
};

// The headers at the start of a chunk: the chunk header, and the segment header in a full segment.
struct chunk_headers {
    ondisk::chunk_header chunk;
    std::optional<ondisk::segment_header> segment;
};

// Returns nullopt if the stream ends first or the headers are invalid. Throws if the chunk was
// written by an older logstor format.
future<std::optional<chunk_headers>> read_chunk_headers(segment_reader& reader, log_segment_id segment_id) {
    auto ch = co_await reader.read<ondisk::chunk_header>();
    if (!ch) {
        co_return std::nullopt;
    }
    const bool old_format = ch->magic == ondisk::chunk_header_magic && ch->version < ondisk::min_readable_version;
    if (!old_format && !ondisk::validate_chunk_header_fields(*ch)) {
        co_return std::nullopt;
    }

    std::optional<ondisk::segment_header> sh;
    if (ch->kind == segment_kind::full) {
        sh = co_await reader.read<ondisk::segment_header>();
        if (!sh) {
            co_return std::nullopt;
        }
    }

    // Only a header whose CRC matches is taken for an old chunk, not stale or garbage bytes.
    if (old_format && ch->calculate_crc(sh) == ch->crc) {
        throw std::runtime_error(format("logstor: {} has a chunk of version {}, written by an older logstor format; "
                "logstor data must be wiped before this version can start", segment_id, ch->version));
    }

    if (!ondisk::validate_chunk_headers(*ch, sh)) {
        co_return std::nullopt;
    }
    co_return chunk_headers{*ch, sh};
}

segment_info make_segment_info(const chunk_headers& headers) {
    const auto& ch = headers.chunk;
    const auto& sh = headers.segment;
    segment_info seg_info {
        .kind = ch.kind,
        .segment_seq = ch.segment_seq,
    };

    switch (ch.kind) {
    case segment_kind::full:
        // read_chunk_headers() reads the segment header of every full segment.
        if (!sh) {
            on_internal_error(logstor_logger, "Full segment chunk header without segment header");
        }
        seg_info.v = segment_info::full {
            .table = sh->table,
            .first_token = sh->first_token,
            .last_token = sh->last_token,
        };
        break;
    case segment_kind::mixed:
        seg_info.v = segment_info::mixed{};
        break;
    }
    return seg_info;
}

// Reads the body of the chunk whose headers the reader read last - its record frames. Returns
// nullopt if the body goes past the end of the segment or the stream ends first. The caller
// checks the body against the header's body CRC.
future<std::optional<temporary_buffer<char>>> read_chunk_body(segment_reader& reader,
        const ondisk::chunk_header& ch,
        size_t segment_size) {
    if (ch.records_size > segment_size - reader.position()) {
        co_return std::nullopt;
    }
    auto body = co_await reader.read_exactly(ch.records_size);
    if (body.size() < ch.records_size) {
        co_return std::nullopt;
    }
    co_return body;
}

// A frame of a chunk body: a record frame, or a schema frame, whose header is empty.
struct parsed_frame {
    ondisk::record_frame_header frame_header;
    simple_memory_input_stream header;
    bytes_view value;
};

// Reads the frame at the start of records, without its padding. Returns nullopt if the frame
// header is invalid or the frame does not fit in what is left of records.
std::optional<parsed_frame> read_frame(simple_memory_input_stream& records) {
    // The sizes a frame claims are checked against what is left of the chunk's records before
    // they are trusted, as the CRC only says the bytes are the ones written, not that the writer
    // was right.
    const size_t bytes_left = records.size();
    if (bytes_left < ondisk::record_frame_header_size) {
        return std::nullopt;
    }
    auto frame_header = ser::deserialize(records, std::type_identity<ondisk::record_frame_header>{});
    if (!ondisk::validate_record_frame_header(frame_header)) {
        return std::nullopt;
    }
    const size_t header_size = ondisk::record_header_size(frame_header);
    if (header_size + frame_header.value_size > bytes_left - ondisk::record_frame_header_size) {
        return std::nullopt;
    }

    auto header_stream = records.read_substream(header_size);
    auto value_stream = records.read_substream(frame_header.value_size);
    return parsed_frame{
        .frame_header = frame_header,
        .header = header_stream,
        .value = bytes_view(reinterpret_cast<const int8_t*>(value_stream.begin()), value_stream.size()),
    };
}

// records_position is where the records start in the segment. The body CRC matched, so a frame that
// does not parse is an error. A schema frame that cannot be read is skipped and the records after
// it are still indexed.
future<> scan_chunk_records(simple_memory_input_stream records,
        size_t records_position,
        log_segment_id segment_id,
        const record_header_consumer& on_record_header,
        const log_record_bytes_consumer& on_record,
        const record_version_consumer& on_record_version) {
    const size_t records_size = records.size();
    utils::small_vector<const known_schema*, 2> chunk_schemas;
    // The versions of chunk_schemas, to look the entries up again: on_record can erase them.
    utils::small_vector<table_schema_version, 2> chunk_versions;
    utils::small_vector<table_schema_version, 2> reported_versions;
    size_t bad_schema_frames = 0;
    size_t records_without_schema_frame = 0;
    while (records.size() > 0) {
        const size_t record_offset = records_position + (records_size - records.size());
        auto frame = read_frame(records);
        if (!frame) {
            throw std::runtime_error(format("logstor: invalid frame at offset {} of {}, in a chunk whose body CRC matches",
                    record_offset, segment_id));
        }

        if (frame->frame_header.kind == frame_kind::schema) {
            try {
                utils::get_local_injector().inject("logstor_skip_schema_frames", [] {
                    throw std::runtime_error("schema frame skipped by injection");
                });
                const auto* k = remember_schema_frame(frame->value);
                chunk_versions.push_back(k->version);
                chunk_schemas.push_back(k);
            } catch (...) {
                if (!bad_schema_frames++) {
                    logstor_logger.warn("Skipped a schema frame at offset {} of {}: {}", record_offset, segment_id, std::current_exception());
                }
            }
        } else {
            const auto header_bytes = bytes_view(reinterpret_cast<const int8_t*>(frame->header.begin()), frame->header.size());
            const auto header = ondisk::read_record_header(frame->header, frame->frame_header.key_size);
            auto bytes = log_record_bytes_view{
                .header = header_bytes,
                .value = frame->value,
            };

            logstor_logger.trace("Found record of size {} bytes in {}", bytes.header.size() + bytes.value.size(), segment_id);

            record_location loc {
                .segment = segment_id,
                .offset = static_cast<uint32_t>(record_offset),
                .size = static_cast<uint32_t>(ondisk::record_frame_size(bytes.header.size(), bytes.value.size()))
            };

            // Checked for every record, so that every scan verifies I1. A copy of the record writes
            // the schema frame found here; the map is only a fallback for a chunk that lacks it.
            const auto v = record_schema_version(frame->value);
            if (on_record_version && !std::ranges::contains(reported_versions, v)) {
                reported_versions.push_back(v);
                on_record_version(v);
            }
            auto it = std::ranges::find(chunk_schemas, v, &known_schema::version);
            const known_schema* known = it != chunk_schemas.end() ? *it : find_known_schema(v);
            if (it == chunk_schemas.end() || !known || known->table != header.table) [[unlikely]] {
                ++records_without_schema_frame;
                known = known && known->table == header.table ? known : nullptr;
            }
            bytes.schema = known;

            if (on_record_header(loc, header) == want_data::yes) {
                const auto erased_before = known_schemas_erased();
                co_await on_record(loc, header, bytes);
                if (known_schemas_erased() != erased_before) [[unlikely]] {
                    // The pointers may dangle. A version that is gone counts as a record without its frame.
                    chunk_schemas.clear();
                    size_t kept = 0;
                    for (const auto cv : chunk_versions) {
                        if (const auto* k = find_known_schema(cv)) {
                            chunk_versions[kept++] = cv;
                            chunk_schemas.push_back(k);
                        }
                    }
                    chunk_versions.erase(chunk_versions.begin() + kept, chunk_versions.end());
                }
            }
        }

        // The next record frame starts at a multiple of record_alignment.
        const size_t consumed = records_size - records.size();
        records.skip(std::min(align_up(consumed, ondisk::record_alignment) - consumed, records.size()));
    }
    if (bad_schema_frames || records_without_schema_frame) [[unlikely]] {
        get_schema_frame_stats().skipped += bad_schema_frames;
        get_schema_frame_stats().records_without_frame += records_without_schema_frame;
        logstor_logger.warn("Chunk at offset {} of {}: {} schema frames skipped, {} records read without the schema frame of their version",
                records_position, segment_id, bad_schema_frames, records_without_schema_frame);
    }
}

} // anonymous namespace

future<std::optional<segment_info>> read_segment_info(seastar::input_stream<char>& in, log_segment_id segment_id) {
    segment_reader reader(in);
    auto headers = co_await read_chunk_headers(reader, segment_id);
    if (!headers) {
        co_return std::nullopt;
    }

    co_return make_segment_info(*headers);
}

future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_info_consumer on_segment_info,
        record_header_consumer on_record_header,
        log_record_bytes_consumer on_record,
        record_version_consumer on_record_version) {
    segment_reader reader(in);
    std::optional<segment_sequence> segment_seq;
    // The number of chunks whose body does not match its CRC, and whether the last chunk of the scan
    // is one of them.
    size_t failed_chunks = 0;
    bool last_chunk_failed = false;

    logstor_logger.trace("Reading records from {}", segment_id);

    while (reader.position() < segment_size) {
        // A chunk with invalid headers, or a chunk of an earlier generation of the segment, ends the
        // scan. Chunks are written in sequence, so no chunk of this generation of the segment
        // follows it.
        auto headers = co_await read_chunk_headers(reader, segment_id);
        if (!headers) {
            break;
        }
        const auto& ch = headers->chunk;

        if (!segment_seq) {
            segment_seq = ch.segment_seq;
        } else if (ch.segment_seq != *segment_seq) {
            break;
        }

        // The segment info comes before the body check. Recovery takes the highest segment_seq from
        // it, and a segment_seq that is on disk must not go to a new segment again, even when the
        // body of its chunk is torn.
        auto seg_info = make_segment_info(*headers);
        co_await on_segment_info(seg_info);

        auto body_buf = co_await read_chunk_body(reader, ch, segment_size);
        if (!body_buf) {
            break;
        }

        // Validate the chunk before delivering records - a torn chunk can hold records of an
        // earlier use of the segment that parse cleanly.
        if (ondisk::validate_chunk_body(ch, std::span(body_buf->get(), body_buf->size()))) {
            last_chunk_failed = false;
            auto records = simple_memory_input_stream(body_buf->get(), body_buf->size());
            co_await scan_chunk_records(records, reader.position() - records.size(), segment_id, on_record_header, on_record, on_record_version);
        } else {
            // The valid header gives the size of the chunk, so the scan continues at the next
            // chunk. After a torn chunk, what follows is not a valid chunk or is a chunk of an
            // earlier generation, which ends the scan.
            last_chunk_failed = true;
            ++failed_chunks;
        }

        if (seg_info.kind == segment_kind::full) {
            // A full segment has only one chunk.
            break;
        }

        // The next chunk starts at the first block boundary after this one.
        co_await reader.skip_to(align_up(reader.position(), ondisk::block_alignment));
    }

    // A segment has one chunk write in flight at a time, and a write is durable when it completes.
    // Thus a crash can tear only the last chunk of a segment. A failed chunk before it is corrupt.
    if (failed_chunks == 1 && last_chunk_failed) {
        logstor_logger.warn("Skipped the last chunk of {}: its body CRC does not match, probably a torn write from a crash.",
                segment_id);
    } else if (failed_chunks > 0) {
        logstor_logger.warn("Skipped {} chunks of {} whose body CRC does not match, including a chunk before the last. "
                "The segment is corrupt; their records are lost.", failed_chunks, segment_id);
    }
}

future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_info_consumer on_segment_info,
        record_header_consumer on_record_header,
        log_record_consumer on_record,
        record_version_consumer on_record_version) {
    co_await scan_segment(in, segment_id, segment_size,
            std::move(on_segment_info), std::move(on_record_header),
            [on_record = std::move(on_record)] (record_location loc, const record_header& header, log_record_bytes_view record_view) {
                return on_record(loc, log_record{header, record_value(record_view.value)});
            }, std::move(on_record_version));
}

streamed_segment_rewriter::streamed_segment_rewriter(log_segment_id target_segment, segment_sequence target_seq, streamed_buffer_consumer on_buffer)
    : _target_segment(target_segment)
    , _target_seq(target_seq)
    , _on_buffer(std::move(on_buffer)) {
}

ondisk::chunk_header streamed_segment_rewriter::read_chunk_header() const {
    simple_memory_input_stream ch_stream(_pending_data.data(), ondisk::chunk_header_size);
    return ser::deserialize(ch_stream, std::type_identity<ondisk::chunk_header>{});
}

void streamed_segment_rewriter::maybe_parse_initial_header() {
    if (_initial_header_size || _pending_data.size() < ondisk::chunk_header_size) {
        return;
    }

    auto ch = read_chunk_header();
    if (!ondisk::validate_chunk_header_fields(ch)) {
        throw std::runtime_error("Invalid streamed logstor chunk header");
    }

    size_t header_size = ondisk::chunk_header_size;
    if (ch.kind == segment_kind::full) {
        header_size += ondisk::segment_header_size;
    }
    _initial_header_size = header_size;
}

void streamed_segment_rewriter::rewrite_chunk_header() {
    simple_memory_input_stream in(_pending_data.data(), *_initial_header_size);
    auto ch = ser::deserialize(in, std::type_identity<ondisk::chunk_header>{});
    std::optional<ondisk::segment_header> sh;
    if (ch.kind == segment_kind::full) {
        sh = ser::deserialize(in, std::type_identity<ondisk::segment_header>{});
    }
    if (!ondisk::validate_chunk_headers(ch, sh)) {
        throw std::runtime_error("Invalid streamed logstor chunk headers");
    }

    logstor_logger.trace("Rewriting chunk header for segment {} seq {} with seq {}", _target_segment, ch.segment_seq, _target_seq);

    ch.segment_seq = _target_seq;
    ch.crc = ch.calculate_crc(sh);

    simple_memory_output_stream ch_stream(_pending_data.data(), ondisk::chunk_header_size);
    ser::serialize<ondisk::chunk_header>(ch_stream, ch);
}

future<> streamed_segment_rewriter::flush_pending_data() {
    if (_pending_data.empty()) {
        co_return;
    }
    co_await _on_buffer(bytes_view(reinterpret_cast<const int8_t*>(_pending_data.data()), _pending_data.size()));
    _pending_data.clear();
}

future<> streamed_segment_rewriter::put(std::span<temporary_buffer<char>> data) {
    for (auto& buf : data) {
        if (buf.empty()) {
            continue;
        }

        if (_header_rewritten) {
            co_await _on_buffer(bytes_view(reinterpret_cast<const int8_t*>(buf.get()), buf.size()));
            continue;
        }

        _pending_data.insert(_pending_data.end(), buf.get(), buf.get() + buf.size());
        maybe_parse_initial_header();
        if (_initial_header_size && _pending_data.size() >= *_initial_header_size) {
            rewrite_chunk_header();
            _header_rewritten = true;
            co_await flush_pending_data();
        }
    }
}

future<> streamed_segment_rewriter::close() {
    if (!_header_rewritten && !_pending_data.empty()) {
        throw std::runtime_error("Truncated streamed logstor segment header");
    }
    co_return;
}

size_t streamed_segment_rewriter::buffer_size() const noexcept {
    return 128 * 1024;
}

}
