/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include <seastar/core/sstring.hh>

namespace cql3 {

/// Helpers for computing external (heap-allocated) memory usage of CQL3 types.

template <typename CharT, typename SizeT, SizeT MaxSize, bool NulTerminate>
size_t basic_sstring_external_memory_usage(
        const seastar::basic_sstring<CharT, SizeT, MaxSize, NulTerminate>& s) noexcept {
    if (s.size() > MaxSize) {
        return s.size() + (NulTerminate ? 1 : 0);
    }
    return 0;
}

inline size_t sstring_external_memory_usage(const seastar::sstring& s) noexcept {
    return basic_sstring_external_memory_usage(s);
}

template <typename T>
size_t vector_external_memory_usage(const std::vector<T>& v) noexcept {
    return v.capacity() * sizeof(T);
}

template <typename T>
size_t optional_external_memory_usage(const std::optional<T>& opt) {
    if (opt) {
        return opt->external_memory_usage();
    }
    return 0;
}

} // namespace cql3
