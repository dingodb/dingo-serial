// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "long_schema.h"

#include <cstdint>

#include "serial/utils/V2/compiler.h"

namespace dingodb {
namespace serialV2 {

constexpr int kDataLength = 8;
constexpr int kDataLengthWithNull = kDataLength + 1;

void DingoSchema<int64_t>::EncodeLongComparable(int64_t data, Buf& buf) {
  uint64_t* l = (uint64_t*)&data;
  if (DINGO_LIKELY(IsLe())) {
    buf.Write(*l >> 56 ^ 0x80);
    buf.Write(*l >> 48);
    buf.Write(*l >> 40);
    buf.Write(*l >> 32);
    buf.Write(*l >> 24);
    buf.Write(*l >> 16);
    buf.Write(*l >> 8);
    buf.Write(*l);
  } else {
    buf.Write(*l ^ 0x80);
    buf.Write(*l >> 8);
    buf.Write(*l >> 16);
    buf.Write(*l >> 24);
    buf.Write(*l >> 32);
    buf.Write(*l >> 40);
    buf.Write(*l >> 48);
    buf.Write(*l >> 56);
  }
}

int64_t DingoSchema<int64_t>::DecodeLongComparable(Buf& buf) {
  uint64_t l = (buf.Read() & 0xFF) ^ 0x80;
  if (DINGO_LIKELY(IsLe())) {
    for (int i = 0; i < 7; i++) {
      l <<= 8;
      l |= buf.Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; i++) {
      l |= (((uint64_t)buf.Read() & 0xFF) << (8 * i));
    }
  }

  return static_cast<int64_t>(l);
}

void DingoSchema<int64_t>::EncodeLongNotComparable(int64_t data, Buf& buf) {
  uint64_t* l = (uint64_t*)&data;
  if (DINGO_LIKELY(IsLe())) {
    buf.Write(*l >> 56);
    buf.Write(*l >> 48);
    buf.Write(*l >> 40);
    buf.Write(*l >> 32);
    buf.Write(*l >> 24);
    buf.Write(*l >> 16);
    buf.Write(*l >> 8);
    buf.Write(*l);
  } else {
    buf.Write(*l);
    buf.Write(*l >> 8);
    buf.Write(*l >> 16);
    buf.Write(*l >> 24);
    buf.Write(*l >> 32);
    buf.Write(*l >> 40);
    buf.Write(*l >> 48);
    buf.Write(*l >> 56);
  }
}

int64_t DingoSchema<int64_t>::DecodeLongNotComparable(Buf& buf) {
  uint64_t l = buf.Read() & 0xFF;
  if (DINGO_LIKELY(IsLe())) {
    for (int i = 0; i < 7; i++) {
      l <<= 8;
      l |= buf.Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; i++) {
      l |= (((uint64_t)buf.Read() & 0xFF) << (8 * i));
    }
  }

  return static_cast<int64_t>(l);
}

int64_t DingoSchema<int64_t>::DecodeLongNotComparable(Buf& buf, int offset) {

  return static_cast<int64_t>(buf.ReadLong(offset));
}

int DingoSchema<int64_t>::GetLengthForKey() {
  if (AllowNull()) {
    return kDataLengthWithNull;
  } else {
    return kDataLength;
  }
}

int DingoSchema<int64_t>::GetLengthForValue() { return kDataLength; }

int DingoSchema<int64_t>::SkipKey(Buf& buf) {
  int len = GetLengthForKey();
  buf.Skip(len);
  return len;
}

int DingoSchema<int64_t>::SkipValue(Buf& buf) {
  buf.Skip(kDataLength);
  return kDataLength;
}

int DingoSchema<int64_t>::EncodeKey(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (AllowNull()) {
    if (data.has_value()) {
      buf.Write(k_not_null);
      const auto& ref_data = std::any_cast<const int64_t&>(data);
      EncodeLongComparable(ref_data, buf);
    } else {
      buf.Write(k_null);
      buf.WriteLong(0);
    }

    return kDataLengthWithNull;
  } else {
    const auto& ref_data = std::any_cast<const int64_t&>(data);
    EncodeLongComparable(ref_data, buf);

    return kDataLength;
  }
}

void DingoSchema<int64_t>::EncodeKeyPrefix(const std::any& data, Buf& buf) {
  EncodeKey(data, buf);
}

// {value: 8byte}
int DingoSchema<int64_t>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    const auto& ref_data = std::any_cast<const int64_t&>(data);
    EncodeLongNotComparable(ref_data, buf);
    return kDataLength;
  }

  return 0;
}

std::any DingoSchema<int64_t>::DecodeKey(Buf& buf) {
  if (AllowNull()) {
    if (buf.Read() == k_null) {
      buf.Skip(kDataLength);
      return std::any();
    }
  }

  return std::any(DecodeLongComparable(buf));
}

inline std::any DingoSchema<int64_t>::DecodeValue(Buf& buf) {
  return std::any(DecodeLongNotComparable(buf));
}

inline std::any DingoSchema<int64_t>::DecodeValue(Buf& buf, int offset) {
  return std::any(DecodeLongNotComparable(buf, offset));
}

}  // namespace serialV2
}  // namespace dingodb