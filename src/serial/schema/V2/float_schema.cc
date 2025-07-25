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

#include "float_schema.h"

#include <cstdint>
#include <cstring>
#include <stdexcept>

#include "serial/utils/V2/compiler.h"

namespace dingodb {
namespace serialV2 {

constexpr int kDataLength = 4;
constexpr int kDataLengthWithNull = kDataLength + 1;

void DingoSchema<float>::EncodeFloatComparable(float data, Buf& buf) {
  uint32_t bits;
  memcpy(&bits, &data, 4);
  if (DINGO_LIKELY(IsLe() && data >= 0)) {
    buf.Write(bits >> 24 ^ 0x80);
    buf.Write(bits >> 16);
    buf.Write(bits >> 8);
    buf.Write(bits);
  } else if (IsLe() && data < 0) {
    buf.Write(~bits >> 24);
    buf.Write(~bits >> 16);
    buf.Write(~bits >> 8);
    buf.Write(~bits);
  } else if (!IsLe() && data >= 0) {
    buf.Write(bits ^ 0x80);
    buf.Write(bits >> 8);
    buf.Write(bits >> 16);
    buf.Write(bits >> 24);
  } else {
    buf.Write(~bits);
    buf.Write(~bits >> 8);
    buf.Write(~bits >> 16);
    buf.Write(~bits >> 24);
  }
}

float DingoSchema<float>::DecodeFloatComparable(Buf& buf) {
  uint32_t in = buf.Read() & 0xFF;
  if (DINGO_LIKELY(IsLe())) {
    if (in >= 0x80) {
      in = in ^ 0x80;
      for (int i = 0; i < 3; i++) {
        in <<= 8;
        in |= buf.Read() & 0xFF;
      }
    } else {
      in = ~in;
      for (int i = 0; i < 3; i++) {
        in <<= 8;
        in |= ~buf.Read() & 0xFF;
      }
    }
  } else {
    if (in >= 0x80) {
      in = in ^ 0x80;
      for (int i = 1; i < 4; i++) {
        in |= (((uint32_t)buf.Read() & 0xFF) << (8 * i));
      }
    } else {
      for (int i = 1; i < 4; i++) {
        in |= (((uint32_t)buf.Read() & 0xFF) << (8 * i));
      }
      in = ~in;
    }
  }

  void* v = &in;
  return *reinterpret_cast<float*>(v);
}

void DingoSchema<float>::EncodeFloatNotComparable(float data, Buf& buf) {
  uint32_t bits;
  memcpy(&bits, &data, 4);
  if (IsLe()) {
    buf.Write(bits >> 24);
    buf.Write(bits >> 16);
    buf.Write(bits >> 8);
    buf.Write(bits);
  } else {
    buf.Write(bits);
    buf.Write(bits >> 8);
    buf.Write(bits >> 16);
    buf.Write(bits >> 24);
  }
}
float DingoSchema<float>::DecodeFloatNotComparable(Buf& buf) {
  uint32_t in = buf.Read() & 0xFF;

  if (DINGO_LIKELY(IsLe())) {
    for (int i = 0; i < 3; i++) {
      in <<= 8;
      in |= buf.Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 4; i++) {
      in |= (((uint32_t)buf.Read() & 0xFF) << (8 * i));
    }
  }

  void* v = &in;
  return *reinterpret_cast<float*>(v);
}

float DingoSchema<float>::DecodeFloatNotComparable(Buf& buf, int offset) {
  uint32_t in = buf.Read(offset++) & 0xFF;

  if (DINGO_LIKELY(IsLe())) {
    for (int i = 0; i < 3; i++) {
      in <<= 8;
      in |= buf.Read(offset++) & 0xFF;
    }
  } else {
    for (int i = 1; i < 4; i++) {
      in |= (((uint32_t)buf.Read(offset++) & 0xFF) << (8 * i));
    }
  }

  void* v = &in;
  return *reinterpret_cast<float*>(v);
}

int DingoSchema<float>::GetLengthForKey() {
  if(AllowNull()) {
    return kDataLengthWithNull;
  }
  else {
    return kDataLength;
  }
}

int DingoSchema<float>::GetLengthForValue() { return kDataLength; }

int DingoSchema<float>::SkipKey(Buf& buf) {
  int len = GetLengthForKey();
  buf.Skip(len);
  return len;
}

int DingoSchema<float>::SkipValue(Buf& buf) {
  buf.Skip(kDataLength);
  return kDataLength;
}

int DingoSchema<float>::EncodeKey(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if(AllowNull()) {
    if (data.has_value()) {
      buf.Write(k_not_null);
      const auto& ref_data = std::any_cast<const float&>(data);
      EncodeFloatComparable(ref_data, buf);
    } else {
      buf.Write(k_null);
      buf.WriteInt(0);
    }
    return kDataLengthWithNull;
  }
  else {
    const auto& ref_data = std::any_cast<const float&>(data);
    EncodeFloatComparable(ref_data, buf);
    return kDataLength;
  }
}

void DingoSchema<float>::EncodeKeyPrefix(const std::any& data, Buf& buf) {
  EncodeKey(data, buf);
}

// {value: 4byte}
int DingoSchema<float>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    const auto& ref_data = std::any_cast<const float&>(data);
    EncodeFloatNotComparable(ref_data, buf);
    return kDataLength;
  }

  return 0;
}

std::any DingoSchema<float>::DecodeKey(Buf& buf) {
  if(AllowNull()) {
    if (buf.Read() == k_null) {
      buf.Skip(kDataLength);
      return std::any();
    }
  }

  return std::move(std::any(DecodeFloatComparable(buf)));
}

inline std::any DingoSchema<float>::DecodeValue(Buf& buf) {
  return std::move(std::any(DecodeFloatNotComparable(buf)));
}

inline std::any DingoSchema<float>::DecodeValue(Buf& buf, int offset) {
  return std::move(std::any(DecodeFloatNotComparable(buf, offset)));
}

}  // namespace V2
}  // namespace dingodb