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

#ifndef DINGO_SERIAL_RECORD_ENCODER_H_
#define DINGO_SERIAL_RECORD_ENCODER_H_

#include <memory>
#include <string>

#include "any"
#include "functional"  // IWYU pragma: keep
#include "optional"    // IWYU pragma: keep
#include "serial/schema/boolean_list_schema.h"
#include "serial/schema/boolean_schema.h"  // IWYU pragma: keep
#include "serial/schema/double_list_schema.h"
#include "serial/schema/double_schema.h"  // IWYU pragma: keep
#include "serial/schema/float_list_schema.h"
#include "serial/schema/float_schema.h"  // IWYU pragma: keep
#include "serial/schema/integer_list_schema.h"
#include "serial/schema/integer_schema.h"  // IWYU pragma: keep
#include "serial/schema/long_list_schema.h"
#include "serial/schema/long_schema.h"  // IWYU pragma: keep
#include "serial/schema/string_list_schema.h"
#include "serial/schema/string_schema.h"  // IWYU pragma: keep
#include "serial/utils/keyvalue.h"        // IWYU pragma: keep
#include "serial/utils/utils.h"           // IWYU pragma: keep

namespace dingodb {

class RecordEncoderV1 {
 private:
  void EncodePrefix(Buf& buf, char prefix) const;
  void EncodeReverseTag(Buf& buf) const;
  void EncodeSchemaVersion(Buf& buf) const;

  uint8_t codec_version_ = 1;
  int schema_version_;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas_;
  long common_id_;
  int key_buf_size_;
  int value_buf_size_;
  bool le_;

 public:
  RecordEncoderV1(
      int schema_version,
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
      long common_id);
  RecordEncoderV1(
      int schema_version,
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
      long common_id, bool le);

  void Init(int schema_version,
            std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
            long common_id);

  int Encode(char prefix, const std::vector<std::any>& record, std::string& key,
             std::string& value);

  int EncodeKey(char prefix, const std::vector<std::any>& record,
                std::string& output);

  int EncodeValue(const std::vector<std::any>& record, std::string& output);

  int EncodeKeyPrefix(char prefix, const std::vector<std::any>& record,
                      int column_count, std::string& output);
  int EncodeKeyPrefix(char prefix, const std::vector<std::string>& keys,
                      std::string& output);

  int EncodeMaxKeyPrefix(char prefix, std::string& output) const;

  int EncodeMinKeyPrefix(char prefix, std::string& output) const;
};

}  // namespace dingodb

#endif
