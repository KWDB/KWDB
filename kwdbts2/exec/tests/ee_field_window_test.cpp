// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "ee_field_window.h"

#include "ee_field_const.h"
#include "ee_global.h"
#include "gtest/gtest.h"

namespace kwdbts {

// Mock Field class for testing
class MockField : public Field {
public:
  MockField(roachpb::DataType type, k_int64 value, bool is_null = false)
      : Field(-1, type, sizeof(k_int64), FIELD_CONSTANT),
        value_(value),
        is_null_(is_null) {
    set_allow_null(true);  // Allow null values
  }

  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override { return nullptr; }
  k_bool is_nullable() override { return true; }  // Allow null values
  bool CheckNull() { return is_null_; }  // Directly return is_null_
  k_int64 ValInt() override { return value_; }
  k_int64 ValInt(k_char *ptr) override { return value_; }
  k_double64 ValReal() override { return static_cast<k_double64>(value_); }
  k_double64 ValReal(k_char *ptr) override { return static_cast<k_double64>(value_); }
  String ValStr() override {
    String str(10);
    snprintf(str.ptr_, 10, "%ld", value_);
    str.length_ = strlen(str.ptr_);
    return str;
  }
  String ValStr(k_char *ptr) override { return ValStr(); }
  Field *field_to_copy() override { return nullptr; }
  k_bool fill_template_field(char *ptr) override { return false; }
  // roachpb::DataType get_storage_type() override { return data_type_; }

  k_int64 value_;
  bool is_null_;
};

class TestFieldWindow : public testing::Test {
protected:
  static void SetUpTestCase() {
  }

  static void TearDownTestCase() {
  }
};

// Test FieldFuncStateWindow
TEST_F(TestFieldWindow, TestFieldFuncStateWindow) {
  // Test with INT type
  MockField *mock_field = new MockField(roachpb::DataType::INT, 100, false);
  FieldFuncStateWindow *state_window = new FieldFuncStateWindow(mock_field);

  // Test is_nullable
  EXPECT_TRUE(state_window->is_nullable());

  // Test ValInt with first call
  k_int64 result = state_window->ValInt();
  EXPECT_EQ(result, 0);

  // Test ValInt with same value
  result = state_window->ValInt();
  EXPECT_EQ(result, 0);

  // Test ValInt with different value
  mock_field->value_ = 200;
  result = state_window->ValInt();
  EXPECT_EQ(result, 0);

  // Test with null value
  mock_field->is_null_ = true;
  EXPECT_TRUE(state_window->is_nullable());
  result = state_window->ValInt();
  EXPECT_EQ(result, 0);

  // Test with STRING type
  SafeDeletePointer(state_window);
  SafeDeletePointer(mock_field);
  mock_field = new MockField(roachpb::DataType::VARCHAR, 100, false);
  state_window = new FieldFuncStateWindow(mock_field);

  // Test ValInt with string
  result = state_window->ValInt();
  EXPECT_EQ(result, 0);

  SafeDeletePointer(state_window);
  SafeDeletePointer(mock_field);
}

// Test FieldFuncEventWindow
TEST_F(TestFieldWindow, TestFieldFuncEventWindow) {
  MockField *mock_field1 = new MockField(roachpb::DataType::INT, 1, false);  // begin event
  MockField *mock_field2 = new MockField(roachpb::DataType::INT, 0, false);  // end event
  FieldFuncEventWindow *event_window = new FieldFuncEventWindow(mock_field1, mock_field2);

  // Test is_nullable when begin is false
  EXPECT_TRUE(event_window->is_nullable());

  // Test ValInt with begin event
  k_int64 result = event_window->ValInt();
  EXPECT_EQ(result, 1);

  // Test ValInt with no event
  result = event_window->ValInt();
  EXPECT_EQ(result, 1);

  // Test ValInt with end event
  mock_field2->value_ = 1;
  result = event_window->ValInt();
  EXPECT_EQ(result, 1);

  // Test is_nullable with null begin
  mock_field1->is_null_ = true;
  EXPECT_TRUE(event_window->is_nullable());

  // Test is_nullable with null end
  mock_field1->is_null_ = false;
  mock_field2->is_null_ = true;
  EXPECT_TRUE(event_window->is_nullable());

  // Test is_nullable with non-positive begin
  mock_field2->is_null_ = false;
  mock_field1->value_ = 0;
  EXPECT_TRUE(event_window->is_nullable());

  // Test IsBegin
  mock_field1->value_ = 1;
  EXPECT_FALSE(event_window->IsBegin());
  mock_field1->is_null_ = true;
  EXPECT_FALSE(event_window->IsBegin());

  // Test IsEnd
  mock_field1->is_null_ = false;
  mock_field2->value_ = 1;
  EXPECT_FALSE(event_window->IsEnd());
  mock_field2->is_null_ = true;
  EXPECT_FALSE(event_window->IsEnd());

  SafeDeletePointer(event_window);
  SafeDeletePointer(mock_field1);
  SafeDeletePointer(mock_field2);
}

// Test FieldFuncSessionWindow
TEST_F(TestFieldWindow, TestFieldFuncSessionWindow) {
  MockField *mock_field1 = new MockField(roachpb::DataType::TIMESTAMP, 1000, false);  // timestamp
  MockField *mock_field2 = new MockField(roachpb::DataType::VARCHAR, 0, false);  // tolerance
  FieldFuncSessionWindow *session_window = new FieldFuncSessionWindow(mock_field1, mock_field2);

  // Test ResolveTolVal (would need proper string setup)
  // For simplicity, we'll test ValInt directly
  // session_window->tol_val_ = 500;  // 500ms tolerance
  // session_window->val_ = 1000;
  // session_window->group_id_ = 1;

  // Test ValInt with same session
  k_int64 result = session_window->ValInt();
  EXPECT_EQ(result, 1);

  // Test ValInt with new session
  mock_field1->value_ = 2000;  // More than 500ms difference
  result = session_window->ValInt();
  EXPECT_EQ(result, 2);

  SafeDeletePointer(session_window);
  SafeDeletePointer(mock_field1);
  SafeDeletePointer(mock_field2);
}

// Test FieldFuncCountWindow
TEST_F(TestFieldWindow, TestFieldFuncCountWindow) {
  MockField *mock_field = new MockField(roachpb::DataType::INT, 5, false);  // count value
  std::list<Field *> fields = {mock_field};
  FieldFuncCountWindow *count_window = new FieldFuncCountWindow(fields);

  // Test ValInt with first call
  k_int64 result = count_window->ValInt();
  EXPECT_EQ(result, 1);

  // Test ValInt with subsequent calls
  result = count_window->ValInt();
  EXPECT_EQ(result, 1);
  result = count_window->ValInt();
  EXPECT_EQ(result, 1);
  result = count_window->ValInt();
  EXPECT_EQ(result, 1);
  result = count_window->ValInt();  // 5th call
  EXPECT_EQ(result, 1);
  result = count_window->ValInt();  // 6th call (reset)
  EXPECT_EQ(result, 2);

  SafeDeletePointer(count_window);
  SafeDeletePointer(mock_field);
}

// Test FieldFuncTimeWindow
TEST_F(TestFieldWindow, TestFieldFuncTimeWindow) {
  MockField *mock_field1 = new MockField(roachpb::DataType::TIMESTAMP, 1000, false);  // timestamp
  MockField *mock_field2 = new MockField(roachpb::DataType::VARCHAR, 0, false);  // interval
  std::list<Field *> fields = {mock_field1, mock_field2};
  FieldFuncTimeWindow *time_window = new FieldFuncTimeWindow(fields, 0);  // time zone 0

  // Test ResolveParams (would need proper string setup)
  // For simplicity, we'll test ValInt directly
  k_int64 result = time_window->ValInt();
  EXPECT_EQ(result, 0);

  SafeDeletePointer(time_window);
  SafeDeletePointer(mock_field1);
  SafeDeletePointer(mock_field2);
}

}  // namespace kwdbts
