// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package protoreflect

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestNewMessage tests the NewMessage function.
func TestNewMessage(t *testing.T) {
	// Test with valid message type
	t.Run("valid message", func(t *testing.T) {
		msg, err := NewMessage("kwbase.roachpb.Attributes")
		require.NoError(t, err)
		require.NotNil(t, msg)
		_, ok := msg.(*roachpb.Attributes)
		require.True(t, ok)
	})

	// Test with invalid message type
	t.Run("invalid message", func(t *testing.T) {
		msg, err := NewMessage("invalid.Message")
		require.Error(t, err)
		require.Nil(t, msg)
	})
}

// TestDecodeMessage tests the DecodeMessage function.
func TestDecodeMessage(t *testing.T) {
	// Test decoding with valid message
	t.Run("valid message", func(t *testing.T) {
		attr := &roachpb.Attributes{Attrs: []string{"ssd", "fast"}}
		data, err := protoutil.Marshal(attr)
		require.NoError(t, err)

		decodedMsg, err := DecodeMessage("kwbase.roachpb.Attributes", data)
		require.NoError(t, err)
		require.NotNil(t, decodedMsg)

		decodedAttr, ok := decodedMsg.(*roachpb.Attributes)
		require.True(t, ok)
		require.Equal(t, attr.Attrs, decodedAttr.Attrs)
	})

	// Test decoding with invalid message type
	t.Run("invalid message type", func(t *testing.T) {
		decodedMsg, err := DecodeMessage("invalid.Message", []byte("invalid data"))
		require.Error(t, err)
		require.Nil(t, decodedMsg)
	})
}

// TestMessageToJSON tests the MessageToJSON function.
func TestMessageToJSON(t *testing.T) {
	// Test with a valid message
	t.Run("valid message", func(t *testing.T) {
		attr := &roachpb.Attributes{Attrs: []string{"ssd", "fast"}}
		jsonObj, err := MessageToJSON(attr, true)
		require.NoError(t, err)
		require.NotNil(t, jsonObj)
		require.Contains(t, jsonObj.String(), "ssd")
		require.Contains(t, jsonObj.String(), "fast")
	})

	// Test with a mock message that fails
	t.Run("mock message", func(t *testing.T) {
		decodedMsg, _ := DecodeMessage("invalid.Message", []byte("invalid data"))
		jsonObj, err := MessageToJSON(decodedMsg, true)
		// This should fail because mockMessage doesn't implement the full protoutil.Message interface
		require.Error(t, err)
		require.Nil(t, jsonObj)
	})
}

func TestJSONBMarshalToMessage(t *testing.T) {
	attr := &roachpb.Attributes{Attrs: []string{"ssd", "fast"}}
	jsonObj, err := MessageToJSON(attr, true)
	require.NoError(t, err)

	// Unmarshal back
	newAttr := &roachpb.Attributes{}
	data, err := JSONBMarshalToMessage(jsonObj, newAttr)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, attr.Attrs, newAttr.Attrs)

	// Test with invalid JSON
	invalidJSON, err := json.ParseJSON(`{"invalid": "format"}`)
	require.NoError(t, err)
	_, err = JSONBMarshalToMessage(invalidJSON, newAttr)
	require.Error(t, err)
}
