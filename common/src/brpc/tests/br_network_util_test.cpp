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

#include "br_network_util.h"

#include <gtest/gtest.h>
#include <sys/socket.h>

#include "cm_kwdb_context.h"
#include "settings.h"

namespace kwdbts {

class NetworkUtilTest : public ::testing::Test {
 protected:
  NetworkUtilTest() { InitServerKWDBContext(ctx_); }

  void SetUp() override { options_.brpc_addr = "127.0.0.1:27257"; }
  void TearDown() override {}
  kwdbContext_t g_pool_context;
  kwdbContext_p ctx_ = &g_pool_context;
  EngineOptions options_;
};

TEST_F(NetworkUtilTest, TNetworkAddressBasicOperations) {
  // Test basic operations
  TNetworkAddress addr;
  addr.SetHostname("example.com");
  addr.SetPort(8080);

  // Verify the setting operation
  EXPECT_EQ(addr.hostname_, "example.com");
  EXPECT_EQ(addr.port_, 8080);

  // Test the copy constructor
  TNetworkAddress copy(addr);
  EXPECT_EQ(copy.hostname_, "example.com");
  EXPECT_EQ(copy.port_, 8080);

  // Test the assignment operation
  TNetworkAddress assigned;
  assigned = addr;
  EXPECT_EQ(assigned.hostname_, "example.com");
  EXPECT_EQ(assigned.port_, 8080);

  // Test the swap operation
  TNetworkAddress other;
  other.SetHostname("localhost");
  other.SetPort(9090);
  swap(addr, other);
  EXPECT_EQ(addr.hostname_, "localhost");
  EXPECT_EQ(addr.port_, 9090);
  EXPECT_EQ(other.hostname_, "example.com");
  EXPECT_EQ(other.port_, 8080);
}

TEST_F(NetworkUtilTest, TNetworkAddressComparison) {
  // Test the comparison operation
  TNetworkAddress addr1;
  addr1.SetHostname("example.com");
  addr1.SetPort(8080);

  TNetworkAddress addr2;
  addr2.SetHostname("example.com");
  addr2.SetPort(8080);

  TNetworkAddress addr3;
  addr3.SetHostname("localhost");
  addr3.SetPort(9090);

  // Test the equal operator
  EXPECT_TRUE(addr1 == addr2);
  EXPECT_FALSE(addr1 == addr3);

  // Test the not equal operator
  EXPECT_FALSE(addr1 != addr2);
  EXPECT_TRUE(addr1 != addr3);
}

TEST_F(NetworkUtilTest, TNetworkAddressDefaultConstructor) {
  // Test the default constructor
  TNetworkAddress addr;

  EXPECT_TRUE(addr.hostname_.empty());
  EXPECT_EQ(addr.port_, 0);
}

// HostnameToIp tests
TEST_F(NetworkUtilTest, HostnameToIp) {
  std::string ip;

  // Test IPv4 parsing
  EXPECT_EQ(HostnameToIpv4("localhost", ip), KStatus::SUCCESS);
  EXPECT_TRUE(IsValidIp(ip));

  // Test IPv6 parsing
  if (HostnameToIpv6("localhost", ip) == KStatus::SUCCESS) {
    EXPECT_TRUE(IsValidIp(ip));
  }

  // Test automatic selection
  EXPECT_EQ(HostnameToIp("localhost", ip), KStatus::SUCCESS);
  EXPECT_TRUE(IsValidIp(ip));

  // Test invalid hostname
  EXPECT_EQ(HostnameToIp("invalid.hostname.xyz", ip), KStatus::FAIL);
}

// IsValidIp tests
TEST_F(NetworkUtilTest, IsValidIp) {
  EXPECT_TRUE(IsValidIp("127.0.0.1"));
  EXPECT_TRUE(IsValidIp("::1"));
  EXPECT_TRUE(IsValidIp("192.168.1.1"));
  EXPECT_TRUE(IsValidIp("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));

  EXPECT_FALSE(IsValidIp("not.an.ip"));
  EXPECT_FALSE(IsValidIp("256.256.256.256"));
  EXPECT_FALSE(IsValidIp("2001:0db8:85a3:0000:0000:8a2e:0370:7334:extra"));
}

// GetHostPort tests
TEST_F(NetworkUtilTest, GetHostPort) {
  EXPECT_EQ(GetHostPort("localhost", 8080), "localhost:8080");
  EXPECT_EQ(GetHostPort("::1", 8080), "[::1]:8080");
  EXPECT_EQ(GetHostPort("192.168.1.1", 9090), "192.168.1.1:9090");
}

}  // namespace kwdbts
