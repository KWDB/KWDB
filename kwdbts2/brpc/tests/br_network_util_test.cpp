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

namespace kwdbts {

// Test fixture for network utility tests.
// No per-test setup or teardown is required.
class NetworkUtilTest : public ::testing::Test {};

// Verifies that TNetworkAddress supports setting hostname/port, copy
// construction, copy assignment, and swapping two instances.
TEST_F(NetworkUtilTest, TNetworkAddressBasicOperations) {
  // Construct an address and populate its fields.
  TNetworkAddress addr;
  addr.SetHostname("example.com");
  addr.SetPort(8080);

  // Verify that SetHostname() and SetPort() store values correctly.
  EXPECT_EQ(addr.hostname_, "example.com");
  EXPECT_EQ(addr.port_, 8080);

  // Copy constructor should produce an identical object.
  TNetworkAddress copy(addr);
  EXPECT_EQ(copy.hostname_, "example.com");
  EXPECT_EQ(copy.port_, 8080);

  // Copy assignment operator should produce an identical object.
  TNetworkAddress assigned;
  assigned = addr;
  EXPECT_EQ(assigned.hostname_, "example.com");
  EXPECT_EQ(assigned.port_, 8080);

  // swap() should exchange the fields of two TNetworkAddress instances.
  TNetworkAddress other;
  other.SetHostname("localhost");
  other.SetPort(9090);
  swap(addr, other);
  EXPECT_EQ(addr.hostname_, "localhost");
  EXPECT_EQ(addr.port_, 9090);
  EXPECT_EQ(other.hostname_, "example.com");
  EXPECT_EQ(other.port_, 8080);
}

// Verifies that operator== and operator!= correctly compare two
// TNetworkAddress instances based on hostname and port.
TEST_F(NetworkUtilTest, TNetworkAddressComparison) {
  TNetworkAddress addr1;
  addr1.SetHostname("example.com");
  addr1.SetPort(8080);

  // addr2 is identical to addr1.
  TNetworkAddress addr2;
  addr2.SetHostname("example.com");
  addr2.SetPort(8080);

  // addr3 differs from addr1 in both hostname and port.
  TNetworkAddress addr3;
  addr3.SetHostname("localhost");
  addr3.SetPort(9090);

  // Equal addresses should satisfy operator==.
  EXPECT_TRUE(addr1 == addr2);
  EXPECT_FALSE(addr1 == addr3);

  // Unequal addresses should satisfy operator!=.
  EXPECT_FALSE(addr1 != addr2);
  EXPECT_TRUE(addr1 != addr3);
}

// Verifies that the default constructor initializes hostname to an empty
// string and port to 0.
TEST_F(NetworkUtilTest, TNetworkAddressDefaultConstructor) {
  TNetworkAddress addr;

  EXPECT_TRUE(addr.hostname_.empty());
  EXPECT_EQ(addr.port_, 0);
}

// Tests hostname-to-IP resolution for IPv4, IPv6, and automatic selection,
// as well as error handling for an unresolvable hostname.
TEST_F(NetworkUtilTest, HostnameToIp) {
  std::string ip;

  // HostnameToIpv4() must resolve "localhost" to a valid IPv4 address.
  EXPECT_EQ(HostnameToIpv4("localhost", ip), KStatus::SUCCESS);
  EXPECT_TRUE(IsValidIp(ip));

  // HostnameToIpv6() resolution is best-effort; validate when available.
  if (HostnameToIpv6("localhost", ip) == KStatus::SUCCESS) {
    EXPECT_TRUE(IsValidIp(ip));
  }

  // HostnameToIp() selects an appropriate address family automatically.
  EXPECT_EQ(HostnameToIp("localhost", ip), KStatus::SUCCESS);
  EXPECT_TRUE(IsValidIp(ip));

  // An unresolvable hostname must return KStatus::FAIL.
  EXPECT_EQ(HostnameToIp("this-hostname-does-not-exist.invalid", ip), KStatus::FAIL);
}

// Verifies that IsValidIp() accepts well-formed IPv4/IPv6 addresses and
// rejects strings that are not valid IP addresses.
TEST_F(NetworkUtilTest, IsValidIp) {
  // Valid IPv4 addresses.
  EXPECT_TRUE(IsValidIp("127.0.0.1"));
  EXPECT_TRUE(IsValidIp("192.168.1.1"));

  // Valid IPv6 addresses.
  EXPECT_TRUE(IsValidIp("::1"));
  EXPECT_TRUE(IsValidIp("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));

  // Invalid: non-IP string.
  EXPECT_FALSE(IsValidIp("not.an.ip"));
  // Invalid: octets exceed 255.
  EXPECT_FALSE(IsValidIp("256.256.256.256"));
  // Invalid: IPv6 address with an extra group.
  EXPECT_FALSE(IsValidIp("2001:0db8:85a3:0000:0000:8a2e:0370:7334:extra"));
}

// Verifies that GetHostPort() formats host-port strings correctly.
// IPv6 addresses must be enclosed in brackets per RFC 2732.
TEST_F(NetworkUtilTest, GetHostPort) {
  // Hostname: no brackets needed.
  EXPECT_EQ(GetHostPort("localhost", 8080), "localhost:8080");
  // IPv6 address: must be wrapped in brackets.
  EXPECT_EQ(GetHostPort("::1", 8080), "[::1]:8080");
  // IPv4 address: no brackets needed.
  EXPECT_EQ(GetHostPort("192.168.1.1", 9090), "192.168.1.1:9090");
}

}  // namespace kwdbts
