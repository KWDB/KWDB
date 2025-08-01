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

#pragma once

#include <sys/un.h>

#include <string>
#include <vector>

#include "kwdb_type.h"

namespace kwdbts {

class TNetworkAddress {
 public:
  TNetworkAddress(const TNetworkAddress&);
  TNetworkAddress& operator=(const TNetworkAddress&);
  TNetworkAddress() noexcept : hostname_(), port_(0) {}

  virtual ~TNetworkAddress() noexcept;
  std::string hostname_;
  k_int32 port_;

  void SetHostname(const std::string& val);
  void SetPort(const k_int32 val);

  k_bool operator==(const TNetworkAddress& rhs) const {
    if (!(hostname_ == rhs.hostname_)) return false;
    if (!(port_ == rhs.port_)) return false;
    return true;
  }

  k_bool operator!=(const TNetworkAddress& rhs) const { return !(*this == rhs); }

  k_bool operator<(const TNetworkAddress&) const;
};

class InetAddress {
 public:
  InetAddress(std::string ip, sa_family_t family, k_bool is_loopback);
  k_bool is_loopback() const;
  std::string GetHostAddress() const;
  k_bool IsIpv6() const;

 private:
  std::string ip_addr_;
  sa_family_t family_;
  k_bool is_loopback_;
};

// Looks up all IP addresses associated with a given hostname. Returns
// an error status if any system call failed, otherwise OK. Even if OK
// is returned, addresses may still be of zero length.
KStatus HostnameToIp(const std::string& host, std::string& ip);
KStatus HostnameToIp(const std::string& host, std::string& ip, k_bool ipv6);
KStatus HostnameToIpv4(const std::string& host, std::string& ip);
KStatus HostnameToIpv6(const std::string& host, std::string& ip);

k_bool IsValidIp(const std::string& ip);

// Sets the output argument to the system defined hostname.
// Returns OK if a hostname can be found, false otherwise.
KStatus GetHostname(std::string* hostname);

KStatus GetHosts(std::vector<InetAddress>* hosts);

// Utility method because Thrift does not supply useful constructors
TNetworkAddress MakeNetworkAddress(const std::string& hostname, k_int32 port);

KStatus GetInetInterfaces(std::vector<std::string>* interfaces, k_bool include_ipv6 = false);

std::string GetHostPort(const std::string& host, k_int32 port);

}  // namespace kwdbts
