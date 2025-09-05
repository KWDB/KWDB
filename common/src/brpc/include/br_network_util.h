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

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <climits>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "kwdb_type.h"

namespace kwdbts {

class TNetworkAddress {
 public:
  TNetworkAddress(const TNetworkAddress& other) {
    hostname_ = other.hostname_;
    port_ = other.port_;
  }

  TNetworkAddress& operator=(const TNetworkAddress& other) {
    hostname_ = other.hostname_;
    port_ = other.port_;
    return *this;
  }

  TNetworkAddress() noexcept : hostname_(), port_(0) {}

  virtual ~TNetworkAddress() noexcept {}

  std::string hostname_;
  k_int32 port_;

  void SetHostname(const std::string& val) { this->hostname_ = val; }
  void SetPort(const k_int32 val) { this->port_ = val; }

  k_bool operator==(const TNetworkAddress& rhs) const {
    if (!(hostname_ == rhs.hostname_)) return false;
    if (!(port_ == rhs.port_)) return false;
    return true;
  }

  k_bool operator!=(const TNetworkAddress& rhs) const { return !(*this == rhs); }

  k_bool operator<(const TNetworkAddress&) const;
};

inline void swap(TNetworkAddress& a, TNetworkAddress& b) {
  using ::std::swap;
  swap(a.hostname_, b.hostname_);
  swap(a.port_, b.port_);
}

inline k_bool IsValidIp(const std::string& ip) {
  unsigned char buf[sizeof(struct in6_addr)];
  return (inet_pton(AF_INET6, ip.data(), buf) > 0) || (inet_pton(AF_INET, ip.data(), buf) > 0);
}

inline std::string GetHostPort(const std::string& host, k_int32 port) {
  std::stringstream ss;
  if (host.find(':') == std::string::npos) {
    ss << host << ":" << port;
  } else {
    ss << "[" << host << "]:" << port;
  }
  return ss.str();
}

inline KStatus HostnameToIpv6(const std::string& host, std::string& ip) {
  char ipv6_str[128];
  struct sockaddr_in6* sockaddr_ipv6;

  struct addrinfo *answer, hint;
  bzero(&hint, sizeof(hint));
  hint.ai_family = AF_INET6;
  hint.ai_socktype = SOCK_STREAM;

  k_int32 err = getaddrinfo(host.c_str(), nullptr, &hint, &answer);
  if (err != 0) {
    return KStatus::FAIL;
  }

  sockaddr_ipv6 = reinterpret_cast<struct sockaddr_in6*>(answer->ai_addr);
  inet_ntop(AF_INET6, &sockaddr_ipv6->sin6_addr, ipv6_str, sizeof(ipv6_str));
  ip = ipv6_str;
  fflush(nullptr);
  freeaddrinfo(answer);
  return KStatus::SUCCESS;
}

inline KStatus HostnameToIpv4(const std::string& host, std::string& ip) {
  addrinfo hints, *res;
  in_addr addr;

  memset(&hints, 0, sizeof(addrinfo));
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_family = AF_INET;
  k_int32 err = getaddrinfo(host.c_str(), nullptr, &hints, &res);
  if (err != 0) {
    return KStatus::FAIL;
  }

  addr.s_addr = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr.s_addr;
  ip = inet_ntoa(addr);

  freeaddrinfo(res);
  return KStatus::SUCCESS;
}

inline KStatus HostnameToIp(const std::string& host, std::string& ip) {
  KStatus status = HostnameToIpv4(host, ip);
  if (status == KStatus::SUCCESS) {
    return status;
  }
  status = HostnameToIpv6(host, ip);
  return status;
}

}  // namespace kwdbts
