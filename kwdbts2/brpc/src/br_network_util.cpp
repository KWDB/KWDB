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

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <climits>
#include <cstring>
#include <sstream>
#include <utility>

#ifdef __APPLE__
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX MAXHOSTNAMELEN
#endif
#endif

namespace kwdbts {

TNetworkAddress::~TNetworkAddress() noexcept {}

void TNetworkAddress::SetHostname(const std::string& val) { this->hostname_ = val; }

void TNetworkAddress::SetPort(const k_int32 val) { this->port_ = val; }

std::ostream& operator<<(std::ostream& out, const TNetworkAddress& obj) { return out; }

void swap(TNetworkAddress& a, TNetworkAddress& b) {
  using ::std::swap;
  swap(a.hostname_, b.hostname_);
  swap(a.port_, b.port_);
}

TNetworkAddress::TNetworkAddress(const TNetworkAddress& other) {
  hostname_ = other.hostname_;
  port_ = other.port_;
}

TNetworkAddress& TNetworkAddress::operator=(const TNetworkAddress& other) {
  hostname_ = other.hostname_;
  port_ = other.port_;
  return *this;
}

InetAddress::InetAddress(std::string ip, sa_family_t family, k_bool is_loopback)
    : ip_addr_(std::move(ip)), family_(family), is_loopback_(is_loopback) {}

k_bool InetAddress::is_loopback() const { return is_loopback_; }

std::string InetAddress::GetHostAddress() const { return ip_addr_; }

k_bool InetAddress::IsIpv6() const { return family_ == AF_INET6; }

KStatus GetHostname(std::string* hostname) {
  char name[HOST_NAME_MAX];
  k_int32 ret = gethostname(name, HOST_NAME_MAX);

  if (ret != 0) {
    return KStatus::FAIL;
  }

  *hostname = std::string(name);
  return KStatus::SUCCESS;
}

KStatus GetHosts(std::vector<InetAddress>* hosts) {
  ifaddrs* if_addrs = nullptr;
  if (getifaddrs(&if_addrs)) {
    return KStatus::FAIL;
  }

  std::vector<InetAddress> hosts_v4;
  std::vector<InetAddress> hosts_v6;
  for (ifaddrs* if_addr = if_addrs; if_addr != nullptr; if_addr = if_addr->ifa_next) {
    if (!if_addr->ifa_addr) {
      continue;
    }
    auto addr = if_addr->ifa_addr;
    if (addr->sa_family == AF_INET) {
      // Check legitimacy of IPv4 Addresses
      char addr_buf[INET_ADDRSTRLEN];
      struct sockaddr_in* sin_addr = reinterpret_cast<struct sockaddr_in*>(if_addr->ifa_addr);
      auto tmp_addr = &sin_addr->sin_addr;
      inet_ntop(AF_INET, tmp_addr, addr_buf, INET_ADDRSTRLEN);
      // Check is loopback Addresses
      struct sockaddr_in* sin_addr2 = reinterpret_cast<struct sockaddr_in*>(if_addr->ifa_addr);
      in_addr_t s_addr = sin_addr2->sin_addr.s_addr;
      k_bool is_loopback = (ntohl(s_addr) & 0xFF000000) == 0x7F000000;
      hosts_v4.emplace_back(std::string(addr_buf), AF_INET, is_loopback);
    } else if (addr->sa_family == AF_INET6) {
      // Check legitimacy of IPv6 Address
      struct sockaddr_in6* sin6_addr = reinterpret_cast<struct sockaddr_in6*>(if_addr->ifa_addr);
      auto tmp_addr = &sin6_addr->sin6_addr;
      char addr_buf[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6, tmp_addr, addr_buf, sizeof(addr_buf));
      // Check is loopback Addresses
      k_bool is_loopback = IN6_IS_ADDR_LOOPBACK(tmp_addr);
      std::string addr_str(addr_buf);
      std::transform(addr_str.begin(), addr_str.end(), addr_str.begin(), ::tolower);
      // Starts with "fe80"(Link local address), not supported.
      if (addr_str.rfind("fe80", 0) == 0) {
        continue;
      }
      hosts_v6.emplace_back(addr_str, AF_INET6, is_loopback);
    } else {
      continue;
    }
  }

  // Prefer ipv4 address by default for compatibility reason.
  hosts->insert(hosts->end(), hosts_v4.begin(), hosts_v4.end());
  hosts->insert(hosts->end(), hosts_v6.begin(), hosts_v6.end());

  if (if_addrs != nullptr) {
    freeifaddrs(if_addrs);
  }

  return KStatus::SUCCESS;
}

KStatus HostnameToIpv4(const std::string& host, std::string& ip) {
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

KStatus HostnameToIpv6(const std::string& host, std::string& ip) {
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

k_bool IsValidIp(const std::string& ip) {
  unsigned char buf[sizeof(struct in6_addr)];
  return (inet_pton(AF_INET6, ip.data(), buf) > 0) || (inet_pton(AF_INET, ip.data(), buf) > 0);
}

// Prefer ipv4 when both ipv4 and ipv6 bound to the same host
KStatus HostnameToIp(const std::string& host, std::string& ip) {
  KStatus status = HostnameToIpv4(host, ip);
  if (status == KStatus::SUCCESS) {
    return status;
  }
  status = HostnameToIpv6(host, ip);
  return status;
}

KStatus HostnameToIp(const std::string& host, std::string& ip, k_bool ipv6) {
  if (ipv6) {
    return HostnameToIpv6(host, ip);
  } else {
    return HostnameToIpv4(host, ip);
  }
}

TNetworkAddress MakeNetworkAddress(const std::string& hostname, k_int32 port) {
  TNetworkAddress ret;
  ret.SetHostname(hostname);
  ret.SetPort(port);
  return ret;
}

KStatus GetInetInterfaces(std::vector<std::string>* interfaces, k_bool include_ipv6) {
  ifaddrs* if_addrs = nullptr;
  if (getifaddrs(&if_addrs)) {
    return KStatus::FAIL;
  }

  for (ifaddrs* if_addr = if_addrs; if_addr != nullptr; if_addr = if_addr->ifa_next) {
    if (if_addr->ifa_addr == nullptr || if_addr->ifa_name == nullptr) {
      continue;
    }
    if (if_addr->ifa_addr->sa_family == AF_INET ||
        (include_ipv6 && if_addr->ifa_addr->sa_family == AF_INET6)) {
      interfaces->emplace_back(if_addr->ifa_name);
    }
  }
  if (if_addrs != nullptr) {
    freeifaddrs(if_addrs);
  }
  return KStatus::SUCCESS;
}

std::string GetHostPort(const std::string& host, k_int32 port) {
  std::stringstream ss;
  if (host.find(':') == std::string::npos) {
    ss << host << ":" << port;
  } else {
    ss << "[" << host << "]:" << port;
  }
  return ss.str();
}

}  // namespace kwdbts
