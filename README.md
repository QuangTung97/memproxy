[![memproxy](https://github.com/QuangTung97/memproxy/actions/workflows/go.yml/badge.svg)](https://github.com/QuangTung97/memproxy/actions/workflows/go.yml)
[![Coverage Status](https://coveralls.io/repos/github/QuangTung97/memproxy/badge.svg?branch=master)](https://coveralls.io/github/QuangTung97/memproxy?branch=master)

# Golang Memcache Proxy Library

## Why this library?

This library helps to utilize memcached in a consistent and efficient way.

**Supporting features**:

* Deal with Consistency between Memcached and Database using the Lease Mechanism.
* Prevent thundering herd (a.k.a Cache Stampede).
* Efficient batching get to the underlining database, batching between lease gets
  and between retries for preventing thundering-herd.
* Memcached replication similar to MCRouter, without the need for external proxies.
* Memory-weighted load-balancing for replication.

## Table of Contents

1. [Usage](#usage)
2. [Consistency between Memcached and Database](docs/consistency.md)
3. [Preventing Thundering Herd](docs/thundering-herd.md)

## Usage
