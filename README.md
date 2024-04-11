# PrIMe (Process Internal Metrics)

> A tool to compute longitudinal software metrics

## Table of Contents

- [PrIMe (Process Internal Metrics)](#prime-process-internal-metrics)
  - [Table of Contents](#table-of-contents)
  - [About](#about)
  - [Documentation](#documentation)
  - [How to Install](#how-to-install)
    - [Dependencies](#dependencies)
      - [Build Tools](#build-tools)
      - [Core Dependencies](#core-dependencies)
    - [Installation steps](#installation-steps)
  - [How to Run](#how-to-run)
  - [References](#references)
  - [Cite PrIMe](#cite-prime)

## About

PrIMe (Process Internal Metrics) is a software tool to facilitate the collection
and computation of direct and derived software process metrics longitudinally
through time. Similar tools to PrIMe include SonarQube and SmartSHARK.

PrIMe is different from these tools via:

- xxx
- yyy
- zzz

PrIMe is based off of earlier academic work \[TODO CITE Snapshot Metrics
paper\]. The metrics that are implemented within PrIMe are derived from academic
works. These citations can be found in the [References](#references) section of
this document.

While PrIMe is able to compute derived metrics, many direct software metrics
need to be collected using supported third-party tools. These tools are listed
within both the [Dependencies](#dependencies) and [References](#references)
sections of this document. However, it should be noted that not all third-party
tools are required to use PrIMe. Tools that are recommended are listed as such,
however, alternatives are supported.

## Documentation

Tool and API documentation is availible at xxx.

This documentation also has instructions on how to develop support for other
version control systems, issue trackers, metrics, and CLOC-like tooling.

## How to Install

PrIMe is tested to work on x86-64 Linux systems with Python 3.10. It should be
noted that certain components of PrIMe are compatible with different
environments as they rely soley on Python 3.10. However, testing is not done to
validate this.

1. `git clone https://github.com/NicholasSynovic/prime-vx`
1. `cd prim-vx`
1. `make`

### Dependencies

PrIMe is dependent upon the following:

#### Build Tools

- `Python 3.10`
- `poetry`

#### Core Dependencies

- `pandas`
- `progress`
- `sqlalchemy`
- `typedframe`
- [`pyfs`](https://github.com/NicholasSynovic/python-fs-utils)

## How to Run

## References

ADD Citations

## Cite PrIMe
