# Flight

A Go Implementation of Apache Arrow Flight SQL

![Apache Arrow Flight Logo](flight.webp)

## Under Construction (does not work)

## Overview

Flight is a Go implementation of the Apache Arrow Flight SQL protocol. This solution leverages the capabilities of Apache Arrow and gRPC to provide high-performance, scalable data transport. It is designed to facilitate efficient data interchange and manipulation for analytics, machine learning, and other data-intensive applications.

## Key Technologies

### Apache Arrow

[Apache Arrow](https://arrow.apache.org/) is a cross-language development platform for in-memory data. It specifies a standardized language-independent columnar memory format for flat and hierarchical data, organized for efficient analytic operations on modern hardware like CPUs and GPUs. Arrow enables zero-copy reads for lightning-fast data access without serialization overhead.

#### Key Features

- **Columnar Format**: Optimized for analytical processing.
- **Interoperability**: Supports multiple programming languages including Go, Python, Java, C++, and more.
- **Efficient**: Enables zero-copy reads to avoid serialization overhead.
- **High Performance**: Designed for modern CPUs and GPUs to accelerate data processing tasks.

### Apache Arrow Flight

[Apache Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) is a framework for high-performance data services based on Arrow. It is designed for efficient data transport, leveraging the Arrow in-memory format for seamless data interchange between systems.

### gRPC

[gRPC](https://grpc.io/) is a high-performance, open-source RPC framework developed by Google. It uses HTTP/2 for transport, Protocol Buffers for serialization, and provides features like authentication, load balancing, and more.

## Implementation Details

### Flight Server

The Flight server is implemented using gRPC and Arrow Flight. It acts as a data service, handling incoming requests for data operations such as SQL query execution and data retrieval.

#### Key Components

- **gRPC Server**: The core of the server, handling incoming RPC calls.
- **Flight Service**: Implements the Flight service interface, managing data operations.
- **SQL Execution**: Integrates with PostgreSQL to execute SQL queries and return results in Arrow format.

### Flight Client

The Flight client interacts with the Flight server to perform data operations. It uses gRPC to communicate with the server and Arrow Flight to handle data interchange.
