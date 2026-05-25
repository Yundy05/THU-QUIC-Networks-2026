# mini RFC

## Overview
This repository hosts a series of mini RFCs. Each mini RFC extracts paragraphs from the four QUIC-related IETF RFCs and reorganize them to give a simplified and annotated summary of one certain feature of QUIC:

* [RFC 8999: Version-Independent Properties of QUIC](https://datatracker.ietf.org/doc/html/rfc8999)
* [RFC 9000: QUIC: A UDP-Based Multiplexed and Secure Transport](https://datatracker.ietf.org/doc/html/rfc9000)
* [RFC 9001: Using TLS to Secure QUIC](https://datatracker.ietf.org/doc/html/rfc9001)
* [RFC 9002: QUIC Loss Detection and Congestion Control](https://datatracker.ietf.org/doc/html/rfc9002)

This repository is part of a course project at Tsinghua University and therefore not suitable for any other purposes. It may not correctly reflect the original thoughts and specifications in above RFCs.

## Important Note
Students taking this course SHOULD NOT distribute any document in this repository. Modification of extracts from IETF RFCs is not granted by IETF as original authors retain their copyrights to RFCs. This document is for educational purposes and for private usage only.

## Indexes

### Essential Part (`essential` subdirectory)

* [mini RFC: The QUIC Transport Protocol](essential/01-QUIC_Transmission.pdf)
* [mini RFC: Reliable Transmission](essential/02-Reliability.pdf)
* [mini RFC: Congestion Control](essential/03-Congestion_Control.pdf)

### Optional Part (`optional` subdirectory)
#### Connection Establishment
* [mini RFC: Transport Parameter Exchange](optional/11-Transport_Parameter.pdf)
* [mini RFC: Address Validation](optional/12-Address_Validation.pdf)
* [mini RFC: Connection Multiplexing](optional/13-Connection_Multiplexing.pdf)
#### Connection Termination
* [mini RFC: Connection Termination](optional/21-Connection_Termination.pdf)

#### Connection Migration
* [mini RFC: Connection ID](optional/31-Connection_ID.pdf)
* [mini RFC: Connection Migration](optional/32-Connection_Migration.pdf)
* [mini RFC: Path Validation](optional/33-Path_Validation.pdf)

#### Basic Transmission
* [mini RFC: Advanced Transmission](optional/41-Advanced_Transmission.pdf)

#### Reliable Transmission
* [mini RFC: Probe Timeout](optional/51-Probe_Timeout.pdf)

#### Flow Control
* [mini RFC: Flow Control](optional/61-Flow_Control.pdf)

#### Congestion Control
* [RFC 8312: CUBIC for Fast Long-Distance Networks](https://datatracker.ietf.org/doc/html/rfc8312) (CUBIC is described in a self-contained RFC. It's short and clear, thus not necessary to be rewritten.)
* [mini RFC: Pacing & Persistent Congestion](optional/72-Pacing.pdf)
* [mini RFC: ECN](optional/73-ECN.pdf)

#### Error Handling
* [mini RFC: Error Handling](optional/81-Error_Handling.pdf)

#### Security
* [mini RFC: QUIC Handshake](optional/91-QUIC_Handshake.pdf)
* [mini RFC: Packet Protection](optional/92-Packet_Protection.pdf)
* [mini RFC: 0-RTT Transmission](optional/93-0-RTT.pdf)

