---
icon: lucide/flask-conical
---

# Lab II-31: Connection ID (Optional)

## Overview

You need to add Connection ID support to your implementation in this lab. The connection ID is a network-layer-agnostic identifier for a QUIC connection. Such an identifier is a prerequisite for many features such as connection migration, connection multiplexing, and stateless reset.
You have to
- generate a random connection ID and negotiate with peers during the handshake.
- properly generate and handle NEW_CONNECTION_ID frames and RETIRE_CONNECTION_ID frames.

## Details

QUIC v1 allows connection ID with lengths from zero to 20 bytes. Since each connection should be
assigned unique IDs, available IDs may be exhausted soon if the size of IDs is too short. Implementations 
may issue connection IDs with variable lengths so long as they can parse Connection IDs
in incoming short packets that do not store destination IDs’ lengths. 
You should use a fixed length of **20 bytes** in this lab. 
This is because it is designed to not let the observers (including our grading script) infer the length of the DCID just from one short-header packet.

ID should be generated randomly and should not expose any clear pattern.
For example, you could not use an auto-incremental counter to generate self-increasing sequences as connection ID.
Generated connection ID should be unique, at least in all active connections. It’s better if each ID is unique for all connections, i.e., including closed ones. It may be tricky to keep this variant for a large system with millions of connections. But in this lab, it’s OK to assume that only several connections are created during one execution, so don’t spend much time optimizing the de-duplication
algorithm’s time or space efficiency.
You should follow the connection ID negotiation procedure described in the mini-RFC. The
destination connection ID field of all packets, except the first Initial sent by the client, should be correctly filled with a valid connection ID selected by the peer.

To show that `NEW_CONNECTION_ID` and `RETIRE_CONNECTION_ID` frames are generated and handled correctly, the client should immediately assign two new connection IDs and retire the one used during the handshake once the handshake completes. The server should use any of the two new connection IDs and never use the old one after receiving packets containing these two frames.

## Related Testcase

We will run the pingpong program with an environment variable `TEST_CONNECTION_ID` set. The program should complete and exit gracefully. Packet traces are checked to see whether all packets contain valid connection IDs and whether `NEW_CONNECTION_ID` and `RETIRE_CONNECTION_ID` frames are sent by the client and correctly handled by the server. Notice that the automated test for this only checks whether these
frames exists. The interaction logic of these will be inspected manually during the meeting with TA from the `.pcap` file.

Besides, to get full credit of this feature, you should explain how your Connection IDs are generated during the meeting with the TA and show that they are securely and randomly generated.
