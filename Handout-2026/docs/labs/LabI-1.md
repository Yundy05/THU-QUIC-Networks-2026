---
icon: lucide/flask-conical
---

# Lab I-1: The QUIC Transport Protocol

## Overview

In this lab, you need to use QUIC packets and frames to create a connection and transfer data
between two endpoints in a lossless network. As our first step, we **skip** almost everything, including cryptographic key generation, transport parameter exchange, connection ID negotiation, and
so on in the QUIC handshake. Our handshake is as simple as both endpoints exchanging an Initial packet. Though your implementation is still not the standard QUIC, we still call it QUIC for convenience in this and all lab handouts.

Your QUIC implementation needs to support a ping-pong program. The ping-pong client creates a connection and sends a "PING" message and replies a "PING" message when it receives a "PONG" message. It closes the connection after receiving "PONG" for 20 times. The ping-pong server sends a "PONG" message whenever it receives a "PING" message.


!!! note

    The ping-pong client and server sends `STREAM` frames (type 0x08 ~ 0x0f) containing the text string of "PING" or "PONG", not the `PING` frames(type 0x01).


## Related Testcases

- `ping-pong`
