---
icon: lucide/flask-conical
---

# Lab II-12: Address Validation (Optional)

## Overview
Address validation validates clients’ intent to communicate before starting the connection handling. It's achieved by sending a small cookie to the client and checking that the same cookie is sent back. In this lab, only `Retry` based address validation is required. `NEW_TOKEN` frame-based validation is optional and **will not** be tested. Address validation assumes that attackers could spoof source addresses of packets but could not peek packets sent to these spoofed address, which means it makes little sense to Man-in-the-middle attack.

## Details
Implementations are free to choose how to generate an address validation token. Generally, the source address and port are embedded in the token, so we could later check that tokens are valid and are assigned to the current connection. Clients or external observers should not be able to generate valid retry tokens. One way to achieve this is using an open encryption or digest algorithm with secret keys or salts. 


If encryption is applied, you could store more information in the token to be used later. If the digest is applied, though, you could only use information still available in subsequent packets, such as IP address and port. You could select any a implementation and could use digest function that’s not crypto-secure.

## Related Testcase

This feature will be tested black-box only.
We will run the `ping-pong` application as before. The server and client should execute address validations before starting data transmission. We validate whether there is a `Retry` packet in the packet trace and whether the retry token is attached to the second Initial packet.