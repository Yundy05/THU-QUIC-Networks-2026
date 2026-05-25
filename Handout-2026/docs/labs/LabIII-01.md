---
icon: lucide/flask-conical
---
 
 
# LabIII-01 Inter-operation with real world QUIC implementations


## Overview

In this lab, you should integrate your QUIC implementation with **one of** the real world QUIC implementations, including but not limited to [tquic](https://github.com/tencent/tquic),
[quic-go](https://github.com/quic-go/quic-go), or [quinn](https://github.com/quinn-rs/quinn).


## Details

To get the scores for this lab, you should make your QUIC implementation work correctly with the chosen real world QUIC implementation, as a HTTP/3 File Server and the corresponding Client. Here, the `HTTP/3` File Server is defined as that used in the famous [quic-interop](https://interop.seemann.io/quic) project. You may refer to [tquic-tools](https://github.com/Tencent/tquic/tree/develop/tools) as an example.

- Yours as Server (60%) : Your implementation as a HTTP/3 File Server, and the corresponding Client is the chosen real world QUIC implementation.
- Yours as Client (40%) : Your implementation as a HTTP/3 Client, and the corresponding Server is the chosen real world QUIC implementation.


!!! tip "You may not need to implement encryption"


    `tquic_tools` exposes an [`--disable_encryption`](https://github.com/Tencent/tquic/blob/0169abde20c6701c68b1ac5021c3e64702f20cfa/tools/src/bin/tquic_server.rs#L257-L259) flag to work with unencrypted 1-RTT packets. This may be of help if you do not plan to support encryption. Thus you do not need to finish LabII-92 first to get full scores of this lab.


## How this would be test

Please set up your own environment and test your implementation against the chosen real world QUIC implementation. To get the scores, please demonstrate the interoperability test during the meeting with TA.