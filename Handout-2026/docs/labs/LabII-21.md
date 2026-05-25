---
icon: lucide/flask-conical
---

# Lab II-21: Idle Timeout (Optional)

## Overview

In this lab, you should implement idle timeout and timeout deferring (“keep-alive”). As the name tells, idle timeout allows an endpoint to silently close the connection if it has not heard from its peer after a while. And, deferring timeout will enable applications to ask the QUIC implementation to prevent the connection from being closed by the peer due to idle timeout.

## Requirements


The timeout value is negotiated using the transport parameter max_idle_timeout by design. In this lab, we use a constant value of **10 seconds**. Remember that the periodic pinging that we use to detect loss of tail packets (as a suboptimal PTO) should be removed before implementing the idle timeout. Otherwise, the link is never idle; thus, an idle timeout is never triggered. But you should implement PTO then to detect loss of tail packets.
Implementation could defer connection timeout by, funny, periodically sending PING frame, so there is not much to do for us. A connection may still be closed due to an idle timeout even if the application asks for deferring. For example, if the link between two endpoints breaks for a long enough time, both endpoints will close the connection eventually.
When a connection is closed due to an idle timeout, the registered connection close callback, if available, should be called.


## Related Testcase


This feature will be tested black-box only.
We will run the file transfer application. The emulated network path between the client and the server will have a bandwidth of merely 1Mbps for the first 10 seconds, so it is impossible for a 32MiB file to be transferred within that time. After that, the loss rate would be increased to 100% for the, effectively cutting off the link and leaving the connection idle. The client should close the connection due to the idle timeout. You get full score if the clients exists with in 40 seconds since the expr starts.