---
icon: lucide/flask-conical
---

# Lab II-41: Advanced Stream Operations (Optional)

## Overview

You need to add support for stream abort (by the receiving endpoint) and reset (by the sending
endpoint). Note that it’s the application that issues abort or reset operations. The stack itself will
not close a single stream even if some error happens; instead, the entire connection is closed. You
need to add four APIs in your QUIC implementations, one that resets a stream, one that aborts a
stream, one registers an abort callback, and one registers a reset callback.




## Details

For bi-directional streams, abort or reset in one direction does not affect the other direction, just like the case of a normal close (by setting the FIN bit in the frame ID). Though the other direction could continue to work as the specification allows, we will close it gracefully when one direction is reset or aborted in all test cases.


## Related Test Cases

???+ warning "Help us"

    The testcase for this lab is currently unimplemented. Reach out to TA if you want to implement it and get the credits. If you are willing to help us with designing this test-case (including the grading script and / or the test application implementation) by pull requests, it will be greatly appreciated with extra credits.
    

We will run a modified version of the ping-pong program.
The client program generates a random integer `x` between `0x3000` and `0x3fff`.
The client starts the ping-pong lopping as before but resets the stream before sending the 10th ping, with the application protocol error code `x`.
The server closes the other direction when it receives the resetting message and changes its response from "pong" to "pong pong".
The client will then continue the ping-pong loop by creating a new stream but sending "ping ping" to the server now.
After receiving 10 “ping ping”, the server aborts the stream as a receiver and ends the stream as a sender, using `x` as the application protocol error code.
The client then handles the aborting message, closes the connection, and exits gracefully.

Packet traces are checked to see whether endpoints transfer frames as described above. 

