---
icon: lucide/circle-check-big
---

# Grading Policy

## Grading Composition

The grade of this project is worth **90%** of your course grade, consisting of two parts:

- **Correctness (75% of 90%)**: how well your implementation scores on the testcases. Note that the correctness of some features, such as the congestion control, is defined by the *delivery performance* of your implementation. This will be described in detail in next section.
- **Meeting and Report (25% of 90%)**: a one-to-one meeting with TA and a final report. Every team should attend a one-to-one meeting with TA. All members should attend this meeting and answer TA’s questions about your design choices and implementation details. Each team also needs to submit a short report to briefly describe its design and respond to all questions not answered in the meeting. Again, both the meeting and the report are required for you to pass the course.


## Test Cases for Correctness

This section lists the breakdown for the "Correctness" portion of the grade. Notice that 
in this section, 100 pts is considered as the "full score" for correctness, corresponding to get 75% of the 90% of your course grade. The pts are distributed as follows, and it is possible to get more than 100 pts according to the following breakdown:

### Basic Features (60 pts) 

You should refer to mini-RFC 01, 02 and 03 to pass the following labs for 60 pts in total.

- [LabI-1](./labs/LabI-1) The QUIC Transporting Protocol
- [LabI-2](./labs/LabI-2) Reliable Transmission
- [LabI-3](./labs/LabI-3) Congestion Control


### Advanced Features (No Upper limit)

Notice that some labs do not have documents and/or testcases. Please reach out to the TA if you plan to earn points from such labs.

#### Connection Establishment

- LabII-11 Transport Parameter Exchange (mini-RFC 11, **5pts**)
- [LabII-12](./labs/LabII-12) Address Validation (mini-RFC 12, **5pts**)
- LabII-13 Connection Multiplexing (mini-RFC 13, **5pts**)

#### Connection Termination

- [LabII-21](./labs/LabII-21) Idle Timeout (mini-RFC 21, **5pts**)
- LabII-22 Stateless Reset (mini-RFC 22, **5pts**)

#### Connection Migration

- [LabII-31](./labs/LabII-31) Connection ID (mini-RFC 31, **8pts**)
- LabII-32 Connection Migration (mini-RFC 32, **10pts**)
- LabII-33 Path Validation (mini-RFC 33, **10pts**)

#### Basic Transmission

- [LabII-41](./labs/LabII-41) Stream Operations: reset/abourt (mini-RFC 41, **5pts**)
- LabII-42 Stream Multiplexing (**8pts**)
- LabII-43 Stream Priority (**8pts**)
- LabII-44 Unidirectional Streams (**5pts**)

#### Reliable Transmission

- LabII-51 Probe Timeout (mini-RFC 51, **5pts**)

#### Flow Control

- LabII-61 Stream Flow Control (mini-RFC 61, **5pts**)
- LabII-62 Connection Flow Control (**5pts**)

#### Congestion Control

- [LabII-71](./labs/LabII-71) alg: CUBIC (RFC 8312, **10pts**)
- [LabII-72](./labs/LabII-72) Pacing (mini-RFC 72, **8pts**)
- LabII-73 ECN (mini-RFC 73, **8pts**)
- [LabII-74](./labs/LabII-74) Persistent Congestion ( **8pts**)

#### Error Handling
- LabII-81 Error Handling (mini-RFC 81, **10pts**)

#### Security
- [LabII-91](./labs/LabII-91) QUIC Handshake (mini-RFC 91, **15pts**)
- LabII-92 Packet Protection (mini-RFC 92, **10pts**)
- LabII-93 0-RTT Transmission (mini-RFC 93, **5pts**)

#### Inter-operability
- [LabIII-01](./labs/LabIII-01) Inter-operation with real world QUIC implementations (**15pts**)


### Bonus (No more than 10 pts)

By submitting PR to improve the framework, testcases and documentation, you can earn **up to 10 pts**.

The following Score tables show the breakdown of points for each test case for the features.



## How is your code tested?

Your code should generate 2 executable files: `server` and `client`.
They should be able to handle all the following CLI arguments:

```bash
# Run file transfer server and client:
./server --file-lock ./xxx.lock   0.0.0.0:8080 file-transfer --path ./original.bin
./client --file-lock ./xxx.lock 127.0.0.1:8080 file-transfer --path ./output.bin

# Run ping pong server and client:
./server --file-lock ./xxx.lock   0.0.0.0:8080 ping-pong
./client --file-lock ./xxx.lock 127.0.0.1:8080 ping-pong

# Run ping pong stop server and client: (For Lab II-41: Advanced Stream Operations)
./server --file-lock ./xxx.lock   0.0.0.0:8080 ping-pong-stop
./client --file-lock ./xxx.lock 127.0.0.1:8080 ping-pong-stop
```

There do exist some extra applications, (e.g. `stream-echo`) in the skeleton code, yet they are informational only and you are free
to remove the support for it. 


The test will be run on a Debian 13 (Trixie) machine, with a 6.12 kernel
```
Linux lethe-4 6.12.74+deb13+1-amd64 #1 SMP PREEMPT_DYNAMIC Debian 6.12.74-2 (2026-03-08) x86_64 GNU/Linux
```

And we will use [this grader program](https://github.com/THU-QUIC-Project/rusty_grader) to test your code.

