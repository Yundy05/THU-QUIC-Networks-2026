---
icon: lucide/flask-conical
---

# Lab I-3: Congestion Control

## Overview

Read `Mini RFC: Congestion Control` and implement the NewReno congestion control algorithm
described in the mini RFC. Your implementation should support running the congestion control `file_transfer` program in a lossy network environment.
It transfers a large chunk (around 10MiB) of data from the server to the client, validates checksum of the data. You have to complete the transfer in a reasonable time. Specifically, your implementation should estimate the RTT accurately, adjust the congestion window effectively, detect and retransmit lost data quickly. However,
we are only concerned with and check the elapsed time in all test cases.
You could estimate the "reasonable" time by transferring the same size of data in the same network environment using TCP (tools such as iperf may be helpful). 


## Related Testcases

- `file_transfer`
- `file_transfer_lossy`


!!! Note

    You will get half of the credit of these testcases, as long as your implementation transferred the data successfully and correctly before timeout (1min), which is defined as passing the SHA-256 checksum check after the transfer.

    To get full credit, your implementation should match some **performance metrics** in 
    the lossy testcase, including flow completion time and total bytes sent. We will update the detailed criteria later here. 
    
    
## Notes

- You can emulate lossy network environment (especially the network bandwidth) using Linux’s `tc` tool, or `rattan`, as with in Project I-2.
- Bugs on congestion control are sometimes difficult to find if the application advances constantly but slowly. TCP is a good reference to learn the desired performance on a given connection.


## Reference Implementation

Following is *pseudo-code* describing a reference implementation of QUIC reliable transmission. **You may or may not follow it**.

```python
def Initial():
    congestion_window = kInitialWindow
    bytes_in_flight = 0
    congestion_recovery_start_time = 0
    ssthresh = infinite
    
def OnPacketSentCC(sent_bytes):
    bytes_in_flight += sent_bytes
    
    
def InCongestionRecovery(sent_time):
    return sent_time <= congestion_recovery_start_time
    
def OnPacketsAcked(acked_packets):
    for acked_packet in acked_packets:
        OnPacketAcked(acked_packet)

def OnPacketAcked(acked_packet):
    if (not acked_packet.in_flight):
        return;
    # Remove from bytes_in_flight.
    bytes_in_flight -= acked_packet.sent_bytes
    # Do not increase congestion_window if application
    # limited or flow control limited.
    if (IsAppOrFlowControlLimited())
    return
    # Do not increase congestion window in recovery period.
    if (InCongestionRecovery(acked_packet.time_sent)):
    return
    if (congestion_window < ssthresh):
    # Slow start.
    congestion_window += acked_packet.sent_bytes
    else:
    # Congestion avoidance.
    congestion_window +=
    max_datagram_size * acked_packet.sent_bytes / congestion_window

def OnCongestionEvent(sent_time):
    # No reaction if already in a recovery period.
    if (InCongestionRecovery(sent_time)):
        return
    # Enter recovery period.
    congestion_recovery_start_time = now()
    ssthresh = congestion_window * kLossReductionFactor
    congestion_window = max(ssthresh, kMinimumWindow)
    # A packet can be sent to speed up loss recovery.
    MaybeSendOnePacket()

def OnPacketsLost(lost_packets):
    sent_time_of_last_loss = 0
    # Remove lost packets from bytes_in_flight.
    for lost_packet in lost_packets:
        if lost_packet.in_flight:
            bytes_in_flight -= lost_packet.sent_bytes
            sent_time_of_last_loss = max(sent_time_of_last_loss,
            lost_packet.time_sent)
    # Congestion event if in-flight packets were lost
    if (sent_time_of_last_loss != 0):
        OnCongestionEvent(sent_time_of_last_loss)
```