---
icon: lucide/flask-conical
---

# Lab I-2: Reliable Transmission

## Overview

You should read the `Mini RFC : Reliable Transmission` first,
and implement the following features described there:
- acknowledgement generation
- RTT estimation
- loss detection
- retransmission

Then, your implementation should support running the `ping-pong` application in a lossy
network environment.
We will test them in a noisy network, including:
- packet loss
- packet reordering
- variable latency

and, as with the requirements in Project I-1, your client should exit gracefully.

!!! note

    Instead of retransmitting "lost packets" (as in traditional TCP), QUIC never resends packets. Instead, it is the frames that contained in the lost packets that are retransmitted.
    
    
## Related Testcases

- `ping-pong-lossy`


## Related parts of framework

### C++ Frame Work

- Check `/src/utils/interval.hh` for classes which help manage ACK ranges.
- Check `/src/utils/chunkstream.hh` for classes that help manage chunks in an ordered byte stream.

### Rust Frame Work
- Check `core/src/connection/stream/recv.rs` and implement a robust and efficient `Assembler` implementation to handle incoming `STREAM` frames that is delivered out of order.
- Check `core/src/connection/stream/send.rs`, and handle the re-transmission related logic carefully.


## Notes

- How can I got a lossy network channel for debugging?

The traditional and cannocial way is to use Linux [`tc`](https://man7.org/linux/man-pages/man8/tc.8.html) command to simulate a lossy network channel. Example usage:

```bash
# make the local interface randomly drop 10% packet and add a radom delay
# with an average of 200ms and a variance of 20ms to each packet.
tc qdisc add dev lo root netem delay 200ms 20ms distribution normal loss 10%
# restore configurations of the local interface
tc qdisc del dev lo root netem
```


Also, you can use [`rattan`](https://github.com/stack-rs/rattan) to simulate a lossy network channel, in line with how your code will be tested. The simplest way to generate a lossy channel with `rattan`:

```bash
rattan link --uplink-loss 0.1 --downlink-loss 0.2
```

You will get a shell (like Mahimahi) afther the command is executed, where 10% part of "uplink" and 20% part of "downlink" packets will be dropped. Here "uplink" means packets sent inside the shell to the outside (the host machine), and "downlink" means packets sent from the outside to inside the shell.


- It's possible that the Initial packet sent by the client is lost. In the standard QUIC, it's recovered by **Probe Timeout (PTO)**: When a sent packet is not acknowledged in time, the sender sends more packets to detect the loss. **In this project** , you could make the client send Initial packets periodically until the connection is established or the number of sent packets reaches a given upper limit. You are free to choose the maximum number of times to retry the connection and the interval between attempts.

- If the ping-pong message is lost, the sender cannot detect the loss because both the sender and its peer won't send anything until receiving "ping" or "pong". This problem is solved by PTO too in the standard QUIC. In this project, you could let both endpoints send PING frames periodically to help detect loss of such "tail packets".

- What about the `CONNECTION_CLOSE` frame? It's possible to be lost too. But as endpoints ping each other periodically, a PING frame sent by its peer will trigger an endpoint in the closing state to send a `CONNECTION_CLOSE` frame again. Eventually, its peer receives one `CONNECTION_CLOSE` frame and stops sending PING frames, making the connection closed in both endpoints.

## Reference implementation

Following is *pseudo-code* describing a reference implementation of QUIC reliable transmission. **You may or may not follow it**.

```python
def Initial():
    loss_detection_timer.reset()
    latest_rtt = 0
    smoothed_rtt = kInitialRtt
    rttvar = kInitialRtt / 2
    min_rtt = 0
    first_rtt_sample = 0
    largest_acked_packet = infinite
    loss_time = 0
    
def OnPacketSent(packet_number, ack_eliciting):
    sent_packets.packet_number = packet_number
    sent_packets.time_sent = now()
    sent_packets.ack_eliciting = ack_eliciting
    
def IncludeAckEliciting(packets):
    for packet in packets:
        if (packet.ack_eliciting):
            return True
        else:
            return False
    
def OnAckReceived(ack):
    if(largest_acked_packet == infinite):
        largest_acked_packet = ack.largest_acked
    else:
        largest_acked_packet = max(largest_acked_packet, ack.largest_acked)
        
    # DetectAndRemoveAckedPackets finds packets that are newly
    # acknowledged and removes them from sent_packets.
    newly_acked_packets = DetectAndRemoveAckedPackets(ack)
    
    # Nothing to do if there are no newly acked packets.
    if (newly_acked_packets.empty()):
        return
    # Update the RTT if the largest acknowledged is newly acked
    # and at least one ack-eliciting was newly acked.
    if (newly_acked_packets.largest().packet_number == ack.largest_acked &&
        IncludesAckEliciting(newly_acked_packets)):
            latest_rtt = now() - newly_acked_packets.largest().time_sent
            UpdateRtt(ack.ack_delay)
     lost_packets = DetectAndRemoveLostPackets()
     
    if (not lost_packets.empty()):
        OnPacketsLost(lost_packets)
        OnPacketsAcked(newly_acked_packets)
        SetLossDetectionTimer()
            
def UpdateRtt(ack_delay):
    if (first_rtt_sample == 0):
        min_rtt = latest_rtt
        smoothed_rtt = latest_rtt
        rttvar = latest_rtt / 2
        first_rtt_sample = now()
        return
    # min_rtt ignores acknowledgment delay.
    min_rtt = min(min_rtt, latest_rtt)
    # Limit ack_delay by max_ack_delay after handshake
    # confirmation.
    if (handshake confirmed):
        ack_delay = min(ack_delay, max_ack_delay)
        
    # Adjust for acknowledgment delay if plausible.
    adjusted_rtt = latest_rtt
    if (latest_rtt > min_rtt + ack_delay):
        adjusted_rtt = latest_rtt - ack_delay
    rttvar = 3/4 * rttvar + 1/4 * abs(smoothed_rtt - adjusted_rtt)
    smoothed_rtt = 7/8 * smoothed_rtt + 1/8 * adjusted_rtt

def GetLossTimeAndSpace():
    # we only use one packet number space
    return loss_time

def SetLossDetectionTimer():
    earliest_loss_time = GetLossTimeAndSpace()
    if (earliest_loss_time != 0):
        # Time threshold loss detection.
        loss_detection_timer.update(earliest_loss_time)
        return
    else:
        loss_detection_timer.cancel()
        return

def OnLossDetectionTimeout():
    earliest_loss_time = GetLossTimeAndSpace()
    if (earliest_loss_time != 0):
        # Time threshold loss Detection
        lost_packets = DetectAndRemoveLostPackets()
        assert(not lost_packets.empty())
        OnPacketsLost(lost_packets)
        SetLossDetectionTimer()
        return

def DetectAndRemoveLostPackets():
    assert(largest_acked_packet != infinite)
    loss_time = 0
    lost_packets = []
    loss_delay = kTimeThreshold * max(latest_rtt, smoothed_rtt)
    # Minimum time of kGranularity before packets are deemed lost.
    loss_delay = max(loss_delay, kGranularity)
    # Packets sent before this time are deemed lost.
    lost_send_time = now() - loss_delay
    foreach unacked in sent_packets:
    if (unacked.packet_number > largest_acked_packet):
        continue
    # Mark packet as lost, or set time when it should be marked.
    # Note: The use of kPacketThreshold here assumes that there
    # were no sender-induced gaps in the packet number space.
    if (unacked.time_sent <= lost_send_time ||
        largest_acked_packet >=
        unacked.packet_number + kPacketThreshold):
        sent_packets.remove(unacked.packet_number)
        lost_packets.insert(unacked)
    elif: (loss_time == 0):
        loss_time = unacked.time_sent + loss_delay
    else:
        loss_time = min(loss_time, unacked.time_sent + loss_delay)
    
    return lost_packets

```
