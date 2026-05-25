---
icon: lucide/flask-conical
---

# Lab II-71: Cubic Congestion Control (Optional)

You need to add CUBIC congestion control algorithm described in [RFC 8312](https://datatracker.ietf.org/doc/html/rfc8312) to your QUIC implementation in this project.

It’s not required to implement an autonomous or manual congestion control switch. It’s OK
that a `#define` directive (or feature gate in Rust) decides congestion control algorithms at compilation time. CUBIC and NewReno are similar in many aspects, so it’s typical that your CUBIC implementation is not much better than NewReno. But it’s not very likely it gets much worse performance, though.
It’s a little tricky to test or even define the correctness of congestion control implementation.

**So this feature is not auto-tested.** Instead, it’s your responsibility, or to say freedom, to show that
CUBIC is different from NewReno under certain circumstances. Specifically, you should design
experiments to compare the performance of CUBIC and NewReno on different network conditions
(e.g., loss rate, latency, bandwidth), and discuss your results in the meeting with TA and in the final
report. You may find the title of the CUBIC RFC, i.e., “Fast Long-Distance Network”, a good start
point to begin your exploration. TA may give some suggestions on the performance experiments
during the meeting, and you will get total points for this feature if you follow those suggestions.


You may also implement one of the following congestion control algorithms instead of CUBIC:

- BBR [Bottleneck Bandwidth and Round-trip propagation time](https://www.ietf.org/archive/id/draft-cardwell-iccrg-bbr-congestion-control-01.html)
- Copa: [Practical Delay-Based Congestion Control for the Internet](https://web.mit.edu/copa/)
- PCC: [Performance-oriented Congestion Control](https://modong.github.io/pcc-page/)

Please talk to TA before starting implementation if you want to implement one of the above algorithms (a.k.a. other than CUBIC).
