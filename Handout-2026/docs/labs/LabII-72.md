---
icon: lucide/flask-conical
---

# Lab II-72: Pacing (Optional)

## Overview
Pacing is an unlucky example of leaky abstraction, which means that you have to learn some
details that are unnecessary for you by design. Sadly, we have to realize that network is not a
water pipe, and packets can not be poured into the wire. Pacing asks us to schedule the sending
of packets to make them more distributed over time.


## Details
Typically, pacer could be integrated into existing architectures easily. In QUIC, sending may be rejected by many components, e.g., congestion controller, flow controller, amplification attack defender, pacer just adds another check you need to do before sending a packet out. You could use the leaky bucket algorithm as hinted by the RFC to implement pacing. It’s a well-known and widely applied algorithm, so it’s OK to refer to or even copy existing implementations if their licenses permit. You may need to tune the scale parameter $N$ to pass our tests.


## How This Feature Is Tested


We will run the large file transferring program in a channel with 0 packet drop rate but a very short drop-head queue (queue length = 5 packets), or a so-called "shallow buffer".

The bottleneck bandwidth (at the shallow buffer) will **change during the transmission**.
Your sender should be able to adapt to these changes and maintain pacing. You implement should be able to send at the "proper" rate to avoid overflow.

There will be some performance metrics to calculate the score:

- Total bytes sent by the sender ($x$).
- Percentage of packets that is sent with a "proper" interval ($y$). A proper interval is defined as utilizing 90% to 100% of the available bandwidth.

The detailed grading formula is as follows (where $1$ is the full credit of this lab):

$$
\text{Score} \ge  \min \left(2.5y - \dfrac{0.95 x}{\text {FILE_SIZE}},  1\right)
$$

The $\ge$ in the formula means that the future amending of this page will only relax the score requirement, not tighten it. Talk to TA if you think the parameters need to be adjusted.

The test will be conducted multiple times with different bandwidths, and the final score is the geometric mean of all test runs.



<script id="MathJax-script" async src="https://unpkg.com/mathjax@3/es5/tex-mml-chtml.js"></script>
<script>
  window.MathJax = {
    tex: {
      inlineMath: [["\\(", "\\)"]],
      displayMath: [["\\[", "\\]"]],
      processEscapes: true,
      processEnvironments: true
    },
    options: {
      ignoreHtmlClass: ".*|",
      processHtmlClass: "arithmatex"
    }
  };
</script>