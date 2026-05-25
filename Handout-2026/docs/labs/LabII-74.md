---
icon: lucide/flask-conical
---


# Lab II-74: Persistent Congestion (Optional)

## Overview
In this lab, you have to recognize persistent congestion in the network and limit the sending rate
under such circumstances. Persistent congestion (detection) avoids adding more pressure on an already broken network and thus may help the network recover. It also saves computation resources on the host by reducing retransmissions.

## Details
The congestion window is reduced to the minimum when persistent congestion is detected. It will
be recovered slowly by later acknowledgments when the network later recovers from the persistent congestion. Obviously, a false positive (declare that network experiences persistent congestion when it does not) is very detrimental to transmission performance, and it’s controlled by the threshold kPersistentCongestionThreshold as specified in the RFC. It’s OK just to use the default value three recommended by the RFC.

## Hot this feature is tested
We will run the file transfer program in a network that periodically drops all packets for a while.
If persistent congestion detection works well and reduces the congestion window in time, the number of packets dropped will be smaller than those without persistent congestion detection.

We will use total bytes sent by the sender ($x$) to finish the transmission as a metric to evaluate the performance.


The detailed grading formula is as follows (where $1$ is the full credit of this lab):

$$
\text{Score} \ge  \min \left(3 - \dfrac{0.9 x}{\text {FILE_SIZE}},  1\right)
$$

The $\ge$ in the formula means that the future amending of this page will only relax the score requirement, not tighten it. Talk to TA if you think the parameters need to be adjusted.

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




