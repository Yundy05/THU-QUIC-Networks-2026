---
icon: lucide/flask-conical
---

# Lab 0: About The Framework 

!!! note "Note on the Frameworks and Languages"
    You may change any part in the framework, or even start from scratch entirely if you have better ideas and design.
    The evaluation will be based on the executable, so it is language-agnostic. See [this page](./Evaluation) for details.

## The C++ Framework

### Basic Model
We provide a framework which applies the single-thread-event-loop model. 
The single-thread design simplifies our implementation as there is no need for synchronization primitives, such as mutex or semaphore, in our code. 
In an event-based framework, the user (application) registers a series of callback functions which react to different events given by the framework. 
Study `src/client.cc` and `src/server.cc` if you are not familiar with such style of programming.
Our framework could be simplified to a big loop as shown below, in which the framework constantly reads data from a UDP socket, feeds data to the application by calling a callback function
registered by the application, and moves data sent by the application to the network.

``` c++ title="The Single-Thread Event Loop Model"

void SocketLoop {
    for(;;) {
        auto packet = Parser(recvFromSocket());
        callback(packet.data);
        sendToSocket(Encoder(pendingPakcets));
    }
}
void callback(Data data) {
    auto newData = process(data);
    pengingPackets.add(newData);
}
```
### Utilities

We also provide a lot of helper functions for you, such as:

- `src/utils/socket.{hh, cc}`: a C++ wrapper of the OS socket API
- `src/payload/{frame, packet}.hh`: encoding and decoding of QUIC frames and packets
- `src/utils/random.hh`: a random number generator

Please read them carefully before coding. Submit a pull request if you find and fix a bug in those
helper functions.

### API

You may need to implement the following API in your implementation:


``` c++ title="The APIs to implement"
/*
* Invoke when the client or server completes handshake and the connection is ready.
* @param connection_descriptor: a 64 bit descriptor of the established connection
* @return errorCode
* */
using ConnectionReadyCallbackType = std::function<int(uint64_t)>;
/* Client Only */
/*
* create a new connection
* @param addrTo: target network address (ip and port)
* @param callback: callback when the connection is ready
* @return: descriptor of the created connection
* */
uint64_t CreateConnection(struct sockaddr_in& addrTo,
const ConnectionReadyCallbackType& callback);
/* Server Only */
/*
* Set the callback when the server receives a new connection
* @param callback: callback when the connection is ready
* @return: error code
* */
int SetConnectionReadyCallback(ConnectionReadyCallbackType callback);
/* Both */
/*
* Close a connection immediately
* @param descriptor: descriptor of the connection to be closed
* @param reason: a message sent to the peer explaining why the connection gets closed
* @param errorCode: the error code sent to the peer
* @return: error code
* */
int CloseConnection(uint64_t descriptor, const std::string& reason,
uint64_t errorCode);
/*
* Invoke when a connection is closed by the peer
* @param descriptor of the connection closed by the peer
* @param reason: the message sent by the peer explaining why the connection is closed
* @param errorCode: the error code sent by the peer
* @return error code
* */
using ConnectionCloseCallbackType = std::function<int(uint64_t, std::string, uint64_t)>;
/*
* Set the callback when a connection is closed by the peer
* @param descriptor of the target connection
* @param callback: callback function
* @return error code
* */
int SetConnectionCloseCallback(uint64_t descriptor,
ConnectionCloseCallbackType callback);
/*
* Create a new stream
* @param descriptor: descriptor of the target connection
* @param whether the stream is unidirectional
* @return stream ID of the created stream
* */
uint64_t CreateStream(uint64_t descriptor, bool bidirectional);
/* End a stream, send all pending data and mark the stream as FIN
* @param descriptor: descriptor of the target connection
* @param streamID of the target stream
* @return errorCode
* */
int CloseStream(uint64_t descriptor, uint64_t streamID);
/* send data to the peer in a certain stream
* @param descriptor: descriptor of the target connection
* @param streamID: streamID of the target stream
* @param buf: the sending buffer
* @param len: length of the sending buffer
* @param FIN: whether the stream ends after sending the buffer
* @return errorCode
* */
int SendData(uint64_t descriptor, uint64_t streamID,
std::unique_ptr<uint8_t[]> buf, size_t len,
bool FIN = false);
/*
* Invoke when receives data in a new stream from the remote peer
* @param descriptor of the connection the new stream belongs to
* @param streamID of the created stream
* @return errorCode
* */
using StreamReadyCallbackType = std::function<int(uint64_t, uint64_t)>;
/*
* set the callback function when a new stream arrives for a certain connection
* @param descriptor: descriptor of the target connection
* @param callback: callback function
* @return errorCode
* */
int SetStreamReadyCallback(uint64_t descriptor, StreamReadyCallbackType callback);
/*
* Invoke when receives data from the peer
* @param connection descriptor
* @param stream ID in which data arrives
* @param receiving data buffer
* @param length of the receiving data buffer
* @param whether the stream ends
* @return errorCode
* */
using StreamDataReadyCallbackType = std::function<int(
uint64_t, uint64_t, std::unique_ptr<uint8_t[]>, size_t, bool)>;
/*
* set the callback function invoked when receives application data
* @param descriptor: descriptor of the target connection
* @param streamID: stream ID of the target stream
* @param callback: callback function
* @return errorCode
* */
int SetStreamDataReadyCallback(uint64_t descriptor, uint64_t streamID,
StreamDataReadyCallbackType callback);
```

## The Rust Framework

Thanks to [@AsakuraMizu](https://github.com/AsakuraMizu)'s brilliant work, and with his consent, we have a Rust framework since 2026 Spring.

### Basic Model

As handling concurrency is much easier in Rust than in C++, the framework is based on the famous 
async runtime [tokio](https://tokio.rs/).


In the framework, there are 2 kinds of coroutines:

- `Endpoint`. Either end of the transportation, client or server, is abstracted as an `Endpoint`. See `core/src/endpoint.rs`. The Endpoint is responsible for **receiving** packets, and may either dispatch the packet to a `Connection`, or create a new `Connection` if none exists. There should be at most one `Endpoint` during the lifetime of the process.


- `Connection` contains all the state of one QUIC connection. It handles the packets dispatched by the `Endpoint`, and may **sending** packets out via a wrapper of a UDP socket.

The loop for the `Connection` can be simplified as the pseudo-code below:

```rust title="Connection loop"
// While the connection is not drained
while !matches!(connection.state, State::Drained) {
    // Generate packets to send
    while connection.poll_transmit(Instant::now()){};
 
    // Send scheduled packets as soon as possible
    while let Some(_) = connection.path.send_one(&mut connection.spaces).await {
        //...
    }
    
    // Wait for any of these:
    tokio::select! {
        biased;
        // Call the application's transport event handler
        Some(event) = connection.events_rx.recv() => {
            transport_handler(&mut connection, event);
        },
        // A timer expired
        timer = &mut connection.timers => {
            connection.handle_timeout(timer, Instant::now());
        },
        // Receive data from endpoint
        Some((now, data)) = data_rx.recv() => {
            connection.handle_data(now, data);
        },
        // Break idle
        _ = tokio::time::sleep(Duration::from_millis(1000))=>{}
    }
}
```
!!! warning "Pitfall: Connection Loop"
    We have basically 2 kinds of things to do in the connection loop:
    
    1. Do some "instant" work, such as generating packets or sending out a UDP packet. As usually sending out a UDP packet requires no waiting, for simplicity, we consider sending out a UDP packet "instant".  
    
    2. Wait for something (a packet, a timer expiration, etc.). 
    
    If you spend too much time in the "instant" works with out checking the "waiting" ones, you may not be able to handle the "waiting"s in a timely manner and may even end up in a deadlock . The design in the framework code is for simplicity and has **not** been fully optimized, yet in the TA's implementation, it works and call pass the tests.

The so-called `transport_handler` is a function that handles the transport events of interest for the application. This contains:
```rust title="Transport Events"
pub enum Event {
    /// The connection was successfully established
    Connected,
    /// The connection was lost
    ConnectionLost(ConnectionError),
    /// Stream events, including a stream become likely readable, finished, or asked to be closed by the peer
    Stream(StreamEvent),
}
```

The application provides a `transport_handler` for each connection to handle these events, whose type is defined as:
```rust title="transport_handler"
pub type TransportHandler = Box<dyn FnMut(&mut Connection, Event) + Send>;
```
See `src/bin/client.rs` and `src/bin/server.rs` for examples.

### Utilities

The `mzquic-proto` crate (located at `proto`) contains types and functions for encoding and decoding QUIC frames and packets.

The `core/src/connection/timer.rs` provide a timer implementation that can wait for a set of timers simultaneously and wait for the first one to expire.

### API

As we abstract the applications as `TransportHandler` functions, the API exposed to applications is basically the public functions of the `Connection` struct.

This includes:

```rust title="Connection API"
/// Close a connection immediately
pub fn close(&mut self, error_code: u64, reason: Bytes);

/// Open a stream with designated direction
pub fn open_stream(&mut self, dir: Dir)-> StreamId ;

/// Provide control over streams
pub fn recv_stream(&mut self, id: StreamId) -> RecvStream<'_> ;
pub fn send_stream(&mut self, id: StreamId) -> SendStream<'_> ;
```

!!! note "About `TODO`"
    We mark `TODO` as places where you may need to implement something in the framework. Due to possibly different design choices, these marks are just suggestions but not requirements or directives.



## Suggestions
- Start as soon as possible. Learn to work as a team at the very beginning of this project.
- Ask questions by submitting issues. You may ask questions without a single correct answer to
start a discussion in our small community.
- I personally encourage all students to ask questions in public, such as by opening an issue in
Github. Others with the same question can learn from it, and others having the answer can
participate in the discussion. But it’s OK if you want to keep your questions private, you can
talk to TA in person during office hours. Note that you cannot ask a question that is too specific
in public. For example, you cannot paste your code that cannot compile and ask others to find
your syntax error.
- (For C++) Learn some C++ if you are not familiar with it or have forgotten it. A Tour of C++ written by Bjarne Stroustrup is a good reference. At least, You need to know how to use smart pointers (std::shared_ptr and std::unique_ptr) and the polymorphic function wrapper (std::function).
- (For Rust) Learn some Rust if you are not familiar with it or have forgotten it. You do not need to be an expert, but it is recommended that you be familiar with the some chapters of the [Rust book, 《Rust 圣经》](https://doc.rust-lang.org/book/), including Chapters 3, 4, 7, 13, 17, and 19. Luckily, we will **not** dive into the domain of the [Rustonomicon, 《Rust 死灵书》](https://doc.rust-lang.org/nomicon/) or [The Little Book of Rust Macros, 《Rust 小宏书》](https://lukaswirth.dev/tlborm/).
