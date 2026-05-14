# How to run

Run file transfer server and client:
```bash
./target/release/server --file-lock ./xxx.lock   0.0.0.0:7722 file-transfer --path ./original.bin
./target/release/client --file-lock ./xxx.lock 127.0.0.1:7722 file-transfer --path ./output.bin
```

Run ping pong server and client:
```bash
./target/release/server --file-lock ./xxx.lock   0.0.0.0:7722 ping-pong
./target/release/client --file-lock ./xxx.lock 127.0.0.1:7722 ping-pong
```

Run stream echo server and client:
```bash
./target/release/server --file-lock ./xxx.lock   0.0.0.0:7722 stream-echo
./target/release/client --file-lock ./xxx.lock 127.0.0.1:7722 stream-echo
```
