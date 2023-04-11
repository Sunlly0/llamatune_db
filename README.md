# LlamaTune DB 端代码补充

LlamaTune 中使用 grpc 实现了 DB 端和 Tune 端的通信，本项目旨在完善 LlamaTune 中缺少的 DB 端代码实现。

proto 定义：
```
message ExecuteRequest{
	// DBMS Info structure
	message DBMSInfo {
	string name = 1;
	google.protobuf.Struct config = 2;
	}
}
message ExecuteReply {
	google.protobuf.Struct results = 1;
}
```

代码结构：
```
.
|-- README.md
|-- config：保存相关配置
|-- executor
|   |-- __init__.py
|   |-- change_db_config.py：实现应用给定配置
|   |-- db_executor.py：实现负载执行和性能结果处理
|   `-- grpc：grpc 通信所需
|       |-- __init__.py
|       |-- nautilus_rpc_pb2.py
|       `-- nautilus_rpc_pb2_grpc.py
`-- run_db_server.py：开启 grpc 服务，处理 Tune 端 request 并发送 response
```