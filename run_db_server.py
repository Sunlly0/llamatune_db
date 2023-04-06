import grpc
import datetime
import random
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import Struct
# pylint: disable=import-error
import executor.grpc.nautilus_rpc_pb2
import executor.grpc.nautilus_rpc_pb2_grpc
import concurrent.futures as futures


# class Greeter(helloworld_pb2_grpc.GreeterServicer):
#     def SayHello(self, request, context):
#         message = 'Hello, {}'.format(request.name)
#         return helloworld_pb2.HelloReply(message=message)
    
class ExecutionService(executor.grpc.nautilus_rpc_pb2_grpc.ExecutionServiceServicer):
    def Execute (self, request, context):
        print("execute")
        # 向 Struct 中添加一个值
        results=Struct()
        performance_stats=Struct()
        run_info=Struct()
        summary_stats=Struct()
        
        warm_up=Struct()
        warm_up['result']='ok'
        benchmark=Struct()
        benchmark['result']='ok'
        run_info['warm_up']=warm_up
        run_info['benchmark']=benchmark
        
        performance_stats['ycsb_result']='ok'
        performance_stats['ycsb_raw_result']='ok'
        
        # dummy perfor
        summary_stats['throughput(req/sec)']=float(random.randint(1000, 10000))
        summary_stats['95th_lat(ms)']=float(random.randint(1000, 10000))
        summary_stats['time(sec)']=0
        performance_stats['pgbench_summary']=summary_stats
        
        # real perfor by pgbench
        
        results['run_info']=run_info
        results['performance_stats']=performance_stats
        # print(results)
        return executor.grpc.nautilus_rpc_pb2.ExecuteReply(results=results)
    
    def Heartbeat (self, request, context):
        # message = 'Hello, {}'.format("")
        alive_since = 1
        timestamp = Timestamp(seconds=1670701800, nanos=0)
        jobs_finished = 3
        # print("heartbeat")
        return executor.grpc.nautilus_rpc_pb2.StatsReply(alive_since=alive_since,time_now=timestamp,jobs_finished=jobs_finished)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    executor.grpc.nautilus_rpc_pb2_grpc.add_ExecutionServiceServicer_to_server(ExecutionService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
