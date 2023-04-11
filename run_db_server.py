import grpc
import datetime
import random
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import Struct
# pylint: disable=import-error
import executor.grpc.nautilus_rpc_pb2
import executor.grpc.nautilus_rpc_pb2_grpc
from executor.db_executor import DBExecutor
from executor.change_db_config import ApplyDBConfig
import concurrent.futures as futures
from google.protobuf.json_format import MessageToDict
import time

iter_time=0
# class Greeter(helloworld_pb2_grpc.GreeterServicer):
#     def SayHello(self, request, context):
#         message = 'Hello, {}'.format(request.name)
#         return helloworld_pb2.HelloReply(message=message)

pgbench_command_set=[
    "pgbench -U postgres -f /files/pg_ycsb/scripts/uniform/read.sql -T60 -c10 pg_ycsb",
    "pgbench -U postgres -f /files/pg_ycsb/scripts/uniform/update.sql -T60 -c10 pg_ycsb",
    "pgbench -U postgres -f /files/pg_ycsb/scripts/uniform/insert.sql -T60 -c10 pg_ycsb",
    "pgbench -U postgres -f /files/pg_ycsb/scripts/uniform/scan.sql -T60 -c10 pg_ycsb"
]
pgbench_command="pgbench -U postgres -f /files/pg_ycsb/scripts/uniform/read.sql -T60 -c10 pg_ycsb"
    
class ExecutionService(executor.grpc.nautilus_rpc_pb2_grpc.ExecutionServiceServicer):
    def Execute (self, request, context):
        print("execute")
        global iter_time
        iter_time=iter_time+1
        print("iter_time:",iter_time)
        start_time = time.time()
        config = MessageToDict(request)['dbmsInfo']['config']
        # print(config)
        # config=request['dbms_info']
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
        # summary_stats['throughput(req/sec)']=float(random.randint(1000, 10000))
        # summary_stats['95th_lat(ms)']=float(random.randint(1000, 10000))
        # summary_stats['time(sec)']=0
        # performance_stats['pgbench_summary']=summary_stats
        
        # real perfor by pgbench
        # 应用配置
        print('应用配置...')
        applydbconfig=ApplyDBConfig(dbname='pg_ycsb', user='postgres', password='123456')
        applydbconfig.connect_conn()
        applydbconfig.apply_config(config)
        applydbconfig.close_conn()
        
        # 评估负载情形
        print('评估负载...')
        # pgbench_command = "pgbench -U postgres -c 10 ycsb_test  -j 10 -T 120"
        ## 手工地增加一个负载变化,每迭代30次，随机更换负载执行
        if iter_time%30==0:
            global pgbench_command
            pgbench_command=pgbench_command_set[random.randint(0,3)]
            print("change workload into:",pgbench_command)
            
        dbexecutor=DBExecutor()
        pgbench_results = dbexecutor.run_pgbench_test(pgbench_command)
        
        end_time = time.time()
        run_time = end_time - start_time
        print(pgbench_results)
        print('pass time:',run_time )
        summary_stats['throughput(req/sec)']=float(pgbench_results['tps'])
        summary_stats['95th_lat(ms)']=float(pgbench_results['latency average'])
        summary_stats['time(sec)']=float(run_time)
        performance_stats['pgbench_summary']=summary_stats

        results['run_info']=run_info
        results['performance_stats']=performance_stats
        # print(results)
        print('返回执行结果...')
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
