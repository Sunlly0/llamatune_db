import subprocess
import re

class DBExecutor():
    def run_pgbench_test(self,pgbench_command):
        # Run the pgbench command and capture the output
        process = subprocess.Popen(pgbench_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        # print(stdout)

        # Parse the pgbench output into a dictionary
        results = {}
        for line in stdout.decode().split('\n'):
            match = re.search(r'^(.*) = ([\d\.]+) ?(.*)', line)
            if match:
                key = match.group(1)
                value = match.group(2)
                value = re.sub(r'\s*\(.+\)', '', value)
                results[key] = value.strip()
            else:
                match = re.search(r'^(.*): ([\d\.]+) ?(.*)', line)
                if match:
                    key = match.group(1)
                    value = match.group(2)
                    value = re.sub(r'\s*\(.+\)', '', value)
                    results[key] = value
        # print(results)
        return results

    

def run():
    ## 这将运行一个包含10个客户端，4个线程，并发执行1000个事务的pgbench测试，并将其输出保存在一个字典中。你可以根据需要修改pgbench_command中的参数。
    pgbench_command = "pgbench -U postgres -c 10 ycsb_test  -j 4 -T 10"
    dbexecutor=DBExecutor()
    results = dbexecutor.run_pgbench_test(pgbench_command)
    print(results)

if __name__ == '__main__':
    run()



