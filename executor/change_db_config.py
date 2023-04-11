import psycopg2
import jinja2

class ApplyDBConfig:
    def __init__(self, dbname, user, password):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn=None

    def connect_conn(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host='localhost', 
            port='5432'
        )

    def apply_config(self, config_values):
        # 创建游标
        self.conn.set_session(autocommit=True)  # 设置连接自动提交
        cur = self.conn.cursor()
        
        try:
            # 将配置SQL语句提交到数据库
            # cur.execute(config_sql)
            for key, value in config_values.items():
            # 根据 value 的类型生成不同的 SQL 语句
                if isinstance(value, int):
                    sql = f"ALTER SYSTEM SET {key} = {value};"
                else:
                    sql = f"ALTER SYSTEM SET {key} = '{value}';"
                # 执行 SQL 语句
                cur.execute(sql)

            # 重新加载配置文件
            cur.execute("SELECT pg_reload_conf();")
            # print("配置已应用成功！")
        except Exception as e:
            # 回滚事务
            self.conn.rollback()
            print("配置应用失败！", e)
        finally:
            # 关闭游标和连接
            cur.close()
            
    def close_conn(self):
        self.conn.close()


if __name__ == '__main__':
    applydbconfig=ApplyDBConfig(dbname='ycsb_test', user='postgres', password='123456')
    # Define configuration values
    config_values = {
        "max_connections": 120,
        "shared_buffers": "4GB",
        "work_mem": "64MB",
        "effective_cache_size": "2GB"
    }
    applydbconfig.connect_conn()
    applydbconfig.apply_config(config_values)
    applydbconfig.close_conn()
