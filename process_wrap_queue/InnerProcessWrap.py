import time
import threading
import multiprocessing
import random
import string
import os

def generate_random_string(length): # 生成随机序列号，用于全局标定一个任务
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string

# 进程状态
#   INIT: 已经初始化，但是还没开始运行
#   RUN : 正在运行
#   TERM: 运行已经结束（可能是出错或者正常结束）

def savedata(filepath: str, data: str): # 将数据存储到指定文件
    dirpos = os.path.dirname(os.path.abspath(filepath))
    os.makedirs(dirpos, exist_ok=True) # 保证文件存在
    try:
        fp = open(filepath, "w")
        fp.write(data)
    except:
        pass

def getdata(filepath: str) -> str: # 读取数据并删除文件
    dirpos = os.path.dirname(os.path.abspath(filepath))
    os.makedirs(dirpos, exist_ok=True) # 保证文件存在
    ans = ""
    try:
        fp = open(filepath, "r")
        ans = fp.read()
    except:
        pass
    finally: # 删除临时文件
        try:
            os.remove(filepath)
        except:
            pass
    return ans

global_inner_process_wrap_dict = {}

class InnerProcessWrap:

    # 指定命令和当前工作目录
    def __init__(self, worker_function, args, timeout=None):
        def monitor_function(): # 轮询监视器函数
            while True:
                time.sleep(0.15)
                if self.get_status()["status"] == "TERM": # 监视器退出
                    return
                if self.timeout is not None: # 超时机制
                    if self.get_status()["status"] == "RUN" and self.get_status_time_now() >= timeout:
                        self.kill_task()
        self.obj_uuid   = "InnerProcessWrap_" + generate_random_string(128) # 把自己注册到全局管理器对象
        global global_inner_process_wrap_dict
        global_inner_process_wrap_dict[self.obj_uuid] = self

        self.worker_function = worker_function
        self.args            = args
        self.begin_time      = time.time() # 什么时刻进入当前状态
        self.pobj            = None
        self.timeout         = timeout
        self.monitor         = threading.Thread(target=monitor_function)
        self.run_time        = None # 记录进程运行的总时长
        self.return_value    = None
        self.end_flag        = False # 是否进行过死亡结算
        self.aux_info        = {}
        self.lock            = threading.Lock()

    # 获取当前状态所处的时间
    def get_status_time_now(self):
        return time.time() - self.begin_time
    
    # 获取当前进程状态
    def get_status(self):
        with self.lock:
            common_dic = {
                "obj_uuid"        : self.obj_uuid,
                "begin_time"      : self.begin_time,
                "worker_function" : str(self.worker_function),
                "args"            : str(self.args),
                "info"            : None,
                "aux_info"        : self.aux_info
            }
            if self.pobj is None: # 当前进程尚未初始化
                update_dic = {
                    "status": "INIT",
                }
            elif self.pobj.is_alive(): # 程序正在运行
                update_dic = {
                    "status": "RUN",
                }
            else:
                if self.end_flag is False: # 初始化时获取
                    self.end_flag     = True
                    self.run_time     = time.time() - self.begin_time # 总运行时间
                    self.begin_time   = time.time()
                    try:
                        self.pobj.join() # 结束进程
                    except:
                        pass
                    try:
                        self.return_value = getdata("/tmp/%s" % self.obj_uuid)
                    except:
                        self.return_value = None
                update_dic = { # 程序已经运行结束
                    "status": "TERM",
                    "info": {
                        "return_value": self.return_value,
                        "run_time"    : self.run_time # 总运行时间
                    }
                }
            common_dic.update(update_dic)
        return common_dic
    
    # 启动任务
    def run_task(self):
        if self.pobj is not None: # 不要重复启动已经启动过的任务
            return
        def worker(args: list) -> None:
            tmp = self.worker_function(*args)
            savedata("/tmp/%s" % self.obj_uuid, str(tmp))
        self.begin_time = time.time()
        self.pobj       = multiprocessing.Process(target=worker, args=(self.args, ))
        self.pobj.start()
        self.monitor.start() # 启动监视器线程

    # 杀死任务
    # 此函数不应该加锁，因为它调用了加锁的函数
    def kill_task(self):
        if self.pobj is None: # 还没运行，谈何杀死
            return
        if not self.pobj.is_alive(): # 已经结束了，不用再杀死了
            return
        self.pobj.terminate()    # 结束进程
        self.pobj.join()         # 结束进程
        with self.lock:
            self.aux_info.update({"killed": True}) # 是由用户自己杀死的
        self.get_status()            # 更新状态信息

def test1():
    def test_function(a, b, c, d) -> str:
        time.sleep(3)
        return "%d, %d, %d, %d" % (a, b, c, d)
    pw = InnerProcessWrap(test_function, (1, 2, 3, 4))
    print(pw.get_status())
    pw.run_task()
    print(pw.get_status())
    time.sleep(2)
    print(pw.get_status())
    pw.kill_task()
    print(pw.get_status())
    time.sleep(1)
    print(pw.get_status())

def test2():
    def test_function(a, b, c, d) -> str:
        for _ in range(5):
            time.sleep(1)
        return "%d, %d, %d, %d" % (a, b, c, d)
    pw = InnerProcessWrap(test_function, (1, 2, 3, 4), timeout=1.7)
    print(pw.get_status())
    pw.run_task()
    print(pw.get_status())
    time.sleep(3)
    print(pw.get_status())

if __name__ == "__main__":
    test2()