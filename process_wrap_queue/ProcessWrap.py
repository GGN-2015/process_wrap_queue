import time
import os
import shlex
import threading
import subprocess
import random
import string

def generate_random_string(length): # 生成随机序列号，用于全局标定一个任务
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string

# 进程状态
#   INIT: 已经初始化，但是还没开始运行
#   RUN : 正在运行
#   TERM: 运行已经结束（可能是出错或者正常结束）

global_process_wrap_dict = {}

class ProcessWrap:

    # 指定命令和当前工作目录
    def __init__(self, cmd: list, cwd: str):
        def monitor_function(): # 轮询监视器函数
            while True:
                time.sleep(0.15)
                if self.get_status()["status"] == "TERM": # 监视器退出
                    return
        self.obj_uuid   = "ProcessWrap_" + generate_random_string(128) # 把自己注册到全局管理器对象
        global global_process_wrap_dict
        global_process_wrap_dict[self.obj_uuid] = self

        self.cmd        = cmd
        self.cwd        = cwd
        self.begin_time = time.time() # 什么时刻进入当前状态
        self.pobj       = None
        self.monitor    = threading.Thread(target=monitor_function)
        self.stdout     = None
        self.stderr     = None
        self.run_time   = None # 记录进程运行的总时长
        self.aux_info   = {}
        self.lock       = threading.Lock()

    # 获取当前状态所处的时间
    def get_status_time_now(self):
        return time.time() - self.begin_time
    
    # 获取当前进程状态
    def get_status(self):
        with self.lock:
            common_dic = {
                "obj_uuid"   : self.obj_uuid,
                "begin_time" : self.begin_time,
                "cmd"        : self.cmd,
                "cwd"        : self.cwd,
                "info"       : None,
                "aux_info"   : self.aux_info
            }
            if self.pobj is None: # 当前进程尚未初始化
                update_dic = {
                    "status": "INIT",
                }
            elif self.pobj.poll() is None: # 程序正在运行
                update_dic = {
                    "status": "RUN",
                }
            else:
                if self.stdout is None: # 初始化时获取
                    self.returncode = self.pobj.wait()
                    try:
                        self.stdout, self.stderr = self.pobj.communicate()
                    except:
                        self.stdout = b""
                        self.stderr = b""
                    self.run_time   = time.time() - self.begin_time # 总运行时间
                    self.begin_time = time.time()
                update_dic = { # 程序已经运行结束
                    "status": "TERM",
                    "info": {
                        "returncode": self.pobj.returncode,
                        "stdout"    : self.stdout.decode(),
                        "stderr"    : self.stderr.decode(),
                        "run_time"  : self.run_time # 总运行时间
                    }
                }
            common_dic.update(update_dic)
        return common_dic
    
    # 启动任务
    def run_task(self):
        if self.pobj is not None: # 不要重复启动已经启动过的任务
            return
        self.begin_time = time.time()
        self.pobj       = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.monitor.start() # 启动监视器线程

    # 杀死任务
    def kill_task(self):
        if self.pobj is None: # 还没运行，谈何杀死
            return
        if self.pobj.poll() is not None: # 已经结束了，不用再杀死了
            return
        self.pobj.terminate()
        self.aux_info.update({"killed": True}) # 是由用户自己杀死的
        self.pobj.wait()         # 等待进程自然结束
        self.get_status()        # 更新状态信息

if __name__ == "__main__":
    pw = ProcessWrap(shlex.split("bash -c 'sleep 1; sleep 1; sleep 1; sleep 1; sleep 1; echo hello'"), os.getcwd())
    print(pw.get_status())
    pw.run_task()
    print(pw.get_status())
    time.sleep(1)
    print(pw.get_status())
    pw.kill_task()
    print(pw.get_status())