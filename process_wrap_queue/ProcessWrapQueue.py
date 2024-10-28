from .ProcessWrap      import ProcessWrap
from .InnerProcessWrap import InnerProcessWrap

# from ProcessWrap      import ProcessWrap
# from InnerProcessWrap import InnerProcessWrap

import threading
import traceback
import time
import os
import shlex
import json

# 任务队列
# 每个任务队列中一个时刻，至多有一个任务在执行
# 所有被执行完成的任务会被放到另一个队列里

# 任务队列状态
# STOP：执行完手头的任务后不要再继续执行后续任务
# RUN：维持队列正常前进，如果手头没有任务了，就再拿一个过来执行
# QUIT: 强制结束当前队列，结束正在运行的进程
# AUTO：如果当前没有任何任务，则自动将状态设置为 QUIT

class ProcessWrapQueue:
    def __init__(self):
        def monitor_function():
            while True:
                time.sleep(0.1) # 抢占锁
                with self.lock:
                    if self.queue_status == "QUIT":   # 停止任务时，要等待监视器退出
                        for pw_obj in self.run_queue: # 强制结束所有正在运行的进程
                            pw_obj.kill_task()
                            self.term_queue.append(pw_obj)
                        return # 让监督线程退出
                    if self.queue_status in ["RUN", "STOP", "AUTO"] and len(self.run_queue) > 0:
                        if self.run_queue[0].get_status()["status"] == "TERM": # 将已经完成的任务放到结束任务队列
                            term_pw, self.run_queue = self.run_queue[0], self.run_queue[1:]
                            self.term_queue.append(term_pw)
                    if self.queue_status in ["RUN", "AUTO"] and len(self.run_queue) == 0: # 加载新的任务过来执行
                        if len(self.pend_queue) > 0:
                            pend_pw, self.pend_queue = self.pend_queue[0], self.pend_queue[1:]
                            self.run_queue.append(pend_pw)
                            time.sleep(0.5)    # 保证不要有太过频繁的任务启停
                            pend_pw.run_task() # 先放到队列里，再启动
                    if self.queue_status == "AUTO" and len(self.run_queue) == 0 and len(self.pend_queue) == 0:
                        self.queue_status = "QUIT"
                        
        self.term_queue   = [] # 已经结束运行的任务队列
        self.pend_queue   = [] # 还未进入执行的任务队列
        self.run_queue    = [] # 正在执行的任务队列，我们要求正在执行的任务不超过一个
        self.queue_status = "STOP"
        self.monitor      = threading.Thread(target=monitor_function)
        self.lock         = threading.Lock()
        self.monitor.start()

    def add_process_wrap(self, process_wrap): # 向任务队列追加任务
        with self.lock:
            if self.queue_status == "QUIT": # 不要向已经死亡的队列中新增元素
                return
            if not isinstance(process_wrap, ProcessWrap) and not isinstance(process_wrap, InnerProcessWrap):
                return
            if not process_wrap.get_status()["status"] == "INIT": # 不要迁移已经运行的进程
                return
            self.pend_queue.append(process_wrap)

    def set_queue_status(self, new_status: str): # 设置队列状态
        with self.lock:
            if self.queue_status == "QUIT": # 已经被杀死的队列不能再做任何操作
                return
            if not new_status in ["RUN", "STOP", "QUIT", "AUTO"]:
                return
            self.queue_status = new_status

    def kill_task_now(self): # 干掉正在运行的所有任务，这些进程会被监视器移动到 term_queue
        with self.lock:
            for pw_obj in self.run_queue:
                pw_obj.kill_task()

    def get_queue_status(self): # 获取完整系统状态
        with self.lock:
            common = {
                "status": self.queue_status,
                "term_queue": [],
                "run_queue": [],
                "pend_queue": []
            }
            for pw in self.term_queue:
                common["term_queue"].append(pw.get_status())
            for pw in self.run_queue:
                common["run_queue"].append(pw.get_status())
            for pw in self.pend_queue:
                common["pend_queue"].append(pw.get_status())
        return common
    
    # 把当前状态同步到文件
    # 由于此函数调用了带锁的函数，因此此函数不得带锁
    def dump_queue_status_to_file(self, filename: str): 
        try:
            fp = open(filename, "w", encoding="utf-8")
            json.dump(self.get_queue_status(), fp, indent=4, ensure_ascii=False)
            fp.close()
        except:
            pass
    
    def get_queue_status_brief(self): # 获得简略系统状态
        with self.lock:
            ans = {
                "queue_status"  : self.queue_status,
                "term_queue_len": len(self.term_queue),
                "run_queue_len" : len(self.run_queue),
                "pend_queue_len": len(self.pend_queue),
            }
        return ans

if __name__ == "__main__":
    pw_queue = ProcessWrapQueue()
    for i in range(3):
        pw = ProcessWrap(shlex.split("bash -c 'sleep 2; echo hello'"), os.getcwd())
        pw_queue.add_process_wrap(pw)
    for i in range(3):
        def sleep_2_ret(val):
            time.sleep(2)
            return val
        pw = InnerProcessWrap(sleep_2_ret, (str(i),))
        pw_queue.add_process_wrap(pw)
    while True:
        inp = input(">>> ").strip()
        if inp == "disp":
            print(json.dumps(pw_queue.get_queue_status_brief(),indent=4))
        elif inp == "run":
            pw_queue.set_queue_status("AUTO") # 任务结束后自动退出
        else:
            try:
                exec(inp)
            except:
                traceback.print_exc()