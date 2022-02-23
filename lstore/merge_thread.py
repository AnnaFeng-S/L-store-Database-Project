import threading, queue, time


class MyThread(threading.Thread):
    def __init__(self, queue):
        super(MyThread, self).__init__()
        self._queue = queue
        self.daemon = True
        self.start()

    def run(self):
        while 1:
            f, args, kargs = self._queue.get()
            try:
                f(*args, **kargs)
            except Exception as e:
                print(e)
            self._queue.task_done()


class ThreadPool():
    def __init__(self):
        self._queue = queue.Queue()
        MyThread(self._queue)

    def add_task(self, f, *args, **kargs):
        self._queue.put((f, args, kargs))

    def wail_complete(self):
        self._queue.join()