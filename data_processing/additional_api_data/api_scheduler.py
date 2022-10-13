import time


class APIScheduler:
    """ Scheduler for Semnatic Scholar API to prevent to get banned """

    def __init__(self):
        self.request_limit = 100
        self.time_limit = 300
        self.buffer = []
        self.ready = True

    def update(self):
        """ Updates the buffer and checks the limit to make shure that the api rate limits are satisfied"""
        self.buffer.append(time.time())
        if len(self.buffer) >= self.request_limit:
            self.ready = False
            self._wait_loop()
            self.ready = True

    def _wait_loop(self) -> None:
        """ waits till buffer length is less than 100 """
        while len(self.buffer) >= self.request_limit:
            for count, req_time in enumerate(self.buffer):
                if time.time() - req_time > self.time_limit:
                    self.buffer.pop(count)
                else:
                    break
            time.sleep(1)
