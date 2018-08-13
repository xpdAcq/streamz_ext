from streamz_ext import Stream

import time
# from distributed import Client
# c = Client('tcp://192.168.0.5:8786')


def inc(x):
    time.sleep(1)
    return x + 1


a = Stream()
b = (a
     # .scatter()
     .thread_scatter()
     .map(inc)
     .buffer(10)
     .gather()
     )
c = b.sink(print)
d = b.sink_to_list()
e = b.sink_to_list()

for i in range(100):
    print('input ', i)
    a.emit(i)
print('hi')
print(b.loop)
print(d)
print(sorted(e))
assert d == sorted(e)
