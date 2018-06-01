# streamz_ext

This project is an extention of Matt Rocklin's [`streamz`](https://streamz.readthedocs.io/en/latest/). `streamz` is a pure python streaming data processing system.
This extension is a drop in extension of `streamz` and serves as a staging ground for PRs into `streamz` and is released a bit more frequently.

# Examples
In many ways `streamz_ext` is the same as `streamz` however there are some points of deviations.

`unique` in `streamz` will fail on non-hashables, it will pass in `streamz_ext`
```python
from streamz_ext import Stream
source = Stream()
L = source.unique(history=1).sink_to_list()
source.emit({'a': 1})
source.emit({'a': 1})
L == [{'a': 1}]
```

We provide `starsink` functionality, which `*` unpacks into a sink (like `starmap`)
```python
from streamz_ext import Stream
source = Stream()
source.starsink(lambda x, y: print(x, y))
source.emit(('a', 'b'))
```

`filter` can also take in args and kwargs
```python
from streamz_ext import Stream
source = Stream()
# This will always emit downstream
source.filter(lambda x, t: bool(t), t=True).sink(print)
source.emit(5)
```
