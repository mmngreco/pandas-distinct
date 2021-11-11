#!ipython

import utils
import pandas as pd
import numpy as np


def make_left_right(n=10):
    left = pd.DataFrame(np.random.randint(0, 5, (n, 2)))
    right = pd.DataFrame(np.random.randint(0, 5, (n, 2)))
    return (left, right)


n = 100_000
%timeit core.distinct(*make_left_right(n=n), subset=[0, 1])
%timeit core.distinct_pandas(*make_left_right(n=n), subset=[0, 1])
%timeit core.distinct_pandas_unstack(*make_left_right(n=n), subset=[0, 1])
%timeit core.distinct_counter(*make_left_right(n=n), subset=[0, 1])

# shape = 100, 2
%timeit core.distinct(left, right, subset=[0, 1])
# 4.28 ms ± 100 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)

%timeit core.distinct_pandas(left, right, subset=[0, 1])
# 31 ms ± 634 µs per loop (mean ± std. dev. of 7 runs, 10 loops each)

# shape = 1000, 2
%timeit core.distinct(left, right, subset=[0, 1])
# 8.06 ms ± 243 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)

%timeit core.distinct_pandas(left, right, subset=[0, 1])
%timeit core.distinct_pandas_unstack(left, right, subset=[0, 1])
# 34.1 ms ± 836 µs per loop (mean ± std. dev. of 7 runs, 10 loops each)
