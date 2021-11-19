"""Distinct implementations.

There're 5 implementations.

distinct
distinct_counter
distinct_merge

distinct_pandas
distinct_pandas_unstack


Todo
----
- [ ] Exaplain differences between implementations.
- [ ] Check build_freq_rows functions.
- [ ] Check repeat_rows functions.
"""
from itertools import chain, zip_longest
from collections import Counter, defaultdict

import pandas as pd


def _update_key_counter(idx, key, same, opposite):
    """Increase/decrease counters.

    Deletes intersection elements. Inplace modifications.

    Parameters
    ----------
    idx : int
    key : tuple
        key belong to `same`.  same : dict
        is the set from `row` comes from.
    opposite : dict
        is the set where `row` doesn't belong to.

    Returns
    -------
    key : tuple
    """

    # - If key is in opposite then reduce its counter by one to remove the
    # element.
    # - If key is not in opposite then increase the counter of same by one as
    # it is a difference.
    if key is None:
        # early stop
        return key

    if opposite.get(key):
        opposite[key].pop(0)  # FIFO
    else:
        same[key].append(idx)

    return key


def dict2dataframe(dict_idx, original):
    """Convert dict to DataFrame.

    Parameters
    ----------
    dict_idx : dict
        Frequency dict.
    original : pandas.DataFrame
        Original DataFrame to build the new DataFrame using the frequency dict.

    Returns
    -------
    out : pandas.DataFrame
    """

    # converts the frequency dictionary into a dataframe.
    idx_list = sorted(chain.from_iterable(dict_idx.values()))
    if not idx_list:
        # early stop
        return original.iloc[0:0]  # empty

    max_idx = max(idx_list)

    if max_idx > (len(original) - 1):
        max_idx_pos = idx_list.index(max_idx)
        idx_list = idx_list[:max_idx_pos]

    out = original.iloc[idx_list]
    return out


def distinct(left, right, subset=None):
    """Get distinct rows between dataframes.

    Parameters
    ----------
    left : pandas.DataFrame
    righ : pandas.DataFrame
    subset : iterable

    Returns
    -------
    left_diff, right_diff : pandas.DataFrame

    Examples
    --------

    >>> left = pd.DataFrame([
    ...     [1, 2, 3],
    ...     [1, 2, 33]  # here's the diff
    ... ])

    >>> right = pd.DataFrame([
    ...     [1, 2, 3],
    ...     [1, 2, 3]   # here's the diff
    ... ])

    >>> left_diff, right_diff = pd.distinct(left, right)

    >>> left_obtained
       0  1   2
    1  1  2  33

    >>> right_obtained
       0  1  2
    1  1  2  3

    """
    # for the sake of efficiency
    right_dict = defaultdict(list)
    left_dict = defaultdict(list)

    if subset is not None:
        left_gen = left[subset].itertuples(index=False, name="left")
        right_gen = right[subset].itertuples(index=False, name="right")
        fillvalue = None
    else:
        left_gen = left.itertuples(index=False, name="left")
        right_gen = right.itertuples(index=False, name="right")
        fillvalue = None

    union_gen = zip_longest(left_gen, right_gen, fillvalue=fillvalue)

    for i, (left_row, right_row) in enumerate(union_gen):

        # if both rows are equal, they cancel out
        if left_row == right_row:
            continue

        # IF THEY ARE DIFFERENT:
        # - if already seen, the number of the opposite set is reduced.
        # - if unseen, increase your own number
        _update_key_counter(i, right_row, right_dict, left_dict)
        _update_key_counter(i, left_row, left_dict, right_dict)

    out_left = dict2dataframe(left_dict, left)
    out_right = dict2dataframe(right_dict, right)

    return out_left, out_right


def build_freq_rows_pivot(left, right, subset):
    # add label of set source
    left["source"] = "A"
    right["source"] = "B"
    # build rows frequency
    _all = pd.concat([left, right], axis=0)
    _all["n"] = 1
    freq_table = _all.pivot_table(
        columns=["source"], index=subset, aggfunc="count"
    )
    a_subs_b = freq_table["n"].fillna(0).eval("A - B").to_frame("AsubsB")
    return a_subs_b


def build_freq_rows_unstack(left, right, subset):
    # add label of set source
    left["source"] = "A"
    right["source"] = "B"

    _all = pd.concat([left, right], axis=0)
    _all["n"] = 1
    freq_table = _all.groupby(subset + ["source"]).count().unstack(-1)
    a_subs_b = freq_table["n"].fillna(0).eval("A - B").to_frame("AsubsB")
    return a_subs_b


def repeat_rows_map(df):
    # counter to dataframe
    from functools import reduce
    from operator import add
    _list = map(lambda row: [row[0]] * int(row[1]), df.itertuples())
    rows = reduce(add, _list)
    out = pd.DataFrame(rows)
    return out


def repeat_rows_for(df):
    # counter to dataframe
    rows = []
    for i, n in df.itertuples():
        rows.extend([i] * int(n))
    out = pd.DataFrame(rows)
    return out


def distinct_pandas(left, right, subset=None):
    """Get distinct rows.

    Parameters
    ----------
    left : pandas.DataFrame
    right : pandas.DataFrame
    subset : list

    Returns
    -------
    left_only, right_only : pandas.DataFrame
    """

    # copy is mandatory
    left = left.copy()
    right = right.copy()

    a_subs_b = build_freq_rows_pivot(left, right)

    a_distinct = a_subs_b.query("AsubsB>0")
    b_distinct = (-a_subs_b).query("AsubsB>0")  # NOBUG: we need possitve freqs

    a_distinct_df = repeat_rows_for(a_distinct)
    b_distinct_df = repeat_rows_for(b_distinct)

    return a_distinct_df, b_distinct_df


def distinct_pandas_unstack(left, right, subset=None):
    """Get distinct rows.

    Parameters
    ----------
    left : pandas.DataFrame
    right : pandas.DataFrame
    subset : list

    Returns
    -------
    left_only, right_only : pandas.DataFrame
    """

    # copy is mandatory
    left = left.copy()
    right = right.copy()

    a_subs_b = build_freq_rows_unstack(left, right, subset)

    # counter
    a_distinct = a_subs_b.query("AsubsB>0")
    b_distinct = (-a_subs_b).query("AsubsB>0")  # NOBUG: we need positive freqs

    # dataframe
    a_distinct_df = repeat_rows_map(a_distinct)
    b_distinct_df = repeat_rows_map(b_distinct)

    return a_distinct_df, b_distinct_df


def distinct_counter(left, right, subset=None):
    """Get distinct rows.

    Parameters
    ----------
    left : pandas.DataFrame
    right : pandas.DataFrame
    subset : list

    Returns
    -------
    left_only, right_only : pandas.DataFrame
    """

    if subset is not None:
        left_gen = left[subset].itertuples(index=False, name="left")
        right_gen = right[subset].itertuples(index=False, name="right")
    else:
        left_gen = left.itertuples(index=False, name="left")
        right_gen = right.itertuples(index=False, name="right")

    left_dict = Counter(left_gen)
    right_dict = Counter(right_gen)

    _out_left = left_dict - right_dict
    _out_right = right_dict - left_dict

    out_left = pd.DataFrame(_out_left.elements(), columns=left.columns)
    out_right = pd.DataFrame(_out_right.elements(), columns=right.columns)

    return out_left, out_right


def distinct_merge(left, right, subset=None):
    """Get distinct rows.

    Parameters
    ----------
    left : pandas.DataFrame
    right : pandas.DataFrame
    subset : list

    Returns
    -------
    left_only, right_only : pandas.DataFrame
    """
    _left = left.copy()
    _right = right.copy()

    if subset is not None:
        _left = _left.loc[:, subset]
        _left = _left.loc[:, subset]

    _left.index.name = 'index'
    _right.index.name = 'index'
    _left = _left.reset_index()
    _right = _right.reset_index()

    # create positional reference by each index as it can be repeated.
    # e.g.: ['a', 'a', 'a', 'b'] --> a: [0, 1, 2], b: [0] --> [0, 1, 2, 0]
    def range_like(x):
        return range(len(x))

    pos = 'posix'
    _left[pos] = _left.groupby(_left.index).apply(range_like).explode()
    _right[pos] = _right.groupby(_right.index).apply(range_like).explode()

    outer_join = _left.merge(_right, how='outer', indicator=True)
    drop_col = ['_merge', pos]
    merge = outer_join._merge
    lonly = outer_join[(merge == 'left_only')].drop(drop_col, axis=1)
    ronly = outer_join[(merge == 'righ_only')].drop(drop_col, axis=1)

    lonly = lonly.set_index('index')
    ronly = ronly.set_index('index')
    # TODO: return the original df columns and index
    # out_left = left.loc[lonly.index]
    # out_righ = righ.loc[ronly.index]

    return lonly, ronly
