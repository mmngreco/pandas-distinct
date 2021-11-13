import pandas as pd
from pandas_distinct import core
import pytest


def _assert_df(left, right):
    kw = {
        'check_index_type': False,
        'check_dtype': False,
    }
    pd.testing.assert_frame_equal(left[0], left[1], **kw)
    pd.testing.assert_frame_equal(right[0], right[1], **kw)


def test_distinct():
    left = pd.DataFrame([[1, 2, 3], [1, 2, 33]])
    right = pd.DataFrame([[1, 2, 3], [1, 2, 3]])

    out_left, out_right = core.distinct(left, right)

    out_left_expected = pd.DataFrame([[1, 2, 33]], index=[1])
    out_right_expected = pd.DataFrame([[1, 2, 3]], index=[1])

    _assert_df((out_left, out_left_expected), (out_right, out_right_expected))


def test_distinct_1():

    columns = [0, 1, 2]
    left = pd.DataFrame([[1, 2, 3], [1, 2, 3], [1, 2, 33]], columns=columns)
    right = pd.DataFrame([[1, 2, 3], [1, 2, 3]], columns=columns)

    out_left, out_right = core.distinct(left, right)

    out_left_expected = pd.DataFrame([[1, 2, 33]], index=[2], columns=columns)
    out_right_expected = pd.DataFrame([], columns=columns)

    _assert_df((out_left, out_left_expected), (out_right, out_right_expected))


def test_distinct_subset():

    columns = [0, 1, 2]
    left = pd.DataFrame([[1, 2, 3], [1, 2, 3], [1, 2, 33]], columns=columns)
    right = pd.DataFrame([[0, 2, 3], [1, 2, 3]], columns=columns)
    # shouldn't affect  ---^

    out_left, out_right = core.distinct(left, right, subset=[1, 2])

    out_left_expected = pd.DataFrame([[1, 2, 33]], index=[2], columns=columns)
    out_right_expected = pd.DataFrame([], columns=columns)

    _assert_df((out_left, out_left_expected), (out_right, out_right_expected))


def test_distinct_subset_1():

    columns = [0, 1, 2]
    left = pd.DataFrame(
        [[1, 2, 3], [1, 2, 3], [1, 2, 33]],
        index=["a", "a", "b"],
        columns=columns,
    )
    right = pd.DataFrame([[1, 2, 33]], index=["b"], columns=columns)

    out_left, out_right = core.distinct(left, right, subset=[1, 2])

    out_left_expected = pd.DataFrame(
        [[1, 2, 3], [1, 2, 3]], index=["a", "a"], columns=columns
    )
    out_right_expected = pd.DataFrame([], columns=columns, index=[])

    _assert_df((out_left, out_left_expected), (out_right, out_right_expected))


def test_distinct_subset_2():

    columns = [0, 1, 2]
    left = pd.DataFrame(
        [[1, 2, 3], [1, 2, 3], [1, 2, 33]],
        index=["a", "a", "b"],
        columns=columns,
    )
    right = pd.DataFrame(
        [[1, 2, 3], [1, 2, 33]], index=["a", "b"], columns=columns
    )

    out_left, out_right = core.distinct(left, right, subset=[1, 2])

    out_left_expected = pd.DataFrame(
        [[1, 2, 3]],
        index=["a"],
        columns=columns,
    )
    out_right_expected = pd.DataFrame([], columns=columns, index=[])

    _assert_df((out_left, out_left_expected), (out_right, out_right_expected))


def test_distinct_pandas():

    left = pd.DataFrame([[1, 2, 3], [1, 2, 33]])
    right = pd.DataFrame([[1, 2, 3], [1, 2, 3]])

    out_left, out_right = core.distinct_pandas_unstack(
        left, right, subset=[0, 1, 2]
    )


def test_distinct_counter():

    columns = [0, 1, 2]
    left = pd.DataFrame([[1, 2, 3], [1, 2, 33]])
    right = pd.DataFrame([[1, 2, 3], [1, 2, 3]])

    # expected
    out_left_expected = pd.DataFrame([[1, 2, 33]], columns=columns)
    out_right_expected = pd.DataFrame([[1, 2, 3]], columns=columns)

    # obtained
    out_left, out_right = core.distinct_counter(left, right, subset=[0, 1, 2])

    _assert_df((out_left, out_left_expected), (out_right, out_right_expected))


@pytest.mark.xfail(AssertionError, reason="bug: lost original index")
def test_distinct_counter_alt():

    columns = [0, 1, 2]
    left = pd.DataFrame(
        [[1, 2, 3], [1, 2, 3], [1, 2, 33]],
        index=["a", "a", "b"],
        columns=columns,
    )
    right = pd.DataFrame(
        [[1, 2, 3], [1, 2, 33]], index=["a", "b"], columns=columns
    )

    out_left, out_right = core.distinct_merge(left, right)

    out_left_expected = pd.DataFrame(
        [[1, 2, 3]],
        index=["a"],
        columns=columns,
    )
    out_right_expected = pd.DataFrame([], columns=columns, index=[])

    _assert_df((out_left, out_left_expected), (out_right, out_right_expected))
