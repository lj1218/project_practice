"""
Big data processing.

Author: lj1218
Date  : 2018-12-08
===========================================
条件：
1、x,y 从B列取值；z从A列取值；
2、z > y > x.

求解：
1、精确到小数点后2位，求 (z-y)/(y-x)；
2、出现频率最高的比值，列出所对应的x,y,z组合.
===========================================
"""
import os
import sys
import time
from functools import partial, wraps

import pandas as pd

from tool import Timer


def get_time():
    """Get current system time."""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def print_msg(msg):
    """Print message."""
    print('[{}] '.format(get_time()) + msg)
    sys.stdout.flush()  # flush buffers


def gen_org_data(out_file, out_index='x,y,z',
                 xlsx_file='data.xlsx', print_n=10000):
    """Generate original data for calculation."""
    xlsx = pd.ExcelFile(xlsx_file)
    df = pd.read_excel(xlsx, 'Sheet1')
    z_set = df['z'].dropna().sort_values()
    x_y_set = df['y,x'].dropna().sort_values()

    print_msg('start generating original data...')
    timer = Timer()
    with open(out_file, 'w', encoding='utf-8') as f:
        __gen_org_data(z_set, x_y_set, f, out_index, print_n)
    print_msg('finish generating original data ({}s)'.format(timer.elapse(2)))


def __gen_org_data(z_set, x_y_set, fh, out_index, print_n):
    fh.write(out_index + '\n')
    count = 0
    for z in z_set:
        for y in x_y_set:
            if y >= z:
                break
            for x in x_y_set:
                if x >= y:
                    break
                count += 1
                fh.write('{},{},{}\n'.format(x, y, int(z)))
                if count % print_n == 0:
                    print_msg('count={}'.format(count))
    print_msg('count={}'.format(count))
    print_msg('total={}'.format(len(x_y_set) * len(x_y_set) * len(z_set)))


def profiled(func=None, *, total=None, print_n=10, name='n'):
    """Decorate function call times and print some message."""
    if func is None:
        return partial(profiled, total=total, print_n=print_n, name=name)

    n_calls = 0

    @wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal n_calls
        n_calls += 1
        if n_calls % print_n == 0:
            if total is None:
                print_msg('{}={}'.format(name, n_calls))
            else:
                print_msg('{}={} ({})'.format(
                    name, n_calls, round(n_calls/total, 3))
                )
        return func(*args, *kwargs)

    wrapper.n_calls = lambda: n_calls
    return wrapper


def do_calc(df, result_col='ratio', ndigits=2, print_n=100000):
    """Do calculation."""
    @profiled(total=len(df), print_n=print_n, name='calc_counter')
    def calc(s):
        x = s['x']
        y = s['y']
        z = s['z']
        return round((z - y) / (y - x), ndigits)

    print_msg('start calculation...')
    print_msg('total rows = {}'.format(len(df)))
    timer = Timer()
    ratios = df.apply(calc, axis=1)
    print_msg('finish calculation ({}s)'.format(timer.elapse(2)))

    print_msg('start appending result column...')
    timer.reset()
    df[result_col] = ratios
    print_msg('finish appending result column ({}s)'.format(timer.elapse(2)))
    return df


def find_max_occur(groups, print_n=10000):
    """Find max occurred results from groups."""
    @profiled(total=len(groups), print_n=print_n, name='find_max_counter')
    def _find_max_occur(grp, max_occur, rs):
        number = len(grp[1])
        if number > max_occur:
            rs = [grp]
            max_occur = number
        elif number == max_occur:
            rs.append(grp)
        return rs, max_occur

    max_ = 0
    result = []
    print_msg('start finding max occur...')
    print_msg('total groups = {}'.format(len(groups)))
    timer = Timer()
    for group in groups:
        result, max_ = _find_max_occur(group, max_, result)
    print_msg('finish finding max occur ({}s)'.format(timer.elapse(2)))
    return result


def save_result(rs, out_dir, print_n=10):
    """Save result."""
    @profiled(total=len(rs), print_n=print_n, name='save_rs_counter')
    def _save_result(r, directory):
        filename = os.path.join(directory, 'result_{}.csv'.format(r[0]))
        df = r[1]
        del df['ratio']
        df.to_csv(filename, index=False)

    print_msg('start saving result...')
    print_msg('total results = {}'.format(len(rs)))
    timer = Timer()
    for x in rs:
        _save_result(x, out_dir)
    print_msg('finish saving result ({}s)'.format(timer.elapse(2)))


def main():
    """Entry."""
    src_dir = 'data'
    out_dir = 'result'
    result_dir = os.path.join(out_dir, 'max_occur')
    src_file = os.path.join(src_dir, 'data.xlsx')
    org_data_file = os.path.join(src_dir, 'org_data.csv')
    sorted_data_file = os.path.join(out_dir, 'data_sorted.csv')
    save_data_sorted = False

    print_msg('start running...')
    main_timer = Timer()

    if not os.path.exists(org_data_file):
        gen_org_data(out_file=org_data_file,
                     xlsx_file=src_file, print_n=100000)

    print_msg('start loading org data...')
    timer = Timer()
    df = pd.read_csv(org_data_file)
    print_msg('finish loading org data ({}s)'.format(timer.elapse(2)))

    do_calc(df, ndigits=2, print_n=100000)

    print_msg('start sorting values...')
    timer.reset()
    data_sorted = df.sort_values(by='ratio')
    print_msg('finish sorting values ({}s)'.format(timer.elapse(2)))

    if save_data_sorted:
        print_msg('start saving sorted data to file...')
        timer.reset()
        data_sorted.to_csv(sorted_data_file, index=False)
        print_msg('finish saving sorted data to file ({}s)'.format(
            timer.elapse(2)))

    print_msg('start grouping data...')
    timer.reset()
    grouped = data_sorted.groupby('ratio')
    print_msg('finish grouping data ({}s)'.format(timer.elapse(2)))

    result = find_max_occur(grouped, print_n=10000)
    save_result(result, result_dir, print_n=10)

    print_msg('finish running ({}s)'.format(main_timer.elapse(2)))


if __name__ == '__main__':
    main()
