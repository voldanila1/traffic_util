import os
import pathlib
from datetime import datetime
from collections import defaultdict, namedtuple
from operator import attrgetter
import argparse


SEP = ","  # items separator

INPUT_FILE_MASK = '*.csv'

BITS_TO_TBYTES = 8*1000*1000*1000*1024  # wrong but close to grafana


Record = namedtuple('Record', ['datetime', 'out_http', 'out_https'])

OUT_HTTP = 'OUT_HTTP'
OUT_HTTPS = 'OUT_HTTPS'


def make_rec(line):
    try:
        time, _in, out_http, out_https = line.strip().split(SEP)
        return Record(
            datetime.strptime(time, '%Y-%m-%d %H:%M:%S'),
            abs(float(out_http)),
            abs(float(out_https))
        )
    except ValueError:
        pass


def get_dx(dt1, dt2):
    # diff in seconds between two datetimes
    return (dt2-dt1).total_seconds()


def trapezoid_area(values, dx):
    return (sum(values) + sum(values[1:-1])) / 2 * dx


def traffic_utilization(input_files, out_filename):
    traf = defaultdict(dict)
    traf_col_names = set()

    for in_file in input_files:
        print(in_file)

        curr_name = in_file.stem.strip()  # current column name = current file name
        traf_col_names.add(curr_name)

        # values grouped by date and series
        dataset = defaultdict(lambda: defaultdict(list))

        curr_dx = None

        num_recs = 0

        with open(in_file) as f:
            f.readline()  # skip header

            d1 = None
            for line in f:
                rec = make_rec(line)

                if rec:
                    dataset[rec.datetime.date()][OUT_HTTP].append(rec.out_http)
                    dataset[rec.datetime.date()][OUT_HTTPS].append(rec.out_https)

                    if not curr_dx:
                        if not d1:
                            d1 = rec.datetime
                        elif d1 and rec.datetime > d1:
                            curr_dx = get_dx(d1, rec.datetime)  # determine dx for current file

                    num_recs += 1

        if not num_recs:
            continue  # no records loaded. process next file
        elif not curr_dx:
            continue  # couldn't determine dx. process next file

        # sort date keys if for some weird reason dates weren't ordered in file
        dataset__date_keys = sorted(list(dataset))

        # steal first value from next date in series
        if len(dataset__date_keys) > 1:
            prev_date = dataset__date_keys[0]
            for curr_date in dataset__date_keys[1:]:
                if (curr_date - prev_date).days != 1:
                    continue
                #
                try:
                    value = dataset[curr_date][OUT_HTTP].pop(0)
                    dataset[prev_date][OUT_HTTP].append(value)
                except IndexError as e:
                    pass
                try:
                    value = dataset[curr_date][OUT_HTTPS].pop(0)
                    dataset[prev_date][OUT_HTTPS].append(value)
                except IndexError as e:
                    pass
                #
                prev_date = curr_date

        # calculate traf
        for curr_date in dataset__date_keys:
            traf_http = trapezoid_area(dataset[curr_date][OUT_HTTP], curr_dx)
            traf_https = trapezoid_area(dataset[curr_date][OUT_HTTPS], curr_dx)
            traf[curr_date][curr_name] = (traf_http + traf_https) / BITS_TO_TBYTES

    # result
    with open(out_filename, 'w') as f_out:
        sorted_col_names = sorted(traf_col_names)

        out_header = 'date;%s\n' % ';'.join(sorted_col_names)
        f_out.write(out_header)

        for date, cols in traf.items():
            date_str = date.strftime('%d.%m.%Y')
            # get values in order, convert floats to strings, replace dots with commas for excel compatibility
            values = (str(cols.get(col_name, 0)).replace('.', ',') for col_name in sorted_col_names)
            values_str = ';'.join(values)
            out_str = f'{date_str};{values_str}\n'
            f_out.write(out_str)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NIC traffic utilization. Version 3.")

    parser.add_argument('output', type=str, help='Output file name.')
    parser.add_argument('--input-dir', type=str, default=os.getcwd(), help='Path to the input files that should be processed. The current working dir if not specified.')
    parser.add_argument('--mask', type=str, default=INPUT_FILE_MASK, help=f'Input file mask. Default: "{INPUT_FILE_MASK}".')

    args = parser.parse_args()

    input_files = [p for p in pathlib.Path(args.input_dir).glob(args.mask) if p.is_file()]

    traffic_utilization(input_files, args.output)
