from struct import pack, calcsize
import argparse

import pytz

import pandas as pd


def csv_to_tzfile(df: pd.DataFrame):
    # magic header
    magic = b"TZif"

    # Decisions
    format_version = b"2"
    leapcnt = 0

    # transitions
    df = df.rename(
        columns={
            "transition_year_utc": "year",
            "transition_month_utc": "month",
            "transition_day_utc": "day",
            "transition_hour_utc": "hour",
            "transition_minute_utc": "minute",
            "transition_second_utc": "second",
        }
    )
    transitions = pd.to_datetime(
        df[["year", "month", "day", "hour", "minute", "second"]].drop(0)
    ).tolist()
    transitions = [
        int(pytz.timezone("UTC").localize(elt.to_pydatetime()).timestamp())
        for elt in transitions
    ]

    # ttinfo
    ttinfo_all = df.apply(
        lambda x: (x.utc_offset_seconds, x.dst_bool, x.tzname), axis=1
    )
    ttinfo = ttinfo_all.unique().tolist()

    # lindexes
    lindexes = [ttinfo.index(elt) for elt in ttinfo_all[1:]]

    # tznames
    tznames_keys = df.tzname.unique().tolist()

    tznames_vals = [0]
    for elt in tznames_keys:
        tznames_vals.append(tznames_vals[-1] + len(elt) + 1)
    tznames = dict(zip(tznames_keys, tznames_vals))

    # ttinfo_raw
    ttinfo_raw = []
    for elt in ttinfo:
        ttinfo_raw.append(elt[0])
        ttinfo_raw.append(int(elt[1]))
        ttinfo_raw.append(tznames[elt[2]])

    # ttinfo_raw = [str(elt) for elt in ttinfo_raw]
    # tznames_raw
    tznames_raw = bytes(("\x00".join(tznames_keys) + "\x00").encode("ASCII"))

    # cnts
    timecnt = len(transitions)
    typecnt = len(ttinfo_raw) // 3
    charcnt = len(tznames_raw)

    ttisgmtcnt = 0  # TODO: handle not zero case
    ttisstdcnt = 0  # TODO: handle not zero case
    # data
    data = transitions + lindexes + ttinfo_raw + [tznames_raw]

    # pack data
    data_fmt = ">%(timecnt)dl %(timecnt)dB %(ttinfo2)s %(charcnt)ds" % dict(
        timecnt=timecnt, ttinfo2="lBB" * typecnt, charcnt=charcnt
    )
    data_packed = pack(data_fmt, *data)

    # pack header
    head_fmt = ">4s c 15x 6l"
    header_packed = pack(
        head_fmt,
        magic,
        format_version,
        ttisgmtcnt,
        ttisstdcnt,
        leapcnt,
        timecnt,
        typecnt,
        charcnt,
    )

    # to disk
    final_pack = header_packed + data_packed
    return final_pack  # bytes


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file")
    args = parser.parse_args()

    df = pd.read_csv(args.csv_file)
    tzfile_bytes = csv_to_tzfile(df)

    with open("{}.tzf".format(args.csv_file), "wb") as fp:
        fp.write(tzfile_bytes)
