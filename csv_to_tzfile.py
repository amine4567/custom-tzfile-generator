import argparse
from struct import pack

import pandas as pd
import pytz


def csv_to_tzfile(df: pd.DataFrame) -> bytes:
    # magic header that identifies a tzfile
    magic = b"TZif"

    # not used ?
    format_version = b"2"
    leapcnt = 0

    # transitions : when there is change in the locale time
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

    # ttinfo : info about each transition time
    ttinfo_all = df.apply(
        lambda x: (x.utc_offset_seconds, x.dst_bool, x.tzname), axis=1
    )
    ttinfo = ttinfo_all.unique().tolist()

    # lindexes : indices of local times
    lindexes = [ttinfo.index(elt) for elt in ttinfo_all[1:]]

    # tznames : timezones names
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

    # tznames_raw
    tznames_raw = bytes(("\x00".join(tznames_keys) + "\x00").encode("ASCII"))

    # sizes of different parts of data
    timecnt = len(transitions)
    typecnt = len(ttinfo_raw) // 3
    charcnt = len(tznames_raw)

    ttisgmtcnt = 0  # TODO: handle not zero case
    ttisstdcnt = 0  # TODO: handle not zero case

    # combined data
    data = transitions + lindexes + ttinfo_raw + [tznames_raw]

    # pack data into binary
    data_fmt = ">%(timecnt)dl %(timecnt)dB %(ttinfo2)s %(charcnt)ds" % dict(
        timecnt=timecnt, ttinfo2="lBB" * typecnt, charcnt=charcnt
    )
    data_packed = pack(data_fmt, *data)

    # pack header into binary
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

    final_pack = header_packed + data_packed
    return final_pack


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file")
    args = parser.parse_args()

    csv_filename = args.csv_file
    df = pd.read_csv(csv_filename)
    tzfile_bytes = csv_to_tzfile(df)

    tzfile_name = csv_filename[:-4] if csv_filename.endswith(".csv") else csv_filename

    with open(f"{tzfile_name}.tzf", "wb") as fp:
        fp.write(tzfile_bytes)
