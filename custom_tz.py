from struct import pack, calcsize

import pytz

import pandas as pd


def to_tzfile(df: pd.DataFrame):
    # magic header
    magic = b"TZif"

    # Decisions
    format_version = b"2"
    leapcnt = 0

    # transitions
    transitions = pd.to_datetime(
        df[["year", "month", "day", "hour", "minute", "second"]].drop(0)
    ).tolist()
    transitions = [int(pytz.timezone('UTC').localize(elt.to_pydatetime()).timestamp())
                   for elt in transitions]

    # ttinfo
    ttinfo_all = df.apply(lambda x: (x.utc_offset_seconds, x.dst_bool, x.tzname), axis=1)
    ttinfo=ttinfo_all.unique().tolist()

    # lindexes
    lindexes = [ttinfo.index(elt) for elt in ttinfo_all[1:]]

    # tznames
    tznames_keys = df.tzname.unique().tolist()

    tznames_vals=[0]
    for elt in tznames_keys:
        tznames_vals.append(tznames_vals[-1]+len(elt)+1)
    tznames = dict(zip(tznames_keys, tznames_vals))

    # ttinfo_raw
    ttinfo_raw = []
    for elt in ttinfo:
        ttinfo_raw.append(elt[0])
        ttinfo_raw.append(int(elt[1]))
        ttinfo_raw.append(tznames[elt[2]])

    # ttinfo_raw = [str(elt) for elt in ttinfo_raw]
    # tznames_raw
    tznames_raw = bytes(("\x00".join(tznames_keys)+"\x00").encode("ASCII"))

    # cnts
    timecnt = len(transitions)
    typecnt = len(ttinfo_raw)//3
    charcnt = len(tznames_raw)

    ttisgmtcnt = typecnt
    ttisstdcnt = typecnt
    # data
    data = transitions + lindexes + ttinfo_raw + [tznames_raw]

    # pack data
    data_fmt = '>%(timecnt)dl %(timecnt)dB %(ttinfo2)s %(charcnt)ds' % dict(
        timecnt=timecnt, ttinfo2='lBB' * typecnt, charcnt=charcnt)
    data_packed = pack(data_fmt, *data)

    # pack header
    head_fmt = '>4s c 15x 6l'
    header_packed = pack(head_fmt, magic, format_version, ttisgmtcnt, ttisstdcnt, leapcnt, timecnt, typecnt, charcnt)
    
    # to disk
    final_pack = header_packed+data_packed
    return final_pack #bytes


if __name__ == "__main__":
    df_paris = pd.read_csv("custom_format.csv")
    tzfile_bytes = to_tzfile(df_paris)

    with open("custom_tzfile", "wb") as fp:
        fp.write(tzfile_bytes)