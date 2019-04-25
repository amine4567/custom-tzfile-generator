import argparse

import pandas as pd
from pytz.tzfile import build_tzinfo


def tzfile_to_df(bin_file):
    tz = build_tzinfo("zone", bin_file)

    df_columns = [
        "transition_year_utc",
        "transition_month_utc",
        "transition_day_utc",
        "transition_hour_utc",
        "transition_minute_utc",
        "transition_second_utc",
        "utc_offset_seconds",
        "dst_bool",
        "tzname",
    ]
    df = pd.DataFrame(columns=df_columns)
    df.transition_year_utc = [elt.year for elt in tz._utc_transition_times]
    df.transition_month_utc = [elt.month for elt in tz._utc_transition_times]
    df.transition_day_utc = [elt.day for elt in tz._utc_transition_times]
    df.transition_hour_utc = [elt.hour for elt in tz._utc_transition_times]
    df.transition_minute_utc = [elt.minute for elt in tz._utc_transition_times]
    df.transition_second_utc = [elt.second for elt in tz._utc_transition_times]
    df.utc_offset_seconds = [elt[0].seconds for elt in tz._transition_info]
    df.dst_bool = [elt[1].seconds != 0 for elt in tz._transition_info]
    df.tzname = [elt[2] for elt in tz._transition_info]

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("tzfile")
    args = parser.parse_args()

    with open(args.tzfile, "rb") as f:
        output_df = tzfile_to_df(f)

    output_df.to_csv("{}.csv".format(args.tzfile))
