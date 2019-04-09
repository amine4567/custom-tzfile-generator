import pandas as pd


def to_tzfile(df: pd.DataFrame):
    # magic header
    magic = b"TZif"

    # Decisions
    format_version = b"2"
    leapcnt = 0

    transitions = pd.to_datetime(
        df[["year", "month", "day", "hour", "minute", "second"]].drop(0)
    ).tolist()
    transitions = [elt.to_pydatetime() for elt in transitions]
    # TODO: Convert pandas timestamps to datetime.datetime. Then convert transitions Series to list
    # with tolist() method

    print(transitions)


if __name__ == "__main__":
    df_paris = pd.read_csv("paris_custom_format.csv")
    to_tzfile(df_paris)
