from pathlib import Path
from typing import List, Optional, Union
import pandas as pd

import numpy as np
from gluonts.dataset.arrow import ArrowWriter


def convert_to_arrow(
    path: Union[str, Path],
    time_series: Union[List[np.ndarray], np.ndarray],
    start_times: Optional[Union[List[np.datetime64], np.ndarray]] = None,
    compression: str = "lz4",
):
    if start_times is None:
        # Set an arbitrary start time
        start_times = [np.datetime64("2000-01-01 00:00", "s")] * len(time_series)

    assert len(time_series) == len(start_times)

    dataset = [
        {"start": start, "target": ts} for ts, start in zip(time_series, start_times)
    ]
    ArrowWriter(compression=compression).write_to_file(
        dataset,
        path=path,
    )


if __name__ == "__main__":
    df = pd.read_csv('./BTC-USD.csv')
    df['Date'] = pd.to_datetime(df['Date'])
    time_series = [df['Close'].values]
    start_times = [np.datetime64(df['Date'].iloc[0])]

    # Convert to GluonTS arrow format
    convert_to_arrow("./BTC-USD.arrow", time_series=time_series, start_times=start_times)