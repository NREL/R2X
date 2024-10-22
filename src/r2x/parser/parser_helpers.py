"""Set of helper functions for parsers."""
# ruff: noqa

from typing import Any
import polars as pl
import numpy as np
import cvxpy as cp

from infrasys.function_data import QuadraticFunctionData, PiecewiseLinearData, XYCoords


def field_filter(
    property_fields: dict[str, Any], eligible_fields: set[str]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Filters a dictionary of property fields into valid and extra fields based on eligibility.

    Parameters
    ----------
    property_fields : dict
        Dictionary of property fields where keys are field names and values are field values.
    eligible_fields : set
        Set of field names that are considered valid (eligible).

    Returns
    -------
    tuple of dict
        A tuple of two dictionaries:
        - valid : dict
            Contains fields that are both in `property_fields` and `eligible_fields`, and are not `None`.
        - extra : dict
            Contains fields that are in `property_fields` but not in `eligible_fields`, and are not `None`.

    Examples
    --------
    >>> property_fields = {"field1": 10, "field2": None, "field3": "hello"}
    >>> eligible_fields = {"field1", "field2"}
    >>> valid, extra = field_filter(property_fields, eligible_fields)
    >>> valid
    {'field1': 10}
    >>> extra
    {'field3': 'hello'}
    """
    valid = {k: v for k, v in property_fields.items() if k in eligible_fields if v is not None}
    extra = {k: v for k, v in property_fields.items() if k not in eligible_fields if v is not None}

    return valid, extra


def prepare_ext_field(valid_fields: dict[str, Any], extra_fields: dict[str, Any]) -> dict[str, Any]:
    """Clean the extra fields by removing any time series data and adds the cleaned extra fields to `valid_fields`.

    Parameters
    ----------
    valid_fields : dict
        Dictionary containing valid fields.
    extra_fields : dict
        Dictionary containing extra fields that may include data types not needed.

    Returns
    -------
    dict
        Updated valid_fields with cleansed extra fields under the "ext" key.

    Examples
    --------
    >>> valid_fields = {"field1": 10, "field2": "hello"}
    >>> extra_fields = {"field3": [1, 2, 3], "field4": 42}
    >>> result = prepare_ext_field(valid_fields, extra_fields)
    >>> result
    {'field1': 10, 'field2': 'hello', 'ext': {'field4': 42}}
    """
    if extra_fields:
        # Filter to only include eligible data types
        eligible_datatypes = [str, int, float, bool]
        extra_fields = {k: v for k, v in extra_fields.items() if type(v) in eligible_datatypes}
        valid_fields["ext"] = extra_fields
    else:
        valid_fields["ext"] = {}
    return valid_fields


def handle_leap_year_adjustment(data_file: pl.DataFrame) -> pl.DataFrame:
    """Duplicate February 28th to February 29th for leap years.

    Parameters
    ----------
    data_file : pl.DataFrame
        DataFrame containing timeseries data.

    Returns
    -------
    pl.DataFrame
        DataFrame adjusted for leap years.

    Examples
    --------
    >>> df = pl.DataFrame({"date": ["2020-02-28"], "value": [1]})
    >>> handle_leap_year_adjustment(df)
    """
    feb_28 = data_file.slice(1392, 24)
    before_feb_29 = data_file.slice(0, 1416)
    after_feb_29 = data_file.slice(1416, len(data_file) - 1440)
    return pl.concat([before_feb_29, feb_28, after_feb_29])


def fill_missing_timestamps(data_file: pl.DataFrame, hourly_time_index: pl.DataFrame) -> pl.DataFrame:
    """Add missing timestamps to data and forward fill nulls to complete a year.

    Parameters
    ----------
    data_file : pl.DataFrame
        DataFrame containing timeseries data.
    hourly_time_index : pl.DataFrame
        DataFrame containing the hourly time index for the study year.

    Returns
    -------
    pl.DataFrame
        DataFrame with missing timestamps filled.

    Examples
    --------
    >>> df = pl.DataFrame({"year": [2020], "month": [2], "day": [28], "hour": [0], "value": [1]})
    >>> hourly_index = pl.DataFrame({"datetime": pl.date_range("2020-01-01", "2020-12-31", freq="1H")})
    >>> fill_missing_timestamps(df, hourly_index)
    """
    if "hour" in data_file.columns:
        data_file = data_file.with_columns(
            pl.datetime(pl.col("year"), pl.col("month"), pl.col("day"), pl.col("hour"))
        )
    if "day" not in data_file.columns:
        data_file = data_file.with_columns(pl.datetime(pl.col("year"), pl.col("month"), 1))  # First day
    else:
        data_file = data_file.with_columns(pl.datetime(pl.col("year"), pl.col("month"), pl.col("day")))

    upsample_data = hourly_time_index.join(data_file, on="datetime", how="left")
    upsample_data = upsample_data.fill_null(strategy="forward")
    return upsample_data


def resample_data_to_hourly(data_file: pl.DataFrame) -> pl.DataFrame:
    """Resample data to hourly frequency from minute data.

    Parameters
    ----------
    data_file : pl.DataFrame
        DataFrame containing timeseries data with minute intervals.

    Returns
    -------
    pl.DataFrame
        DataFrame resampled to hourly frequency.

    Examples
    --------
    >>> df = pl.DataFrame(
    ...     {
    ...         "year": [2020, 2020, 2020, 2020],
    ...         "month": [2, 2, 2, 2],
    ...         "day": [28, 28, 28, 28],
    ...         "hour": [0, 0, 1, 1],
    ...         "minute": [0, 30, 0, 30],  # Minute-level data
    ...         "value": [1, 2, 3, 4],
    ...     }
    ... )
    >>> resampled_data = resample_data_to_hourly(df)
    >>> resampled_data.shape[0]
    # Expecting two rows: one for hour 0 and one for hour 1
    """
    # Create a timestamp from year, month, day, hour, and minute
    data_file = data_file.with_columns(
        pl.datetime(
            data_file["year"],
            data_file["month"],
            data_file["day"],
            hour=data_file["hour"],
            minute=data_file["minute"],
        ).alias("timestamp")
    )

    # Group by the hour and aggregate the values
    return (
        data_file.group_by_dynamic("timestamp", every="1h")
        .agg([pl.col("value").mean().alias("value")])  # Average of values for the hour
        .with_columns(
            pl.col("timestamp").dt.year().alias("year"),
            pl.col("timestamp").dt.month().alias("month"),
            pl.col("timestamp").dt.day().alias("day"),
            pl.col("timestamp").dt.hour().alias("hour"),
            pl.col("value").alias("value"),
        )
        .select(["year", "month", "day", "hour", "value"])
    )


def reconcile_timeseries(data_file: pl.DataFrame, hourly_time_index: pl.DataFrame) -> pl.DataFrame:
    """Adjust timeseries data to match the study year datetime index.

    Parameters
    ----------
    data_file : pl.DataFrame
        The input DataFrame containing the timeseries data to adjust.
    hourly_time_index : pl.DataFrame
        The DataFrame containing the hourly time index for the study year, used for alignment.

    Returns
    -------
    pl.DataFrame
        The adjusted DataFrame after reconciling leap-year or non-leap-year data with the time index.

    Raises
    ------
    AssertionError
        If the provided `hourly_time_index` is empty.

    Notes
    -----
    - If the length of the `data_file` corresponds to a leap year (8784 hours) and the `hourly_time_index` is
      not a leap year, February 29 data is removed.
    - If the length of `data_file` is less than or equal to 8760 hours, missing timestamps are filled to match
      the study year.
    - If the `data_file` contains half-hourly data (17568 or 17520 rows), the data is resampled to an hourly
      frequency.
    """
    assert not hourly_time_index.is_empty()
    leap_year = len(hourly_time_index) == 8784

    if data_file.height in [8784, 8760]:
        if data_file.height == 8784 and not leap_year:
            before_feb_29 = data_file.slice(0, 1416)
            after_feb_29 = data_file.slice(1440, len(data_file) - 1440)
            return pl.concat([before_feb_29, after_feb_29])
        elif data_file.height == 8760 and leap_year:
            return handle_leap_year_adjustment(data_file)
        return data_file

    if data_file.height <= 8760:
        return fill_missing_timestamps(data_file, hourly_time_index)

    if data_file.height in [17568, 17520]:
        return resample_data_to_hourly(data_file)

    return data_file


def construct_pwl_from_quadtratic(fn, mapped_records, num_tranches=6):
    """Given function data of quadratic curve, construct piecewise linear curve with num_tranches tranches."""
    assert isinstance(fn, QuadraticFunctionData), "Input function data must be of type QuadraticFunctionData"
    if isinstance(num_tranches, str):
        num_tranches = int(num_tranches)

    a = fn.quadratic_term
    b = fn.proportional_term
    c = fn.constant_term
    x_min = mapped_records["active_power_limits"].min.magnitude
    x_max = mapped_records["active_power_limits"].max.magnitude

    # Use evenly spaced X values for the tranches
    # Future iteration should accept custom X values for Bid Cost Markup
    x_vals, y_vals = optimize_pwl(a, b, c, x_min, x_max, num_tranches)

    pwl_fn = PiecewiseLinearData(
        points=[XYCoords(x, y) for x, y in sorted(zip(x_vals, y_vals), key=lambda x: x[0])]
    )

    # # Plot the results
    # import matplotlib.pyplot as plt
    # plt.figure(figsize=(8, 6))
    # # Plot the quadratic function
    # x_plot = np.linspace(x_min, x_max, 100)
    # y_quad = a * x_plot**2 + b * x_plot + c
    # plt.plot(x_plot, y_quad, label='Quadratic Function', color='blue', lw=2)
    # plt.plot(x_vals, y_vals, label='Optimized PWL Function', color='red', linestyle='--', marker='o')
    # plt.xlabel('MW')
    # plt.ylabel('MMbtu/hr')
    # plt.title(f'Generator {mapped_records.get("name")} PWL to Quadratic Cost Functions')
    # plt.legend()
    # plt.grid(True)
    # plt.savefig(f'pwl_curves/pwl_optimization_{mapped_records.get("name")}.png')
    # plt.close()

    return pwl_fn


def optimize_pwl(a, b, c, min, max, n_tranches=6):
    y = cp.Variable(n_tranches)
    x_target = np.linspace(min, max, n_tranches)
    y_quad = a * x_target**2 + b * x_target + c
    mse = cp.sum_squares(y - y_quad)

    # Monotonicity constraints
    constraints = [y[i] <= y[i + 1] for i in range(n_tranches - 1)]

    objective = cp.Minimize(mse)
    problem = cp.Problem(objective, constraints)
    problem.solve()

    return x_target, y.value


def _bid_cost_mark_up(fn, mapped_records):
    # TODO(ktehranchi): Implement bid-cost markup
    # First we need to convert whichever type of function we have to a piecewise linear function
    # This PWL function must have X values definted at the mark-up points
    # We can easily modify the mark-up prices by changing the Y values of the PWL function
    # Issue right now is we need to do this for time-varying data but market bid cost isnt implemented
    pass
