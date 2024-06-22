import pandera.polars as pa


class FlightSchema():

    schema = pa.DataFrameSchema(
        columns={
            'FLIGHT_DATE': pa.Column(dtype=pa.Date),
            'DEP_DELAY': pa.Column(dtype=pa.Int16),
            'IS_LATE': pa.Column(dtype=pa.Bool),
            'ARR_DELAY': pa.Column(dtype=pa.Int16),
            'AIR_TIME': pa.Column(dtype=pa.Int16),
            'DISTANCE': pa.Column(dtype=pa.Int16),
            'DISTANCE_OVER_TIME': pa.Column(dtype=pa.Int16),
            'DEPARTURE_TIME': pa.Column(dtype=pa.Int16),
            'ARR_TIME': pa.Column(dtype=pa.Int16)
        },
        coerce=True,
        add_missing_columns=True,
        name='Flight Schema'
    )
