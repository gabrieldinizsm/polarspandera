import pandera.polars as pa


class FlightSchema():

    schema = pa.DataFrameSchema(
        columns={
            'FLIGHT_DATE': pa.Column(dtype=pa.Date)
        }
    )
