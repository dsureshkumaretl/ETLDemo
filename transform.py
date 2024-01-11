from dagster import op, Out, In
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd

TransformedCovi19VaccinationDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedCovi19VaccinationDataFrame",
    columns=[
        PandasColumn.integer_column("original_employee_id", min_value=0),
        PandasColumn.string_column("employee_name", non_nullable=True),
        PandasColumn.string_column("employee_title",  non_nullable=True),
        PandasColumn.datetime_column("birth_date",  non_nullable=True),
        PandasColumn.datetime_column("hire_date",  non_nullable=True),
        PandasColumn.string_column("territory",  non_nullable=True),
        PandasColumn.string_column("region",  non_nullable=True),
        PandasColumn.integer_column("age",  non_nullable=True),
        PandasColumn.integer_column("years_of_service",  non_nullable=True)
    ],
)

TransformedOrdersDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedOrdersDataFrame",
    columns=[
        PandasColumn.integer_column("order_id", min_value=0),
        PandasColumn.string_column("customer_id"),
        PandasColumn.integer_column("employee_id", min_value=0),
        PandasColumn.datetime_column(
            "order_date",
            min_datetime=datetime(year=1996, month=1, day=1),
            max_datetime=datetime(year=1998, month=5, day=6)
        ),
        PandasColumn.datetime_column(
            "required_date",
            min_datetime=datetime(year=1996, month=1, day=1),
            max_datetime=datetime(year=1998, month=12, day=31)
        ),
        PandasColumn.datetime_column(
            "shipped_date",
            non_nullable=False,
            ignore_missing_vals=True
        ),
        PandasColumn.string_column("ship_via"),
        PandasColumn.numeric_column("freight_cost"),
        PandasColumn.string_column("ship_country"),
        PandasColumn.integer_column("product_id", min_value=0),
        PandasColumn.numeric_column("unit_price"),
        PandasColumn.integer_column("quantity", min_value=1),
        PandasColumn.numeric_column("discount"),
        PandasColumn.numeric_column("gross_price"),
        PandasColumn.numeric_column("discount_amount"),
        PandasColumn.numeric_column("net_price"),
        PandasColumn.numeric_column("freight_cost")
    ],
)

TransformedProductsDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedProductsDataFrame",
    columns=[
        PandasColumn.integer_column("original_product_id", min_value=0),
        PandasColumn.string_column("product_name"),
        PandasColumn.string_column("product_category"),
        PandasColumn.boolean_column("stock_level_critical",non_nullable=True),
        PandasColumn.string_column("supplier_name", non_nullable=True),
        PandasColumn.string_column("supplier_city", non_nullable=True),
        PandasColumn.string_column("supplier_country", non_nullable=True)
    ],
)