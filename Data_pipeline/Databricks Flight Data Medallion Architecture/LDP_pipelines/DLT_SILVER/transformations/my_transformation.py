import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Bookings Bronze Staging Table
@dlt.table(
    name="stage_bookings"
)
def stage_bookings():
    df= spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df   

# Bookings Transformation View
@dlt.table(
    name="trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("stage_bookings") 
    df = df.withColumn("amount", col("amount").cast(DoubleType())) \
        .withColumn("modifiedDate", current_timestamp()) \
        .withColumn("booking_date", to_date(col("booking_date"))) \
        .drop("_rescued_data")
    return df 
# Expectation Rules
rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

# Silver Bookings Table with Expectations
@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df= spark.readStream.table("trans_bookings")
    return df

#----------------------------------------------------------

# Flights Transformation View
@dlt.view(
    name="trans_flights"
)
def trans_flights():
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/flight/data/")
    df = df.drop("_rescued_data")\
          .withColumn("modifiedDate", current_timestamp())   
    return df

# Create Silver Flights Table
dlt.create_streaming_table("silver_flights")

# CDC Flow for Flights
dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("flight_id"),
    stored_as_scd_type=1
)


#----------------------------------------------------------

# Passengers Transformation View
@dlt.view(
    name="trans_passengers"
)
def trans_passengers():
    df= spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
    df = df.drop("_rescued_data")\
          .withColumn("modifiedDate", current_timestamp())   
    return df 

# Create Silver Passengers Table
dlt.create_streaming_table("silver_passengers")

# CDC Flow for Passengers
dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    sequence_by=col("passenger_id"),
    stored_as_scd_type=1
)

#----------------------------------------------------------


# Airports Transformation View
@dlt.view(
    name="trans_airports"
)
def trans_airports():
    df= spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    df = df.drop("_rescued_data")\
          .withColumn("modifiedDate", current_timestamp())   
    return df 

# Create Silver Airports Table
dlt.create_streaming_table("silver_airports")

# CDC Flow for Airports
dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by=col("airport_id"),
    stored_as_scd_type=1
)

#----------------------------------------------------------


# Silver Business View
@dlt.table(
    name="silver_business"
)
def silver_business():
    df = dlt.readStream("silver_bookings")\
        .join(dlt.readStream("silver_flights"),["flight_id"])\
        .join(dlt.readStream("silver_passengers"), ["passenger_id"])\
        .join(dlt.readStream("silver_airports"),["airport_id"])\
        .drop("modifiedDate")
    return df


@dlt.table(
    name="silver_business_mat"
)
def silver_business_mat():
    df = dlt.read("silver_business")
    return df
