from .utils import SchemaBuilder

def build():

    builder = SchemaBuilder()

    builder.add("id", "STRING")
    builder.add("timestamp", "TIMESTAMP")
    builder.add("timestamp_delta_s", "FLOAT")
    builder.add("distance_m", "FLOAT")
    builder.add("reported_speed_delta_mps", "FLOAT")
    builder.add("reported_course_delta_degrees", "FLOAT")
    builder.add("implied_speed_delta_mps", "FLOAT")
    builder.add("implied_course_delta_degrees", "FLOAT")
    builder.add("local_time_h", "FLOAT")
    builder.add("length_of_day_h", "FLOAT")
    builder.add("distance_from_shore_km", "FLOAT")
    builder.add("distance_to_prev_anchorage", "FLOAT")
    builder.add("distance_to_next_anchorage", "FLOAT")
    builder.add("time_to_prev_anchorage", "FLOAT")
    builder.add("time_to_next_anchorage", "FLOAT")

    return builder.schema
