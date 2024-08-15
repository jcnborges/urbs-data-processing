from pyspark.sql import DataFrame

from .sparketl import ETLSpark
import pyspark.sql.functions as F


class MySQLDataProcess:

    def __init__(self, year, month, day):
        self.etlspark = ETLSpark()
        self.year = year
        self.month = month
        self.day = day

    def __call__(self, *args, **kwargs):
        self.perform()

    def perform(self):

        etl_line = self.etl_line()
        self.save(etl_line.select("line_code", "line_name", "service_category", "color"), "/var/lib/mysql-files/etl_line")

        etl_itinerary = self.etl_itinerary()
        self.save(etl_itinerary.select("line_code", "id", "name", "latitude", "longitude", "type", "itinerary_id", "line_way", "next_stop_id", "next_stop_delta_s", "seq"), "/var/lib/mysql-files/etl_itinerary")

        etl_event = self.etl_event()
        self.save(etl_event.select("line_code", "vehicle", "id", "itinerary_id", "event_timestamp", "seq"), "/var/lib/mysql-files/etl_event")

    def etl_line(self) -> DataFrame:
        return (self.etlspark.sqlContext.read.option("mergeSchema", "true").parquet("/data/refined/bus_lines")
                .filter(f"year =='{self.year}' and month=='{self.month}' and day=='{self.day}'")
                .distinct())

    def etl_itinerary(self) -> DataFrame:
        return (self.etlspark.sqlContext.read.option("mergeSchema", "true").parquet("/data/refined/bus_itineraries")
                .filter(f"year =='{self.year}' and month=='{self.month}' and day=='{self.day}'")
                .distinct())

    def etl_event(self) -> DataFrame:
        return (self.etlspark.sqlContext.read.option("mergeSchema", "true").parquet("/data/refined/bus_tracking")
                .filter(f"year =='{self.year}' and month=='{self.month}' and day=='{self.day}'")
                .distinct())

    @staticmethod
    def save(df: DataFrame, output: str):
        (df.coalesce(1).write.mode('overwrite').option("header", False)
         .format("csv").save(output))