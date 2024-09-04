from pyspark.sql import DataFrame

from .sparketl import ETLSpark
import pyspark.sql.functions as F
import os
import glob
import shutil
import subprocess

class MySQLDataProcess:

    def __init__(self, year, month, day):
        self.etlspark = ETLSpark()
        self.year = year
        self.month = month
        self.day = day

    def __call__(self, *args, **kwargs):
        self.perform()

    def perform(self):

        # Data ingestion

        etl_line = self.etl_line()
        self.save(etl_line.select("line_code", "line_name", "service_category", "color"), "/var/lib/mysql-files/etl_line")

        etl_itinerary = self.etl_itinerary()
        self.save(etl_itinerary.select("line_code", "id", "name", "latitude", "longitude", "type", "itinerary_id", "line_way", "next_stop_id", "next_stop_delta_s", "seq"), "/var/lib/mysql-files/etl_itinerary")

        etl_event = self.etl_event()
        etl_event = etl_event.select(
            "line_code", 
            "vehicle", 
            "id", 
            "itinerary_id", 
            "event_timestamp", 
            "seq"
        ).withColumn(
            "event_timestamp",
            F.date_format(F.col("event_timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
        )

        self.save(etl_event, "/var/lib/mysql-files/etl_event")

        self.move("etl_line")
        self.move("etl_itinerary")
        self.move("etl_event")

        # Data loading

        # Access environment variables
        host = os.environ.get("MYSQL_HOST", "mysql")  # Default to "mysql" if not set
        port = int(os.environ.get("MYSQL_PORT", 3306)) # Default to 3306
        user = os.environ.get("MYSQL_USER", "root")
        password = os.environ.get("MYSQL_PASSWORD") 
        database = os.environ.get("MYSQL_DATABASE")

        # Execute the MySQL command using subprocess
        # mysql -uroot --password=$pwd -D busanalysis_dw -e "source ./src/sql/bulk_insert.sql"
        command = [
            "mysql",
            f"-h{host}",  # Add host 
            f"-P{str(port)}",  # Add port (convert to string)
            f"-u{user}",
            f"-p{password}",
            f"-D{database}",
            f"-e", "source /opt/urbs-data-processing/dataprocessing/sql/bulk_insert.sql" 
        ]
        print(command)
        # Replace the file path with your actual script path.

        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # Print output or handle errors
        print(stdout.decode("utf-8"))
        if stderr:
            print(stderr.decode('utf-8'))  

        # mysql -uroot --password=$pwd -D busanalysis_dw -e "call busanalysis_dw.sp_load_all('$1');"
        # Execute the MySQL command using subprocess
        # mysql -uroot --password=$pwd -D busanalysis_dw -e "source ./src/sql/bulk_insert.sql"
        command = [
            "mysql",
            f"-h{host}",  # Add host 
            f"-P{str(port)}",  # Add port (convert to string)
            f"-u{user}",
            f"-p{password}",
            f"-D{database}",
            f"-e", f"call busanalysis_dw.sp_load_all('{self.year}-{self.month:02}-{self.day:02}');" 
        ]
        print(command)
        # Replace the file path with your actual script path.

        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # Print output or handle errors
        print(stdout.decode("utf-8"))
        if stderr:
            print(stderr.decode('utf-8'))                             

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
        
    @staticmethod
    def move(filename: str):
        dir = f"/var/lib/mysql-files/{filename}"
        csv_files = glob.glob(f"{dir}/*.csv")
        # Ensure only one file is found
        if len(csv_files) != 1:
            raise ValueError(f"Expected exactly one CSV file in the etl_line directory, found: {len(csv_files)}")
        # Move and rename the file
        source_path = csv_files[0]
        destination_path = f"/var/lib/mysql-files/{filename}.csv"
        os.rename(source_path, destination_path)

        # Remove the original directory
        shutil.rmtree(dir)