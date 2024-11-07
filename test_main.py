import unittest
from main import start_spark, stop_spark, data_process, query, transform_data


class TestDataProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = start_spark("Test Data Processing with PySpark")

        data = [
            ("User1", 10.0, 20.0, 0.0, 500.0, "2023-11-06"),
            ("User2", 15.0, 25.0, 0.0, 600.0, "2023-11-07"),
            ("User3", 20.0, 30.0, 0.0, 400.0, "2023-11-08"),
        ]
        columns = ["UserID", "Latitude", "Longitude", "AllZero", "Altitude", "Date"]
        cls.df = cls.spark.createDataFrame(data, columns)

    @classmethod
    def tearDownClass(cls):
        stop_spark(cls.spark)

    def test_data_process(self):
        processed_df = data_process(self.spark)

        self.assertGreater(
            processed_df.count(), 0, "The processed DataFrame should not be empty."
        )
        self.assertIn(
            "Altitude",
            processed_df.columns,
            "The column 'Altitude' should be present in the DataFrame.",
        )

    def test_query(self):
        result = query(
            """
            SELECT UserID, MAX(Altitude) AS MaxAltitude
            FROM trajectory_data
            GROUP BY UserID
            ORDER BY MaxAltitude DESC
            """,
            self.spark,
            self.df,
            "trajectory_data",
        )

        self.assertIsNotNone(result, "The SQL query result should not be None.")

    def test_transform_data(self):
        transformed_df = transform_data(self.df)

        self.assertIn(
            "Adjusted_Altitude",
            transformed_df.columns,
            "The column 'Adjusted_Altitude' should be present in the transformed DataFrame.",
        )
        self.assertGreater(
            transformed_df.count(),
            0,
            "The transformed DataFrame should not be empty after filtering.",
        )


if __name__ == "__main__":
    unittest.main()
