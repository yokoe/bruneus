import unittest
import bruneus
from dotenv import load_dotenv

load_dotenv()


class TestSelect(unittest.TestCase):
    def test_select_with_no_params(self):
        query = "SELECT 1"
        task = bruneus.select(query)
        self.assertEqual(query, task.query)

    def test_select(self):
        query = "SELECT word FROM `bigquery-public-data.samples.shakespeare` order by rand() limit 5"
        words = bruneus.select(query).as_dicts()
        self.assertEqual(len(words), 5)

    def test_select_with_template(self):
        words = (
            bruneus.select_with_template("./tests/templates", "shakespeare.j2")
            .int64_param("max_count", 3)
            .as_dicts()
        )
        self.assertEqual(len(words), 3)

    def test_select_to_dataframe(self):
        query = "SELECT word FROM `bigquery-public-data.samples.shakespeare` order by rand() limit 5"
        df = bruneus.select(query).to_dataframe()
        self.assertEqual(len(df.index), 5)

    def test_select_int64_param(self):
        query = "SELECT word FROM `bigquery-public-data.samples.shakespeare` order by rand() limit @max_count"
        select_task = bruneus.select(query)
        self.assertEqual(len(select_task.query_parameters), 0)
        select_task.int64_param("max_count", 5)
        self.assertEqual(len(select_task.query_parameters), 1)

    def test_select_int64_array_param(self):
        query = "SELECT word FROM `bigquery-public-data.samples.shakespeare` WHERE word_count in UNNEST(@word_counts) order by rand() LIMIT 5"
        select_task = bruneus.select(query)
        self.assertEqual(len(select_task.query_parameters), 0)
        select_task.int64_array_param("word_counts", [10, 20, 30])
        self.assertEqual(len(select_task.query_parameters), 1)

    def test_select_str_array_param(self):
        query = "SELECT DISTINCT word FROM `bigquery-public-data.samples.shakespeare` WHERE word in UNNEST(@words)"
        select_task = bruneus.select(query)
        self.assertEqual(len(select_task.query_parameters), 0)
        select_task.str_array_param("words", ["quality", "that", "your"])
        self.assertEqual(len(select_task.query_parameters), 1)
        self.assertEqual(len(select_task.to_dataframe().index), 3)

    def test_select_params(self):
        query = "SELECT word FROM `bigquery-public-data.samples.shakespeare` order by rand() limit @max_count"
        task1 = bruneus.select(query).int64_param("max_count", 1)
        self.assertEqual(len(task1.query_parameters), 1)

        task2 = bruneus.select(query).int64_param("max_count", 2)
        self.assertEqual(len(task2.query_parameters), 1)


if __name__ == "__main__":
    unittest.main()
