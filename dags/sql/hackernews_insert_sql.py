SQL_QUERY = {
        "HACKERNEWS_INSERT_COMMENT": """
                INSERT `data-engineering-2-329815.data_engineering_4.hacker_news_comment_airflow_test`
                SELECT *
                FROM `bigquery-public-data.hacker_news.comments`
                LIMIT 10;
        """
}
