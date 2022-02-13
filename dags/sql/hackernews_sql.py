SQL_QUERY = {
        "HACKERNEWS_COMMENT": """
                SELECT *
                FROM `bigquery-public-data.hacker_news.comments`
                LIMIT 10;
        """,
        
        "HACKERNEWS_STORIES": """
                SELECT *
                FROM `bigquery-public-data.hacker_news.stories`
                WHERE url IS NOT NULL
                LIMIT 10;
        """
}
