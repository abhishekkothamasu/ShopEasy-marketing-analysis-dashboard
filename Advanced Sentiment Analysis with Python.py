import pyodbc
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# Download VADER lexicon if not already downloaded
nltk.download('vader_lexicon')

def fetch_data_from_sql():
    try:
        # FIXED CONNECTION STRING
        conn_str = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=ABHISHEK\\SQLEXPRESS;"
            "Database=PortfolioProject_MarketingAnalytics;"
            "Trusted_Connection=yes;"
        )

        conn = pyodbc.connect(conn_str)
        print("✅ Connected to SQL Server Successfully")

        query = """
            SELECT
                ReviewID,
                CustomerID,
                ProductID,
                ReviewDate,
                Rating,
                ReviewText
            FROM dbo.customer_reviews
        """

        df = pd.read_sql(query, conn)
        conn.close()

        return df

    except Exception as e:
        print("❌ SQL Connection Error:", e)
        raise


def enrich_reviews_with_sentiment(df):
    sid = SentimentIntensityAnalyzer()

    df["sentiment"] = df["ReviewText"].apply(lambda x: sid.polarity_scores(str(x))["compound"])
    df["sentiment_label"] = df["sentiment"].apply(
        lambda score: "Positive" if score > 0.05 else ("Negative" if score < -0.05 else "Neutral")
    )

    return df


def main():
    df = fetch_data_from_sql()

    print("\n📌 Sample Input Data:")
    print(df.head())

    enriched_df = enrich_reviews_with_sentiment(df)

    print("\n📌 Enriched Data:")
    print(enriched_df.head())

    enriched_df.to_csv("enriched_customer_reviews.csv", index=False)
    print("\n✅ File saved: enriched_customer_reviews.csv")


if __name__ == "__main__":
    main()
