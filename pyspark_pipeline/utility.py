from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
import requests
import time
import logging
from pyspark.sql.functions import *
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a session to reuse for multiple requests
session = requests.Session()

def fetch_movie_data_spark(movie_id, api_key):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&append_to_response=credits"
    try:
        time.sleep(0.5)  # Adding a delay to avoid rate limiting
        response = session.get(url)
        response.raise_for_status()

        data = response.json()

        # Check for missing values in critical fields and replace with []
        critical_fields = ['genres', 'production_companies', 'production_countries', 'spoken_languages']
        for field in critical_fields:
            if field not in data or data[field] is None or len(data[field]) == 0:
                data[field] = []  # Replace with an empty list

        return data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data for movie ID {movie_id}: {e}")
        return {'movie_id': movie_id, 'error': str(e)}
    except Exception as e:
        logging.error(f"An unexpected error occurred for movie ID {movie_id}: {e}")
        return {'movie_id': movie_id, 'error': str(e)}

# Define the Spark schema with crew fields
movie_schema = StructType([
    StructField("adult", BooleanType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("belongs_to_collection", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("poster_path", StringType(), True),
        StructField("backdrop_path", StringType(), True)
    ]), True),
    StructField("budget", IntegerType(), True),
    StructField("genres", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("homepage", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("imdb_id", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("poster_path", StringType(), True),
    StructField("production_companies", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("logo_path", StringType(), True),
        StructField("name", StringType(), True),
        StructField("origin_country", StringType(), True)
    ])), True),
    StructField("production_countries", ArrayType(StructType([
        StructField("iso_3166_1", StringType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", LongType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("spoken_languages", ArrayType(StructType([
        StructField("english_name", StringType(), True),
        StructField("iso_639_1", StringType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("status", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("title", StringType(), True),
    StructField("video", BooleanType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("credits", StructType([
        StructField("cast", ArrayType(StructType([
            StructField("adult", BooleanType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("known_for_department", StringType(), True),
            StructField("name", StringType(), True),
            StructField("original_name", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("profile_path", StringType(), True),
            StructField("cast_id", IntegerType(), True),
            StructField("character", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("order", IntegerType(), True)
        ])), True),
        StructField("crew", ArrayType(StructType([
            StructField("adult", BooleanType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("known_for_department", StringType(), True),
            StructField("name", StringType(), True),
            StructField("original_name", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("profile_path", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("department", StringType(), True),
            StructField("job", StringType(), True)
        ])), True)
    ]), True)
])


def create_raw_movies_spark_dataframe(movie_ids_list, api_key, schema=None):
    spark = SparkSession.builder.appName("MovieDataPipeline").getOrCreate()
    movie_ids_rdd = spark.sparkContext.parallelize(movie_ids_list)
    raw_data_rdd = movie_ids_rdd.map(lambda movie_id: fetch_movie_data_spark(movie_id, api_key))

    # Apply the schema if provided
    if schema:
        raw_movies_df = spark.createDataFrame(raw_data_rdd, schema=schema)
    else:
        raw_movies_df = raw_data_rdd.map(lambda data: Row(**data)).toDF()

    return raw_movies_df


def process_genres_column_spark(df, genre_column='genres'):
    """
    Extracts genre names from a list of dictionaries in a DataFrame column
    and joins them into a pipe-separated string, creating a new column 
    named 'genres_pro'.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame containing the genre data.
        genre_column (str, optional): The name of the column containing the
                                       list of genre dictionaries.
                                       Defaults to 'genres'.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the new 'genres_pro' column.
    """

    # Extract and join genre names, using PySpark SQL functions for conditionals
    df = df.withColumn(
        "genres_pro",
        concat_ws("|", transform(
            genre_column,
            lambda genre: when(genre.isNotNull() & genre.getItem("name").isNotNull(), genre.getItem("name")).otherwise(None)
        ))
    )

    return df

def process_languages_column_spark(df, language_column='spoken_languages'):
    """
    Extracts English language names from a list of dictionaries in a DataFrame column
    and joins them into a pipe-separated string, creating a new column
    named 'spoken_languages_pro'.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame containing the language data.
        language_column (str, optional): The name of the column containing the
                                          list of language dictionaries.
                                          Defaults to 'spoken_languages'.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the new 'spoken_languages_pro' column.
    """
    # Extract and join English language names using PySpark SQL functions
    df = df.withColumn(
        "spoken_languages_pro",
        concat_ws("|", transform(
            language_column,
            lambda lang: when(lang.getItem("english_name").isNotNull(), lang.getItem("english_name")).otherwise(None)
        ))
    )

    return df


def process_production_companies_column_spark(df, production_companies_column='production_companies'):
    """
    Extracts production company names from a list of dictionaries in a DataFrame column
    and joins them into a pipe-separated string, creating a new column
    named 'production_companies_pro'.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame containing the production company data.
        production_companies_column (str, optional): The name of the column containing the
                                                     list of production company dictionaries.
                                                     Defaults to 'production_companies'.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the new 'production_companies_pro' column.
    """
    # Extract and join production company names using PySpark SQL functions
    df = df.withColumn(
        "production_companies_pro",
        concat_ws("|", transform(
            production_companies_column,
            lambda company: when(company.getItem("name").isNotNull(), company.getItem("name")).otherwise(None)
        ))
    )
    
    return df


def process_production_countries_column_spark(df, production_countries_column='production_countries'):
    """
    Extracts production country names from a list of dictionaries in a DataFrame column
    and joins them into a pipe-separated string, creating a new column
    named 'production_countries_pro'.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame containing the production country data.
        production_countries_column (str, optional): The name of the column containing the
                                                       list of production country dictionaries.
                                                       Defaults to 'production_countries'.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the new 'production_countries_pro' column.
    """
    # Extract and join production country names using PySpark SQL functions
    df = df.withColumn(
        "production_countries_pro",
        concat_ws("|", transform(
            production_countries_column,
            lambda country: when(country.getItem("name").isNotNull(), country.getItem("name")).otherwise(None)
        ))
    )

    return df



def convert_column_to_datetime_spark(df, column_name='release_date'):
    """
    Converts a specified column in a PySpark DataFrame to date type.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to modify.
        column_name (str, optional): The name of the column to convert.
                                     Defaults to 'release_date'.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the specified column 
                               converted to date type.
    """
    # Check if the column exists
    if column_name in df.columns:
        # Convert the column to date type using to_date function
        # Added the format string 'yyyy-MM-dd' to properly parse the dates
        df = df.withColumn(column_name, to_date(column_name, 'yyyy-MM-dd'))  
        return df
    else:
        print(f"Warning: Column '{column_name}' not found in the DataFrame.")
        return df