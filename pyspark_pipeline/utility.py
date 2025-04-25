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