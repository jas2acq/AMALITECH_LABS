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
    

def process_director_column_spark(df, crew_column='credits.crew', director_column='director'):
    """
    Extracts and joins the names of directors from the 'crew' field within the 'credits'
    column of a PySpark DataFrame, creating a new column named 'director'.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame containing the crew data.
        crew_column (str, optional): The name of the column containing the crew data
                                    (nested within 'credits'). Defaults to 'credits.crew'.
        director_column (str, optional): The name of the new column to create for directors.
                                         Defaults to 'director'.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the added 'director' column.
    """
    df = df.withColumn(
        director_column,
        concat_ws("|", transform(
            crew_column,
            lambda crew_member: when(
                (crew_member.getItem("job") == "Director"),
                crew_member.getItem("name")
            ).otherwise(None)
        ))
    )
    return df


def process_cast_size_column_spark(df, cast_column='credits.cast', cast_size_column="cast_size"):
    """
    Extracts the size of the cast (number of unique cast member IDs)
    from a list of dictionaries in a specified cast column of a PySpark DataFrame.
    Handles potential null values or empty lists.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame containing the cast data.
        cast_column (str, optional): The name of the column containing the
                                     list of cast dictionaries.
                                     Defaults to 'credits.cast'.
        cast_size_column (str, optional): The name of the new column to create
                                          with the cast size.
                                          Defaults to 'cast_size'.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the added cast size column.
    """

    # Define a UDF to extract unique cast IDs and get the size, handling nulls
    extract_cast_size_udf = udf(lambda cast_list: len(set([c[2] for c in cast_list if isinstance(c, (list, tuple)) and len(c) > 2])) if cast_list else 0, IntegerType())

    # Apply the UDF to the DataFrame
    df = df.withColumn(cast_size_column, extract_cast_size_udf(col(cast_column)))
    
    return df


def process_crew_size_column_spark(df, crew_column='credits.crew', crew_size_column='crew_size'):
    """
    Extracts the size of the crew (number of unique crew member IDs)
    from a list of dictionaries in a specified crew column of a PySpark DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame containing the crew data.
        crew_column (str, optional): The name of the column containing the
                                     list of crew dictionaries.
                                     Defaults to 'credits.crew'.
        crew_size_column (str, optional): The name of the new column to create
                                          with the crew size.
                                          Defaults to 'crew_size'.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the added crew size column.
    """

    # Define a UDF to extract unique crew IDs and get the size
    extract_crew_size_udf = udf(lambda crew_list: len(set([c[2] for c in crew_list if isinstance(c, (list, tuple)) and len(c) > 2])) if crew_list else 0, IntegerType())


    # Apply the UDF to the DataFrame
    df = df.withColumn(crew_size_column, extract_crew_size_udf(col(crew_column)))
    
    return df


def convert_budget_to_musd_spark(df):
    """
    Converts the 'budget' column (in USD) to millions of USD (MUSD) 
    and rounds to the nearest whole number, creating a new column 
    'budget_musd'.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to modify.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the added 'budget_musd' column.
    """
    original_column = 'budget'
    new_column = 'budget_musd'

    if original_column in df.columns:
        df = df.withColumn(new_column, round(col(original_column) / 1000000))
        return df
    else:
        print(f"Warning: Column '{original_column}' not found in the DataFrame.")
        return df

def convert_revenue_to_musd_spark(df):
    """
    Converts the 'revenue' column (in USD) to millions of USD (MUSD) 
    and rounds to two decimal places, creating a new column 'revenue_musd'.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to modify.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the added 'revenue_musd' column.
    """
    original_column = 'revenue'
    new_column = 'revenue_musd'

    if original_column in df.columns:
        df = df.withColumn(new_column, round(col(original_column) / 1000000, 2))
        return df
    else:
        print(f"Warning: Column '{original_column}' not found in the DataFrame.")
        return df
    

def process_cast_column_spark(df, cast_column='credits.cast', output_cast_column='cast'):
    """
    Extracts and joins the original names of cast members from a list of dictionaries
    in a specified cast column of a PySpark DataFrame, creating a new column
    with the joined cast names.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame containing the cast data.
        cast_column (str, optional): The name of the column containing the
                                     list of cast dictionaries.
                                     Defaults to 'credits.cast'.
        output_cast_column (str, optional): The name of the new column to create
                                            with the joined cast names.
                                            Defaults to 'cast'.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the added cast column.
    """
    df = df.withColumn(
        output_cast_column,
        concat_ws("|", transform(
            cast_column,
            lambda castor: when(
                castor.getItem("original_name").isNotNull(),
                castor.getItem("original_name")
            ).otherwise(None)
        ))
    )
    return df


def reorder_dataframe_columns_spark(df, column_order):
    """
    Reorders the columns of a PySpark DataFrame according to the specified order.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to reorder.
        column_order (list): A list of column names in the desired order.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the columns reordered.
    """
    existing_columns = df.columns
    
    # Check for invalid columns and print warnings
    invalid_columns = [col for col in column_order if col not in existing_columns]
    for col in invalid_columns:
        print(f"Warning: Column '{col}' not found in the DataFrame and will be skipped.")
        
    # Select only valid columns in the desired order
    valid_column_order = [col for col in column_order if col in existing_columns]
    reordered_df = df.select(*valid_column_order)  
    
    return reordered_df


def drop_unwanted_columns_spark(df, columns_to_drop=None):
    """
    Drops specified columns from a PySpark DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to modify.
        columns_to_drop (list, optional): A list of column names to drop.
                                          Defaults to None.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the specified columns dropped.
    """
    if columns_to_drop is None:
        columns_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']

    # Check if columns exist before dropping to avoid errors
    existing_columns = df.columns
    valid_columns_to_drop = [col for col in columns_to_drop if col in existing_columns]

    # Drop the columns
    df = df.drop(*valid_columns_to_drop)  
    return df



def create_cleaned_movie_dataframe_spark(raw_df):
    """
    Applies all cleaning operations to a raw movie DataFrame to create a cleaned DataFrame.

    Args:
        raw_df (pyspark.sql.DataFrame): The raw movie DataFrame.

    Returns:
        pyspark.sql.DataFrame: The cleaned movie DataFrame.
    """
    # 1. Process genres column
    cleaned_df = process_genres_column_spark(raw_df)
    
    # 2. Process languages column
    cleaned_df = process_languages_column_spark(cleaned_df)
    
    # 3. Process production companies column
    cleaned_df = process_production_companies_column_spark(cleaned_df)
    
    # 4. Process production countries column
    cleaned_df = process_production_countries_column_spark(cleaned_df)
    
    # 5. Convert release date to datetime
    cleaned_df = convert_column_to_datetime_spark(cleaned_df)
    
    # 6. Process director column
    cleaned_df = process_director_column_spark(cleaned_df)
    
    # 7. Process cast size column
    cleaned_df = process_cast_size_column_spark(cleaned_df)
    
    # 8. Process crew size column
    cleaned_df = process_crew_size_column_spark(cleaned_df)
    
    # 9. Convert budget to MUSD
    cleaned_df = convert_budget_to_musd_spark(cleaned_df)
    
    # 10. Convert revenue to MUSD
    cleaned_df = convert_revenue_to_musd_spark(cleaned_df)
    
    # 11. Process cast column
    cleaned_df = process_cast_column_spark(cleaned_df)
    
    # 12. Reorder columns (optional, add your desired column order)
    new_column_order =['id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection', 
'original_language', 'budget_musd', 'revenue_musd', 'production_companies', 
'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime', 
'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size', 'spoken_languages_pro','production_companies_pro','production_countries_pro','genres_pro'] # Replace with your desired order
    cleaned_df = reorder_dataframe_columns_spark(cleaned_df, new_column_order)
    
    # 13. Drop unwanted columns
    cleaned_df = drop_unwanted_columns_spark(cleaned_df)  
    
    return cleaned_df