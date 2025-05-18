from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, array_join, transform, filter, when, to_date, desc, asc,
    count_distinct, size, array_distinct, explode, split, count,
    sum, avg, max, round
)
import requests
import os
import logging
import time
from typing import List, Optional, Dict, Callable
from requests.exceptions import RequestException
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Logger
logger = logging.getLogger('movie_pipeline')

# This global variable will store the SparkSession instance to ensure it's a singleton
_spark_session_instance = None

def get_spark_session(app_name: str = "MovieDataPipeline") -> SparkSession:
    """Initialize a Spark session with optimized configurations and distribute utility.py."""
    global _spark_session_instance

    if _spark_session_instance is None:
        spark_master = os.getenv("SPARK_MASTER_URL")
        if not spark_master:
            logger.error("SPARK_MASTER_URL not found in environment variables")
            raise ValueError("SPARK_MASTER_URL is required")
        _spark_session_instance = SparkSession.builder \
            .appName(app_name) \
            .master(spark_master) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.memory.storageFraction", "0.3") \
            .getOrCreate()

        utility_file_path = os.path.abspath(__file__)
        _spark_session_instance.sparkContext.addPyFile(utility_file_path)
        logger.info(f"Added {utility_file_path} to SparkContext's Python files.")

    return _spark_session_instance

def fetch_movie_data_single(movie_id: int, api_key: str, max_retries: int = 3) -> Dict:
    """Fetch movie data for a single movie ID from the TMDB API."""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&append_to_response=credits"
    logger.debug(f"Attempting to fetch data for movie ID {movie_id} from {url}")
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            critical_fields = ['genres', 'production_companies', 'production_countries', 'spoken_languages']
            for field in critical_fields:
                if field not in data or data[field] is None:
                    data[field] = []
            logger.info(f"Successfully fetched data for movie ID {movie_id}")
            return data
        except RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for movie ID {movie_id}: {e}")
            if attempt + 1 == max_retries:
                logger.error(f"Movie ID {movie_id} failed after {max_retries} attempts")
                return {'movie_id': movie_id, 'error': str(e)}
        except Exception as e:
            logger.error(f"Unexpected error for movie ID {movie_id}: {e}")
            return {'movie_id': movie_id, 'error': str(e)}
        time.sleep(0.2)
    return {'movie_id': movie_id, 'error': 'Max retries reached'}

def fetch_batch(batch_ids: List[int], api_key: str) -> List[Dict]:
    """Fetch movie data for a batch of movie IDs."""
    return [fetch_movie_data_single(movie_id, api_key) for movie_id in batch_ids]

def fetch_and_save_raw_data(movie_ids: List[int], api_key: str, parquet_path: str) -> Optional[DataFrame]:
    """Fetch movie data using RDD batching and save to Parquet."""
    spark = get_spark_session()
    try:
        batch_size = 5
        rate_limit_delay = 1.0

        schema = StructType([
            StructField("movie_id", IntegerType(), True),
            StructField("error", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("budget", LongType(), True),
            StructField("revenue", LongType(), True),
            StructField("release_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("vote_count", IntegerType(), True),
            StructField("vote_average", FloatType(), True),
            StructField("popularity", FloatType(), True),
            StructField("runtime", IntegerType(), True),
            StructField("overview", StringType(), True),
            StructField("tagline", StringType(), True),
            StructField("poster_path", StringType(), True),
            StructField("original_language", StringType(), True),
            StructField("genres", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ])), True),
            StructField("production_companies", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ])), True),
            StructField("production_countries", ArrayType(StructType([
                StructField("iso_3166_1", StringType(), True),
                StructField("name", StringType(), True)
            ])), True),
            StructField("spoken_languages", ArrayType(StructType([
                StructField("iso_639_1", StringType(), True),
                StructField("english_name", StringType(), True)
            ])), True),
            StructField("credits", StructType([
                StructField("cast", ArrayType(StructType([
                    StructField("id", IntegerType(), True),
                    StructField("original_name", StringType(), True)
                ])), True),
                StructField("crew", ArrayType(StructType([
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("job", StringType(), True)
                ])), True)
            ]), True),
            StructField("belongs_to_collection", StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ]), True)
        ])

        movie_ids_rdd = spark.sparkContext.parallelize(movie_ids)

        def process_partition(iterator):
            batch = []
            for movie_id in iterator:
                batch.append(movie_id)
                if len(batch) >= batch_size:
                    yield from fetch_batch(batch, api_key)
                    batch = []
                    time.sleep(rate_limit_delay)
            if batch:
                yield from fetch_batch(batch, api_key)

        raw_data_rdd = movie_ids_rdd.mapPartitions(process_partition)
        raw_df = spark.createDataFrame(raw_data_rdd, schema)

        # Log failed movie IDs
        failed_df = raw_df.filter(col('error').isNotNull()).select('movie_id', 'error')
        if failed_df.count() > 0:
            failed_records = failed_df.collect()
            for row in failed_records:
                logger.error(f"Failed to fetch movie ID {row['movie_id']}: {row['error']}")

        valid_df = raw_df.filter(col('error').isNull())
        if valid_df.count() == 0:
            logger.error("No valid data fetched.")
            return None

        try:
            os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
            logger.info(f"Ensured directory exists for {parquet_path}")
        except OSError as e:
            logger.error(f"Failed to create directory for {parquet_path}: {e}")
            return None

        valid_df.write.parquet(parquet_path, mode='overwrite')
        logger.info(f"Saved raw data to {parquet_path}")
        return valid_df
    except Exception as e:
        logger.error(f"Failed to fetch or save raw data: {e}")
        return None

def load_data(parquet_path: str) -> Optional[DataFrame]:
    """Load movie data from a Parquet file."""
    spark = get_spark_session()
    try:
        df = spark.read.parquet(parquet_path)
        logger.info(f"Loaded data from {parquet_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to load data from {parquet_path}: {e}")
        return None

def pipe(df: DataFrame, *funcs: Callable[[DataFrame], DataFrame]) -> DataFrame:
    """Apply a sequence of transformation functions to a Spark DataFrame."""
    result = df
    for func in funcs:
        try:
            result = func(result)
        except Exception as e:
            logger.error(f"Failed to apply function {func.__name__}: {e}")
            raise
    return result

def filter_released(df: DataFrame) -> DataFrame:
    """Filter movies with 'Released' status and drop the status column."""
    if 'status' in df.columns:
        df = df.filter(col('status') == 'Released').drop('status')
        logger.info("Filtered for 'Released' movies and dropped 'status' column")
    return df

def process_genres(df: DataFrame) -> DataFrame:
    """Convert genres list to a pipe-separated string."""
    df = df.withColumn('genres', array_join(transform(col('genres'), lambda x: x["name"]), '|'))
    logger.info("Processed genres column")
    return df

def process_languages(df: DataFrame) -> DataFrame:
    """Convert spoken_languages list to a pipe-separated string."""
    df = df.withColumn('spoken_languages', array_join(transform(col('spoken_languages'), lambda x: x["english_name"]), '|'))
    logger.info("Processed spoken_languages column")
    return df

def process_production_companies(df: DataFrame) -> DataFrame:
    """Convert production_companies list to a pipe-separated string."""
    df = df.withColumn('production_companies', array_join(transform(col('production_companies'), lambda x: x["name"]), '|'))
    logger.info("Processed production_companies column")
    return df

def process_production_countries(df: DataFrame) -> DataFrame:
    """Convert production_countries list to a pipe-separated string."""
    df = df.withColumn('production_countries', array_join(transform(col('production_countries'), lambda x: x["name"]), '|'))
    logger.info("Processed production_countries column")
    return df

def process_directors(df: DataFrame) -> DataFrame:
    """Extract directors from credits.crew and convert to a pipe-separated string."""
    df = df.withColumn(
        'director',
        array_join(
            transform(
                filter(col('credits.crew'), lambda x: x["job"] == 'Director'),
                lambda x: x["name"]
            ),
            '|'
        )
    )
    logger.info("Processed director column")
    return df

def process_cast_size(df: DataFrame) -> DataFrame:
    """Calculate the number of unique cast members from credits.cast."""
    df = df.withColumn('cast_size', size(array_distinct(transform(col('credits.cast'), lambda x: x["id"]))))
    logger.info("Processed cast_size column")
    return df

def process_crew_size(df: DataFrame) -> DataFrame:
    """Calculate the number of unique crew members from credits.crew."""
    df = df.withColumn('crew_size', size(array_distinct(transform(col('credits.crew'), lambda x: x["id"]))))
    logger.info("Processed crew_size column")
    return df

def process_cast(df: DataFrame) -> DataFrame:
    """Convert cast list to a pipe-separated string of names."""
    df = df.withColumn('cast', array_join(transform(col('credits.cast'), lambda x: x["original_name"]), '|'))
    logger.info("Processed cast column")
    return df

def convert_release_date(df: DataFrame) -> DataFrame:
    """Convert release_date to a datetime format."""
    if 'release_date' in df.columns:
        df = df.withColumn('release_date', to_date(col('release_date'), 'yyyy-MM-dd'))
        logger.info("Converted release_date to datetime")
    return df

def convert_numeric_columns(df: DataFrame) -> DataFrame:
    """Convert specified columns to numeric (float) type."""
    for col_name in ['budget', 'popularity', 'id']:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast('float'))
            logger.info(f"Converted {col_name} to numeric")
    return df

def convert_budget_musd(df: DataFrame) -> DataFrame:
    """Convert budget to million USD, replacing 0 with null."""
    if 'budget' in df.columns:
        df = df.withColumn('budget', when(col('budget') == 0, None).otherwise(col('budget')))
        df = df.withColumn('budget_musd', round(col('budget') / 1_000_000, 2))
        logger.info("Converted budget to MUSD and replaced 0 with null")
    return df

def convert_revenue_musd(df: DataFrame) -> DataFrame:
    """Convert revenue to million USD, replacing 0 with null."""
    if 'revenue' in df.columns:
        df = df.withColumn('revenue', when(col('revenue') == 0, None).otherwise(col('revenue')))
        df = df.withColumn('revenue_musd', round(col('revenue') / 1_000_000, 2))
        logger.info("Converted revenue to MUSD and replaced 0 with null")
    return df

def reorder_columns(df: DataFrame) -> DataFrame:
    """Reorder DataFrame columns to a predefined order."""
    desired_columns = [
        'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
        'belongs_to_collection.id', 'belongs_to_collection.name', 'original_language',
        'budget_musd', 'revenue_musd', 'production_companies', 'production_countries',
        'vote_count', 'vote_average', 'popularity', 'runtime', 'overview',
        'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
    ]
    existing_columns = [c for c in desired_columns if c in df.columns]
    df = df.select(*existing_columns)
    logger.info("Reordered columns")
    return df

def drop_unwanted_columns(df: DataFrame) -> DataFrame:
    """Drop unwanted columns from the DataFrame."""
    columns_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    df = df.drop(*[c for c in columns_to_drop if c in df.columns])
    logger.info("Dropped unwanted columns")
    return df

def preprocess_data(df: DataFrame) -> DataFrame:
    """Preprocess the Spark DataFrame by applying transformations."""
    logger.info("Starting data preprocessing")
    try:
        result = pipe(
            df,
            filter_released,
            process_genres,
            process_languages,
            process_production_companies,
            process_production_countries,
            process_directors,
            process_cast_size,
            process_crew_size,
            process_cast,
            convert_release_date,
            convert_numeric_columns,
            convert_budget_musd,
            convert_revenue_musd,
            reorder_columns,
            drop_unwanted_columns
        )
        return result
    except Exception as e:
        logger.error(f"Failed during preprocessing: {e}")
        return df

def save_processed_data(df: DataFrame, parquet_path: str) -> None:
    """Save the processed Spark DataFrame to a Parquet file."""
    try:
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        logger.info(f"Ensured directory exists for {parquet_path}")
        df.write.parquet(parquet_path, mode='overwrite')
        logger.info(f"Saved processed data to {parquet_path}")
    except Exception as e:
        logger.error(f"Failed to save processed data: {e}")

def calculate_kpis(df: DataFrame) -> Dict[str, DataFrame]:
    """Calculate key performance indicators (KPIs) for movie data."""
    logger.info("Calculating KPIs")
    spark = get_spark_session()
    kpis = {}
    try:
        kpis['top_10_revenue'] = df.select('title', 'revenue_musd').orderBy(desc('revenue_musd')).limit(10)
        logger.info("Calculated top_10_revenue")
    except Exception as e:
        logger.error(f"Failed to calculate top_10_revenue: {e}")
        kpis['top_10_revenue'] = spark.createDataFrame([], StructType([]))

    try:
        kpis['top_10_budget'] = df.select('title', 'budget_musd').orderBy(desc('budget_musd')).limit(10)
        logger.info("Calculated top_10_budget")
    except Exception as e:
        logger.error(f"Failed to calculate top_10_budget: {e}")
        kpis['top_10_budget'] = spark.createDataFrame([], StructType([]))

    try:
        kpis['top_10_profit'] = df.withColumn('profit', col('revenue_musd') - col('budget_musd')) \
                                  .select('title', 'profit').orderBy(desc('profit')).limit(10)
        logger.info("Calculated top_10_profit")
    except Exception as e:
        logger.error(f"Failed to calculate top_10_profit: {e}")
        kpis['top_10_profit'] = spark.createDataFrame([], StructType([]))

    try:
        kpis['bottom_10_profit'] = df.withColumn('profit', col('revenue_musd') - col('budget_musd')) \
                                     .select('title', 'profit').orderBy(asc('profit')).limit(10)
        logger.info("Calculated bottom_10_profit")
    except Exception as e:
        logger.error(f"Failed to calculate bottom_10_profit: {e}")
        kpis['bottom_10_profit'] = spark.createDataFrame([], StructType([]))

    try:
        kpis['top_10_roi'] = df.filter(col('budget_musd') >= 10) \
                               .withColumn('ROI', col('revenue_musd') / col('budget_musd')) \
                               .select('title', 'ROI').orderBy(desc('ROI')).limit(10)
        logger.info("Calculated top_10_roi")
    except Exception as e:
        logger.error(f"Failed to calculate top_10_roi: {e}")
        kpis['top_10_roi'] = spark.createDataFrame([], StructType([]))

    try:
        kpis['bottom_10_roi'] = df.filter(col('budget_musd') >= 10) \
                                  .withColumn('ROI', col('revenue_musd') / col('budget_musd')) \
                                  .select('title', 'ROI').orderBy(asc('ROI')).limit(10)
        logger.info("Calculated bottom_10_roi")
    except Exception as e:
        logger.error(f"Failed to calculate bottom_10_roi: {e}")
        kpis['bottom_10_roi'] = spark.createDataFrame([], StructType([]))

    try:
        max_vote_count = df.select(max('vote_count')).collect()[0][0]
        kpis['most_voted'] = df.filter(col('vote_count') == max_vote_count).select('title', 'vote_count')
        logger.info("Calculated most_voted")
    except Exception as e:
        logger.error(f"Failed to calculate most_voted: {e}")
        kpis['most_voted'] = spark.createDataFrame([], StructType([]))

    try:
        kpis['highest_rated'] = df.filter(col('vote_count') >= 10) \
                                 .select('title', 'vote_average').orderBy(desc('vote_average')).limit(10)
        logger.info("Calculated highest_rated")
    except Exception as e:
        logger.error(f"Failed to calculate highest_rated: {e}")
        kpis['highest_rated'] = spark.createDataFrame([], StructType([]))

    try:
        kpis['lowest_rated'] = df.filter(col('vote_count') >= 10) \
                                .select('title', 'vote_average').orderBy(asc('vote_average')).limit(10)
        logger.info("Calculated lowest_rated")
    except Exception as e:
        logger.error(f"Failed to calculate lowest_rated: {e}")
        kpis['lowest_rated'] = spark.createDataFrame([], StructType([]))

    try:
        kpis['most_popular'] = df.select('title', 'popularity').orderBy(desc('popularity')).limit(10)
        logger.info("Calculated most_popular")
    except Exception as e:
        logger.error(f"Failed to calculate most_popular: {e}")
        kpis['most_popular'] = spark.createDataFrame([], StructType([]))

    try:
        kpis['uma_tarantino'] = df.filter(
            (col('cast').contains('Uma Thurman')) & (col('director').contains('Quentin Tarantino'))
        ).select('title', 'director', 'runtime').orderBy('runtime')
        logger.info("Calculated uma_tarantino")
    except Exception as e:
        logger.error(f"Failed to calculate uma_tarantino: {e}")
        kpis['uma_tarantino'] = spark.createDataFrame([], StructType([]))

    try:
        kpis['sci_fi_bruce'] = df.filter(
            (col('genres').contains('Science Fiction')) & 
            (col('genres').contains('Action')) & 
            (col('cast').contains('Bruce Willis'))
        ).select('title', 'vote_average').orderBy(desc('vote_average')).limit(5)
        logger.info("Calculated sci_fi_bruce")
    except Exception as e:
        logger.error(f"Failed to calculate sci_fi_bruce: {e}")
        kpis['sci_fi_bruce'] = spark.createDataFrame([], StructType([]))

    return kpis

def analyze_franchises(df: DataFrame) -> DataFrame:
    """Analyze movie franchises by aggregating metrics."""
    logger.info("Analyzing franchises")
    try:
        franchise_df = df.filter(col('belongs_to_collection.id').isNotNull())
        if franchise_df.count() == 0:
            logger.warning("No franchise data available")
            return get_spark_session().createDataFrame([], StructType([]))

        return franchise_df.groupBy('belongs_to_collection.name') \
                           .agg(
                               count('id').alias('movie_count'),
                               sum('budget_musd').alias('total_budget'),
                               avg('budget_musd').alias('mean_budget'),
                               sum('revenue_musd').alias('total_revenue'),
                               avg('revenue_musd').alias('mean_revenue'),
                               avg('vote_average').alias('mean_rating')
                           ) \
                           .orderBy(desc('total_revenue'))
    except Exception as e:
        logger.error(f"Failed to analyze franchises: {e}")
        return get_spark_session().createDataFrame([], StructType([]))

def analyze_directors(df: DataFrame) -> DataFrame:
    """Analyze directors by aggregating metrics."""
    logger.info("Analyzing directors")
    try:
        return df.filter(col('director').isNotNull()) \
                .withColumn('director', explode(split(col('director'), '[|]'))) \
                .filter(col('director') != '') \
                .groupBy('director') \
                .agg(
                    count_distinct('id').alias('movie_count'),
                    sum('revenue_musd').alias('total_revenue'),
                    avg('vote_average').alias('mean_rating')
                ) \
                .orderBy(desc('total_revenue'))
    except Exception as e:
        logger.error(f"Failed to analyze directors: {e}")
        return get_spark_session().createDataFrame([], StructType([]))

def visualize_data(df: DataFrame) -> None:
    """Generate visualizations from processed movie data."""
    logger.info("Generating visualizations")
    vis_dir = os.getenv("VISUALIZATION_PATH", "/app/data/visualizations")
    if not vis_dir:
        logger.warning("VISUALIZATION_PATH not set, using default: /app/data/visualizations")
    
    try:
        os.makedirs(vis_dir, exist_ok=True)
        logger.info(f"Ensured directory exists for {vis_dir}")
    except OSError as e:
        logger.error(f"Failed to create visualizations directory: {e}")
        return

    try:
        pdf = df.toPandas()
    except Exception as e:
        logger.error(f"Failed to convert DataFrame to Pandas: {e}")
        return

    sns.set(style="whitegrid")

    # Revenue vs Budget Scatter Plot
    try:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=pdf, x='budget_musd', y='revenue_musd', alpha=0.6)
        plt.plot([0, pdf['budget_musd'].max()], [0, pdf['budget_musd'].max()], 'r--', label='Break-even')
        plt.title('Revenue vs Budget')
        plt.xlabel('Budget (MUSD)')
        plt.ylabel('Revenue (MUSD)')
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(vis_dir, 'revenue_vs_budget.png'))
        plt.close()
        logger.info(f"Saved Revenue vs Budget plot to {os.path.join(vis_dir, 'revenue_vs_budget.png')}")
    except Exception as e:
        logger.error(f"Failed to generate Revenue vs Budget plot: {e}")

    # ROI by Genre Bar Plot
    try:
        roi_genre = pdf[pdf['budget_musd'] > 0.1].copy()
        roi_genre['ROI'] = roi_genre['revenue_musd'] / roi_genre['budget_musd']
        roi_genre['genres'] = roi_genre['genres'].str.split('|')
        roi_genre = roi_genre.explode('genres')
        roi_genre = roi_genre[roi_genre['genres'].notnull() & (roi_genre['genres'] != '')]
        roi_genre = roi_genre.groupby('genres')['ROI'].median().reset_index()
        roi_genre = roi_genre.sort_values('ROI', ascending=False).head(15)
        
        plt.figure(figsize=(12, 6))
        sns.barplot(data=roi_genre, x='ROI', y='genres')
        plt.title('Median ROI by Genre')
        plt.xlabel('Median ROI')
        plt.ylabel('Genre')
        plt.tight_layout()
        plt.savefig(os.path.join(vis_dir, 'roi_by_genre.png'))
        plt.close()
        logger.info(f"Saved ROI by Genre plot to {os.path.join(vis_dir, 'roi_by_genre.png')}")
    except Exception as e:
        logger.error(f"Failed to generate ROI by Genre plot: {e}")

    # Popularity vs Rating Scatter Plot
    try:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=pdf, x='popularity', y='vote_average', alpha=0.6)
        plt.title('Popularity vs Rating')
        plt.xlabel('Popularity')
        plt.ylabel('Vote Average')
        plt.tight_layout()
        plt.savefig(os.path.join(vis_dir, 'popularity_vs_rating.png'))
        plt.close()
        logger.info(f"Saved Popularity vs Rating plot to {os.path.join(vis_dir, 'popularity_vs_rating.png')}")
    except Exception as e:
        logger.error(f"Failed to generate Popularity vs Rating plot: {e}")

    # Yearly Revenue Trends Line Plot
    try:
        yearly_revenue = pdf[pdf['revenue_musd'].notnull()].copy()
        yearly_revenue['release_year'] = pd.to_datetime(yearly_revenue['release_date']).dt.year
        yearly_revenue = yearly_revenue.groupby('release_year')['revenue_musd'].sum().reset_index()

        plt.figure(figsize=(10, 6))
        sns.lineplot(data=yearly_revenue, x='release_year', y='revenue_musd')
        plt.title('Yearly Revenue Trends')
        plt.xlabel('Release Year')
        plt.ylabel('Total Revenue (MUSD)')
        plt.tight_layout()
        plt.savefig(os.path.join(vis_dir, 'yearly_revenue_trends.png'))
        plt.close()
        logger.info(f"Saved Yearly Revenue Trends plot to {os.path.join(vis_dir, 'yearly_revenue_trends.png')}")
    except Exception as e:
        logger.error(f"Failed to generate Yearly Revenue Trends plot: {e}")

    # Franchise vs Standalone Box Plot
    try:
        franchise = pdf[pdf['revenue_musd'].notnull()].copy()
        franchise['is_franchise'] = franchise['belongs_to_collection'].notnull()
        franchise['is_franchise'] = franchise['is_franchise'].map({True: 'Franchise', False: 'Standalone'})

        plt.figure(figsize=(10, 6))
        sns.boxplot(data=franchise, x='is_franchise', y='revenue_musd')
        plt.title('Franchise vs Standalone Revenue')
        plt.xlabel('Type')
        plt.ylabel('Revenue (MUSD)')
        plt.tight_layout()
        plt.savefig(os.path.join(vis_dir, 'franchise_vs_standalone.png'))
        plt.close()
        logger.info(f"Saved Franchise vs Standalone plot to {os.path.join(vis_dir, 'franchise_vs_standalone.png')}")
    except Exception as e:
        logger.error(f"Failed to generate Franchise vs Standalone plot: {e}")