import requests
import pandas as pd
import os
import json
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
from typing import List, Optional, Dict, Any
from requests.exceptions import RequestException

# Logging is configured in main.py, so we just get the logger
logger = logging.getLogger('movie_pipeline')

def fetch_movie_data(movie_ids: List[int], api_key: str, max_retries: int = 3) -> List[Dict[str, Any]]:
    """Fetches movie data from The Movie Database API for given movie IDs."""
    all_movies_data = []
    for movie_id in movie_ids:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&append_to_response=credits"
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                all_movies_data.append(response.json())
                logger.info(f"Successfully fetched data for movie ID {movie_id}")
                break
            except RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for movie ID {movie_id}: {e}")
                if attempt + 1 == max_retries:
                    logger.error(f"Movie ID {movie_id} could not be reached after {max_retries} attempts")
                    continue
    return all_movies_data

def save_raw_data(all_movies_data: List[Dict[str, Any]], csv_path: str, json_path: str) -> Optional[pd.DataFrame]:
    """Converts movie data to a DataFrame and saves it to CSV and JSON."""
    if not all_movies_data:
        logger.error("No movie data fetched. Skipping save.")
        return None
    try:
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create directory for {csv_path}: {e}")
        return None
    try:
        df = pd.json_normalize(all_movies_data)
    except Exception as e:
        logger.error(f"Failed to normalize movie data: {e}")
        return None
    try:
        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient='records', indent=4)
        logger.info(f"Saved raw data to {csv_path} and {json_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to save raw data to CSV/JSON: {e}")
        return None

def load_data(csv_path: str, json_path: str) -> Optional[pd.DataFrame]:
    """Loads data from CSV or JSON with fallback mechanism."""
    try:
        dtypes = {'id': 'int32', 'budget': 'float32', 'revenue': 'float32', 'vote_count': 'int32',
                  'vote_average': 'float32', 'popularity': 'float32', 'runtime': 'float32'}
        return pd.read_csv(csv_path, dtype=dtypes, low_memory=False).pipe(
            lambda x: logger.info(f"Loaded data from {csv_path}") or x
        )
    except FileNotFoundError:
        logger.warning(f"CSV not found at {csv_path}. Attempting to load JSON.")
        try:
            return pd.read_json(json_path, dtype=dtypes).pipe(
                lambda x: logger.info(f"Loaded data from {json_path}") or x
            )
        except FileNotFoundError:
            logger.error(f"JSON not found at {json_path}")
            return None
        except Exception as e:
            logger.error(f"Failed to load JSON data: {e}")
            return None
    except pd.errors.EmptyDataError:
        logger.error(f"CSV file at {csv_path} is empty")
        return None
    except Exception as e:
        logger.error(f"Failed to load CSV data: {e}")
        return None

def process_column(df: pd.DataFrame, column: str, extract_fn: callable, output_column: Optional[str] = None,
                  inplace: bool = False) -> pd.DataFrame:
    """Processes a DataFrame column using a provided extraction function."""
    if column not in df.columns:
        logger.warning(f"Column '{column}' not found in DataFrame")
        return df
    try:
        new_column = output_column or column
        result = df[column].apply(extract_fn)
        if inplace:
            df[new_column] = result
            return df
        else:
            return df.copy().assign(**{new_column: result})
    except Exception as e:
        logger.error(f"Failed to process column '{column}': {e}")
        return df

def extract_genres(genre_list: Any) -> str:
    """Extracts genre names and joins with '|'."""
    try:
        return "|".join(genre['name'] for genre in (genre_list or []) if isinstance(genre, dict) and 'name' in genre) if isinstance(genre_list, list) else ""
    except Exception as e:
        logger.warning(f"Failed to extract genres: {e}")
        return ""

def extract_languages(lang_list: Any) -> str:
    """Extracts language English names and joins with '|'."""
    try:
        return "|".join(lang['english_name'] for lang in (lang_list or []) if isinstance(lang, dict) and 'english_name' in lang) if isinstance(lang_list, list) else ""
    except Exception as e:
        logger.warning(f"Failed to extract languages: {e}")
        return ""

def extract_production_companies(comp_list: Any) -> str:
    """Extracts production company names and joins with '|'."""
    try:
        return "|".join(comp['name'] for comp in (comp_list or []) if isinstance(comp, dict) and 'name' in comp) if isinstance(comp_list, list) else ""
    except Exception as e:
        logger.warning(f"Failed to extract production companies: {e}")
        return ""

def extract_production_countries(country_list: Any) -> str:
    """Extracts production country names and joins with '|'."""
    try:
        return "|".join(country['name'] for country in (country_list or []) if isinstance(country, dict) and 'name' in country) if isinstance(country_list, list) else ""
    except Exception as e:
        logger.warning(f"Failed to extract production countries: {e}")
        return ""

def extract_directors(crew_list: Any) -> str:
    """Extracts director names and joins with '|'."""
    try:
        return "|".join(crew['name'] for crew in (crew_list or []) if isinstance(crew, dict) and crew.get('job') == 'Director') if isinstance(crew_list, list) else ""
    except Exception as e:
        logger.warning(f"Failed to extract directors: {e}")
        return ""

def extract_cast_size(cast_list: Any) -> int:
    """Counts unique cast member IDs."""
    try:
        return len({cast['id'] for cast in (cast_list or []) if isinstance(cast, dict) and 'id' in cast}) if isinstance(cast_list, list) else 0
    except Exception as e:
        logger.warning(f"Failed to extract cast size: {e}")
        return 0

def extract_crew_size(crew_list: Any) -> int:
    """Counts unique crew member IDs."""
    try:
        return len({crew['id'] for crew in (crew_list or []) if isinstance(crew, dict) and 'id' in crew}) if isinstance(crew_list, list) else 0
    except Exception as e:
        logger.warning(f"Failed to extract crew size: {e}")
        return 0

def extract_cast(cast_list: Any) -> str:
    """Extracts cast member names and joins with '|'."""
    try:
        return "|".join(cast['original_name'] for cast in (cast_list or []) if isinstance(cast, dict) and 'original_name' in cast) if isinstance(cast_list, list) else ""
    except Exception as e:
        logger.warning(f"Failed to extract cast: {e}")
        return ""

def convert_to_musd(value: float) -> float:
    """Converts a value to millions USD, rounded appropriately."""
    try:
        return round(value / 1_000_000, 2) if pd.notna(value) else np.nan
    except (TypeError, ValueError) as e:
        logger.warning(f"Failed to convert value to MUSD: {e}")
        return np.nan

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the DataFrame by cleaning and transforming columns per project instructions."""
    logger.info("Starting data preprocessing")
    try:
        # Define a function to log numeric conversions
        def log_numeric_conversions(df, columns):
            for col in columns:
                if col in df.columns:
                    logger.info(f"Converted {col} to numeric")
            return df

        return (df.pipe(lambda x: x[x['status'] == 'Released'].drop(columns=['status'], errors='ignore') if 'status' in x.columns else x)
                .pipe(lambda x: process_column(x, 'genres', extract_genres, inplace=True))
                .pipe(lambda x: logger.info(f"After processing genres, columns: {list(x.columns)}") or x)
                .pipe(lambda x: process_column(x, 'spoken_languages', extract_languages, inplace=True))
                .pipe(lambda x: logger.info(f"After processing spoken_languages, columns: {list(x.columns)}") or x)
                .pipe(lambda x: process_column(x, 'production_companies', extract_production_companies, inplace=True))
                .pipe(lambda x: logger.info(f"After processing production_companies, columns: {list(x.columns)}") or x)
                .pipe(lambda x: process_column(x, 'production_countries', extract_production_countries, inplace=True))
                .pipe(lambda x: logger.info(f"After processing production_countries, columns: {list(x.columns)}") or x)
                .pipe(lambda x: process_column(x, 'credits.crew', extract_directors, output_column='director', inplace=True))
                .pipe(lambda x: logger.info(f"After processing directors, columns: {list(x.columns)}") or x)
                .pipe(lambda x: process_column(x, 'credits.cast', extract_cast_size, output_column='cast_size', inplace=True))
                .pipe(lambda x: logger.info(f"After processing cast_size, columns: {list(x.columns)}") or x)
                .pipe(lambda x: process_column(x, 'credits.crew', extract_crew_size, output_column='crew_size', inplace=True))
                .pipe(lambda x: logger.info(f"After processing crew_size, columns: {list(x.columns)}") or x)
                .pipe(lambda x: process_column(x, 'credits.cast', extract_cast, output_column='cast', inplace=True))
                .pipe(lambda x: logger.info(f"After processing cast, columns: {list(x.columns)}") or x)
                .pipe(lambda x: x.assign(release_date=pd.to_datetime(x['release_date'], errors='coerce')) if 'release_date' in x.columns else x)
                .pipe(lambda x: x.assign(**{col: pd.to_numeric(x[col], errors='coerce') for col in ['budget', 'popularity', 'id'] if col in x.columns}))
                .pipe(lambda x: log_numeric_conversions(x, ['budget', 'popularity', 'id']))
                .pipe(lambda x: x.assign(budget_musd=x['budget'].replace(0, np.nan).apply(convert_to_musd)) if 'budget' in x.columns else x)
                .pipe(lambda x: x.assign(revenue_musd=x['revenue'].replace(0, np.nan).apply(convert_to_musd)) if 'revenue' in x.columns else x)
                .pipe(lambda x: x[[col for col in ['id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
                                                   'belongs_to_collection.id', 'belongs_to_collection.name', 'original_language',
                                                   'budget_musd', 'revenue_musd', 'production_companies', 'production_countries',
                                                   'vote_count', 'vote_average', 'popularity', 'runtime', 'overview',
                                                   'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size']
                                  if col in x.columns]])
                .pipe(lambda x: x.drop(columns=[col for col in ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
                                               if col in x.columns], errors='ignore'))
                .pipe(lambda x: x.reset_index(drop=True))
                .pipe(lambda x: logger.info("Filtered for 'Released' movies and dropped 'status' column") or x if 'status' in df.columns else x)
                .pipe(lambda x: logger.info("Converted release_date to datetime") or x if 'release_date' in df.columns else x)
                .pipe(lambda x: logger.info("Converted budget to MUSD and replaced 0 with NaN") or x if 'budget' in df.columns else x)
                .pipe(lambda x: logger.info("Converted revenue to MUSD and replaced 0 with NaN") or x if 'revenue' in df.columns else x)
                .pipe(lambda x: logger.info("Reordered columns and dropped unwanted columns") or x)
                .pipe(lambda x: logger.info("Reset DataFrame index") or x))
    except Exception as e:
        logger.error(f"Failed during preprocessing: {e}")
        return df

def save_processed_data(df: pd.DataFrame, csv_path: str, json_path: str) -> None:
    """Saves the processed DataFrame to CSV and JSON."""
    try:
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create directory for {csv_path}: {e}")
        return
    try:
        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient='records', indent=4)
        logger.info(f"Saved processed data to {csv_path} and {json_path}")
    except Exception as e:
        logger.error(f"Failed to save processed data: {e}")

def calculate_kpis(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Calculates key performance indicators (KPIs) for movie analysis per project instructions."""
    logger.info("Calculating KPIs")
    kpis = {}
    try:
        kpis['top_10_revenue'] = df[['title', 'revenue_musd']].nlargest(10, 'revenue_musd').pipe(
            lambda x: setattr(x, 'commentary', "Identifies the highest-grossing movies, indicating market success and audience reach.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate top_10_revenue: {e}")
        kpis['top_10_revenue'] = pd.DataFrame()
    try:
        kpis['top_10_budget'] = df[['title', 'budget_musd']].nlargest(10, 'budget_musd').pipe(
            lambda x: setattr(x, 'commentary', "Highlights movies with the largest production investments, reflecting studio confidence.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate top_10_budget: {e}")
        kpis['top_10_budget'] = pd.DataFrame()
    try:
        kpis['top_10_profit'] = df[['title', 'revenue_musd', 'budget_musd']].assign(profit=lambda x: x['revenue_musd'] - x['budget_musd']).nlargest(10, 'profit')[['title', 'profit']].pipe(
            lambda x: setattr(x, 'commentary', "Shows movies with the highest profit margins, balancing revenue against costs.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate top_10_profit: {e}")
        kpis['top_10_profit'] = pd.DataFrame()
    try:
        kpis['bottom_10_profit'] = df[['title', 'revenue_musd', 'budget_musd']].assign(profit=lambda x: x['revenue_musd'] - x['budget_musd']).nsmallest(10, 'profit')[['title', 'profit']].pipe(
            lambda x: setattr(x, 'commentary', "Identifies underperforming movies, useful for understanding financial risks.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate bottom_10_profit: {e}")
        kpis['bottom_10_profit'] = pd.DataFrame()
    try:
        kpis['top_10_roi'] = df[df['budget_musd'] >= 10][['title', 'revenue_musd', 'budget_musd']].assign(ROI=lambda x: x['revenue_musd'] / x['budget_musd']).nlargest(10, 'ROI')[['title', 'ROI']].pipe(
            lambda x: setattr(x, 'commentary', "Measures efficiency of investment for high-budget films, highlighting cost-effective successes.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate top_10_roi: {e}")
        kpis['top_10_roi'] = pd.DataFrame()
    try:
        kpis['bottom_10_roi'] = df[df['budget_musd'] >= 10][['title', 'revenue_musd', 'budget_musd']].assign(ROI=lambda x: x['revenue_musd'] / x['budget_musd']).nsmallest(10, 'ROI')[['title', 'ROI']].pipe(
            lambda x: setattr(x, 'commentary', "Identifies least efficient high-budget films, indicating financial underperformance.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate bottom_10_roi: {e}")
        kpis['bottom_10_roi'] = pd.DataFrame()
    try:
        kpis['most_voted'] = df[['title', 'vote_count']][df['vote_count'] == df['vote_count'].max()].pipe(
            lambda x: setattr(x, 'commentary', "Indicates the movie with the highest audience engagement via votes.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate most_voted: {e}")
        kpis['most_voted'] = pd.DataFrame()
    try:
        kpis['highest_rated'] = df[df['vote_count'] >= 10][['title', 'vote_average']].nlargest(10, 'vote_average').pipe(
            lambda x: setattr(x, 'commentary', "Shows top-rated movies with sufficient votes, reflecting critical acclaim.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate highest_rated: {e}")
        kpis['highest_rated'] = pd.DataFrame()
    try:
        kpis['lowest_rated'] = df[df['vote_count'] >= 10][['title', 'vote_average']].nsmallest(10, 'vote_average').pipe(
            lambda x: setattr(x, 'commentary', "Identifies poorly rated movies with sufficient votes, indicating critical failure.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate lowest_rated: {e}")
        kpis['lowest_rated'] = pd.DataFrame()
    try:
        kpis['most_popular'] = df[['title', 'popularity']].nlargest(10, 'popularity').pipe(
            lambda x: setattr(x, 'commentary', "Highlights movies with the highest popularity scores, reflecting audience interest.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate most_popular: {e}")
        kpis['most_popular'] = pd.DataFrame()
    try:
        kpis['uma_tarantino'] = df[df['cast'].str.contains('Uma Thurman', na=False) & df['director'].str.contains('Quentin Tarantino', na=False)][['title', 'director', 'runtime']].sort_values('runtime').pipe(
            lambda x: setattr(x, 'commentary', "Finds collaborations between Uma Thurman and Quentin Tarantino, sorted by runtime.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate uma_tarantino: {e}")
        kpis['uma_tarantino'] = pd.DataFrame()
    try:
        kpis['sci_fi_bruce'] = df[df['genres'].str.contains('Science Fiction', na=False) & df['genres'].str.contains('Action', na=False) & df['cast'].str.contains('Bruce Willis', na=False)][['title', 'vote_average']].nlargest(5, 'vote_average').pipe(
            lambda x: setattr(x, 'commentary', "Identifies top-rated Sci-Fi Action movies starring Bruce Willis.") or x
        )
    except Exception as e:
        logger.error(f"Failed to calculate sci_fi_bruce: {e}")
        kpis['sci_fi_bruce'] = pd.DataFrame()
    return kpis

def analyze_franchises(df: pd.DataFrame) -> pd.DataFrame:
    """Analyzes movie franchises based on number of movies, budget, revenue, and rating."""
    logger.info("Analyzing franchises")
    try:
        franchise_df = df[df['belongs_to_collection.id'].notna()]
        if franchise_df.empty:
            logger.warning("No franchise data available")
            return pd.DataFrame()
        return (franchise_df.groupby('belongs_to_collection.name')
                .agg(movie_count=('id', 'count'), total_budget=('budget_musd', 'sum'),
                     mean_budget=('budget_musd', 'mean'), total_revenue=('revenue_musd', 'sum'),
                     mean_revenue=('revenue_musd', 'mean'), mean_rating=('vote_average', 'mean'))
                .sort_values('total_revenue', ascending=False)
                .pipe(lambda x: setattr(x, 'commentary', "Analyzes franchise performance: movie count shows franchise size, total and mean budget/revenue indicate financial scale and success, and mean rating reflects critical reception.") or x))
    except Exception as e:
        logger.error(f"Failed to analyze franchises: {e}")
        return pd.DataFrame()

def analyze_directors(df: pd.DataFrame) -> pd.DataFrame:
    """Analyzes directors by movie count, total revenue, and mean rating."""
    logger.info("Analyzing directors")
    try:
        return (df[['id', 'director', 'revenue_musd', 'vote_average']]
                .assign(**{'revenue_musd': lambda x: pd.to_numeric(x['revenue_musd'], errors='coerce'),
                           'vote_average': lambda x: pd.to_numeric(x['vote_average'], errors='coerce')})
                .dropna(subset=['director', 'id'])
                .assign(director=lambda x: x['director'].str.split('|'))
                .explode('director')
                .assign(director=lambda x: x['director'].str.strip())
                .query('director != ""')
                .groupby('director')
                .agg(movie_count=('id', 'nunique'), total_revenue=('revenue_musd', 'sum'),
                     mean_rating=('vote_average', 'mean'))
                .pipe(lambda x: setattr(x, 'commentary', "Provides insights into director performance: movie count reflects productivity, total revenue indicates commercial success, and mean rating shows critical reception.") or x))
    except Exception as e:
        logger.error(f"Failed to analyze directors: {e}")
        return pd.DataFrame()

def plot_visualizations(df: pd.DataFrame) -> None:
    """Generates visualizations for data analysis."""
    logger.info("Generating visualizations")
    plots_dir = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')), 'data', 'plots')
    try:
        os.makedirs(plots_dir, exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create plots directory: {e}")
        return
    try:
        df = df.assign(release_date=lambda x: pd.to_datetime(x['release_date'], errors='coerce'),
                       release_year=lambda x: x['release_date'].dt.year.astype('Int64'))
    except Exception as e:
        logger.error(f"Failed to preprocess DataFrame for plotting: {e}")
        return

    # Revenue vs Budget
    try:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df, x='budget_musd', y='revenue_musd', alpha=0.5)
        plt.plot([0, df['budget_musd'].max()], [0, df['budget_musd'].max()], 'r--', label='Break-even')
        plt.title('Revenue vs. Budget')
        plt.xlabel('Budget (MUSD)')
        plt.ylabel('Revenue (MUSD)')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'revenue_vs_budget.png'))
        plt.close()
        logger.info(f"Saved Revenue vs Budget plot to {os.path.join(plots_dir, 'revenue_vs_budget.png')}")
        logger.info("Commentary: This scatter plot shows the relationship between movie budgets and revenues, with the break-even line indicating where revenue equals budget. Points above the line represent profitable movies.")
    except Exception as e:
        logger.error(f"Failed to generate Revenue vs Budget plot: {e}")
        plt.close()

    # ROI by Genre
    try:
        df_roi = df[df['budget_musd'] > 0.1].assign(ROI=lambda y: y['revenue_musd'] / y['budget_musd'])[['genres', 'ROI']]
        df_roi = df_roi.dropna().assign(genres=lambda y: y['genres'].str.split('|')).explode('genres')
        if df_roi.empty:
            logger.warning("No data available for ROI by Genre plot after filtering.")
            plt.close()
            return
        median_roi = df_roi.groupby('genres')['ROI'].median().sort_values(ascending=False).head(15)
        if median_roi.empty:
            logger.warning("No genres with sufficient data for ROI by Genre plot.")
            plt.close()
            return
        plt.figure(figsize=(10, 8))
        median_roi.plot(kind='barh')
        plt.title('Median ROI by Genre (Top 15)')
        plt.xlabel('Median ROI')
        plt.ylabel('Genre')
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'roi_by_genre.png'))
        plt.close()
        logger.info(f"Saved ROI by Genre plot to {os.path.join(plots_dir, 'roi_by_genre.png')}")
        logger.info("Commentary: This bar plot displays the median ROI for the top 15 genres, highlighting which genres yield the highest returns relative to their budgets.")
    except Exception as e:
        logger.error(f"Failed to generate ROI by Genre plot: {e}")
        plt.close()

    # Popularity vs Rating
    try:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df, x='popularity', y='vote_average', alpha=0.5)
        plt.title('Popularity vs. Vote Average')
        plt.xlabel('Popularity Score')
        plt.ylabel('Vote Average')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'popularity_vs_rating.png'))
        plt.close()
        logger.info(f"Saved Popularity vs Rating plot to {os.path.join(plots_dir, 'popularity_vs_rating.png')}")
        logger.info("Commentary: This scatter plot examines the relationship between a movie's popularity score and its average rating, indicating whether popular movies are also critically acclaimed.")
    except Exception as e:
        logger.error(f"Failed to generate Popularity vs Rating plot: {e}")
        plt.close()

    # Yearly Revenue Trends
    try:
        df_yearly = df.dropna(subset=['release_year', 'revenue_musd']).assign(release_year=lambda y: y['release_year'].astype(int))
        yearly_revenue = df_yearly.groupby('release_year')['revenue_musd'].sum()
        plt.figure(figsize=(12, 6))
        yearly_revenue.plot(kind='line', marker='o')
        plt.title('Total Box Office Revenue Over Years')
        plt.xlabel('Release Year')
        plt.ylabel('Total Revenue (MUSD)')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'yearly_revenue_trends.png'))
        plt.close()
        logger.info(f"Saved Yearly Revenue Trends plot to {os.path.join(plots_dir, 'yearly_revenue_trends.png')}")
        logger.info("Commentary: This line plot shows the total box office revenue over years, revealing trends in the movie industry's financial performance.")
    except Exception as e:
        logger.error(f"Failed to generate Yearly Revenue Trends plot: {e}")
        plt.close()

    # Franchise vs Standalone Revenue
    try:
        df_franchise = df.dropna(subset=['revenue_musd']).assign(is_franchise=lambda y: y['belongs_to_collection.id'].notna())
        plt.figure(figsize=(8, 6))
        sns.boxplot(x='is_franchise', y='revenue_musd', data=df_franchise, showfliers=False)
        plt.title('Revenue Distribution: Franchise vs. Standalone Movies')
        plt.xlabel('Is Part of Franchise?')
        plt.ylabel('Revenue (MUSD)')
        plt.xticks([0, 1], ['Standalone', 'Franchise'])
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'franchise_vs_standalone.png'))
        plt.close()
        logger.info(f"Saved Franchise vs Standalone Revenue plot to {os.path.join(plots_dir, 'franchise_vs_standalone.png')}")
        logger.info("Commentary: This box plot compares the revenue distribution of franchise movies (part of a collection) versus standalone movies, highlighting differences in financial performance.")
    except Exception as e:
        logger.error(f"Failed to generate Franchise vs Standalone Revenue plot: {e}")
        plt.close()