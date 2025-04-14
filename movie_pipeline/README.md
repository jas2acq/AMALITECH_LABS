# TMDB Movie Data Analysis using Pandas and APIs

## Project Overview

This project analyzes movie data fetched from the TMDb API using Python and Pandas. The pipeline includes data extraction, cleaning, transformation, and the implementation of key performance indicators (KPIs), focusing on director analysis and overall movie performance.

## Project Objectives

-   Fetch movie data from the TMDb API.
-   Clean and preprocess the fetched data for analysis.
-   Implement director performance analysis as a key KPI.
-   Identify top and bottom performing movies based on various metrics.
-   Present key findings concisely.

## Project Steps

1.  **Fetch Movie Data from API:** Extracts raw movie data using provided IDs.
2.  **Data Cleaning and Preprocessing:** Transforms and cleans the data using various Pandas operations.
3.  **KPI Implementation & Analysis:** Focuses on director analysis, calculating metrics like movie count, total revenue, and mean rating.
4.  **Data Visualization:** Generates visualizations such as Revenue vs. Budget, Median ROI by Genre, and Popularity vs. Vote Average.

## Key Findings

-   **Most Popular Movie:** Titanic has the highest popularity score in the dataset.
-   **Highest Voted Movie:** Avatar received the most votes.
-   **Top Directors:** James Cameron directed movies with the highest total revenue. Anthony and Joe Russo also show strong performance.
-   **Highest ROI:** Avatar demonstrates the highest Return on Investment for movies with a budget over $10 million.
-   **Most Profitable Movies:** Avatar and Avengers: Endgame generated the highest profits.
-   **Top Revenue Movies:** Avatar and Avengers: Endgame lead in terms of revenue.

## Usage

1.  Ensure Python and required libraries are installed.
2.  Obtain and configure a TMDb API key.
3.  Run the provided scripts to fetch, process, and analyze the data.

## Dependencies

-   pandas
-   requests
-   os
-   dotenv
-   regex
-   matplotlib
-   seaborn
-   warnings
-   datetime
-   pytz

## Configuration

API key should be configured using the `.env` file.

**Installation Steps:**

1.  **Clone the repository** (if you haven't already).
2.  **Navigate to the project directory** in your terminal.
3.  **Install the required Python libraries** using the command:
    ```bash
    pip install -r requirements.txt
    ```
Once you have these prerequisites installed, you should be able to run the project scripts.