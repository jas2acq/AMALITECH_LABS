# Documentation

## Main Components
- **`main.py`**: Orchestrates the pipeline, configuring logging and calling utility functions.
- **`utility.py`**: Contains functions for:
  - `fetch_movie_data`: Retrieves movie data from TMDb.
  - `save_raw_data` and `save_processed_data`: Saves data to CSV and JSON.
  - `preprocess_data`: Cleans and transforms the DataFrame.
  - `calculate_kpis`: Computes KPIs (e.g., top revenue, ROI).
  - `analyze_directors` and `analyze_franchises`: Analyzes performance metrics.
  - `plot_visualizations`: Generates plots (Revenue vs. Budget, ROI by Genre, etc.).

## Logging
- Logs are written to `logs/main.log` and the console with the format: `%(asctime)s - %(levelname)s - %(message)s`.
- Levels include `INFO`, `WARNING`, `ERROR` for tracking progress and issues.

## KPIs
- **top_10_revenue**: Highest-grossing movies.
- **top_10_budget**: Movies with the largest budgets.
- **top_10_profit**: Movies with the highest profit margins.
- **bottom_10_profit**: Underperforming movies.
- **top_10_roi**: Best return on investment for high-budget films.
- **bottom_10_roi**: Least efficient high-budget films.
- **most_voted**: Movie with the highest vote count.
- **highest_rated**: Top-rated movies with sufficient votes.
- **lowest_rated**: Poorly rated movies with sufficient votes.
- **most_popular**: Movies with the highest popularity scores.
- **uma_tarantino**: Uma Thurman and Quentin Tarantino collaborations (currently empty due to missing relevant movie IDs).
- **sci_fi_bruce**: Top-rated Sci-Fi Action movies with Bruce Willis (currently empty due to missing relevant movie IDs).

## Visualizations
- **Revenue vs Budget**: Scatter plot with a break-even line.
- **ROI by Genre**: Bar plot of median ROI for top 15 genres.
- **Popularity vs Rating**: Scatter plot of popularity vs. vote average.
- **Yearly Revenue Trends**: Line plot of total revenue over years.
- **Franchise vs Standalone Revenue**: Box plot comparing revenue distributions.

## Customization
- **Add Movie IDs**: Modify the `movie_ids` list in `main.py` to include additional movies (e.g., `680` for *Pulp Fiction*).
- **Adjust KPIs**: Update `calculate_kpis` in `utility.py` to add or modify metrics.
- **Enhance Visualizations**: Modify `plot_visualizations` to add new plots or customize existing ones.

## Contributing
- Fork the repository, create a feature branch, and submit a pull request.
- Ensure code follows PEP 8 style guidelines.
- Add tests and update documentation as needed.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details (add a LICENSE file if desired).

## Contact
For questions or support, contact [your-email@example.com] (replace with your email).