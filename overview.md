# Project Overview

This project is a Python-based data pipeline designed to fetch, process, and analyze movie data from The Movie Database (TMDb) API. It retrieves information for a predefined list of movies, preprocesses the data, calculates key performance indicators (KPIs), analyzes director and franchise performance, and generates visualizations. The pipeline is structured to handle errors gracefully and log all operations for debugging and auditing purposes.

## Key Features
- Fetches movie data using the TMDb API.
- Preprocesses data by cleaning and transforming columns (e.g., converting budgets and revenues to millions USD).
- Calculates KPIs such as top revenue, profit, ROI, and custom metrics (e.g., Uma Thurman/Quentin Tarantino collaborations).
- Analyzes director productivity and franchise performance.
- Generates visualizations (e.g., Revenue vs. Budget, ROI by Genre) using Matplotlib and Seaborn.
- Logs all operations to a file and console with a custom logging setup.

## Project Structure
```
New_Lab_1/
├── data/
│   ├── raw/                # Raw data files (CSV, JSON)
│   ├── processed/          # Processed data files (CSV, JSON)    
│   └── plots/              # Generated visualization images
├── logs/
├── src/
│   ├── main.py             # Main script to run the pipeline
│   └── utility.py          # Utility functions for data fetching, processing, and analysis
├── .env                    # Environment file for API key
├── requirements.txt        # Python dependencies            
├── README.md               # Main README file (links to other docs)
├── overview.md
├── setup-guide.md
└── documentation.md        
```