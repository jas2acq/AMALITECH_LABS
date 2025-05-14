# Setup Guide

## Prerequisites
- **Python 3.8+**: Ensure Python is installed on your system.
- **Git** (optional): For cloning the repository if hosted.
- **Internet Connection**: Required to fetch data from the TMDb API.

## Installation

1. **Clone the Repository** (if applicable):
   ```
   git clone <repository-url>
   cd New_Lab_1
   ```

2. **Set Up a Virtual Environment**:
   - Create a virtual environment:
     ```
     python -m venv .venv
     ```
   - Activate the virtual environment:
     - Windows: `.venv\Scripts\activate`
     - macOS/Linux: `source .venv/bin/activate`

3. **Install Dependencies**:
   - Install the required Python packages:
     ```
     pip install -r requirements.txt
     ```
   - The `requirements.txt` should include:
     ```
     requests==2.28.1
     pandas==2.0.3
     matplotlib==3.7.1
     seaborn==0.12.2
     python-dotenv==1.0.0
     ```

4. **Configure the API Key**:
   - Obtain an API key from [TMDb](https://www.themoviedb.org/documentation/api).
   - Create a `.env` file in the project root with the following content:
     ```
     movie_api_key=your_tmdb_api_key_here
     ```
   - Replace `your_tmdb_api_key_here` with your actual TMDb API key. Keep this file out of version control (add `.env` to `.gitignore` if using Git).

5. **Verify Directory Structure**:
   - Ensure the `data`, `logs`, `src`, and `plots` directories exist or are created automatically on the first run.

## Running the Pipeline
1. **Activate the Virtual Environment** (if not already active):
   - Windows: `.venv\Scripts\activate`
   - macOS/Linux: `source .venv/bin/activate`

2. **Execute the Pipeline**:
   - Run the main script from the project root:
     ```
     python src/main.py
     ```
   - Output will be logged to `logs/main.log` and the console, with data saved to `data/` and plots to `data/plots/`.

## Troubleshooting
- **API Key Issues**: If you see an error about a missing API key, verify the `.env` file and its contents.
- **File Permissions**: Ensure write permissions for the `data` and `logs` directories.
- **Dependency Errors**: Update `pip` (`pip install --upgrade pip`) and reinstall dependencies if issues arise.