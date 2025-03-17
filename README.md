1. Build the Docker Image

Open a terminal in the project directory and run:

docker build -t streamlit-scraper .

2. Run the Container

Once the image is built, start the container with:

docker run -p 8501:8501 --name streamlit-scraper-container streamlit-scraper

This maps port 8501 (default for Streamlit) so you can access the app in your browser.

3. Access the App

Open your browser and go to:

http://localhost:8501

