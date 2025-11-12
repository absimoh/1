import apache_beam as beam
from apache_beam.coders import BytesCoder
import os
import logging

# ----------------------------
# Configure Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

VALID_GENRES = {
    "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
    "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
    "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"
}


def file_exists_and_same(path, expected_lines):
    """Check if a processed file already exists and matches expected line count."""
    if not os.path.exists(path):
        return False
    try:
        with open(path, 'r', encoding='ISO-8859-1') as f:
            lines = sum(1 for _ in f)
        return lines == expected_lines
    except Exception as e:
        logging.warning(f"Failed to verify {path}: {e}")
        return False


def parse_rating(line):
    try:
        user_id, movie_id, rating, timestamp = line.split("::")
        return {
            'user_id': int(user_id),
            'movie_id': int(movie_id),
            'rating': float(rating),
            'timestamp': int(timestamp)
        }
    except Exception as e:
        logging.warning(f"Skipping bad rating line: {line} ({e})")
        return None


def parse_user(line):
    try:
        user_id, gender, age, occupation, zip_code = line.split("::")
        return {
            'user_id': int(user_id),
            'gender': gender,
            'age': int(age),
            'occupation': int(occupation),
            'zip_code': zip_code
        }
    except Exception as e:
        logging.warning(f"Skipping bad user line: {line} ({e})")
        return None


def parse_movie(line):
    parts = line.strip().split("::")
    if len(parts) < 3:
        logging.warning(f"Malformed movie line: {line}")
        return None

    try:
        movie_id = int(parts[0])
    except ValueError:
        logging.warning(f"Invalid movie ID in line: {line}")
        return None

    title = "::".join(parts[1:-1])
    genres = [g for g in parts[-1].split("|") if g in VALID_GENRES]

    return {
        'movie_id': movie_id,
        'title': title,
        'genres': genres
    }


def count_lines(file_path):
    try:
        with open(file_path, 'r', encoding='ISO-8859-1') as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return 0


def run():
    logging.info("Starting data preprocessing pipeline...")

    os.makedirs("data/processed", exist_ok=True)

    input_rating = "data/raw/ratings.dat"
    input_users = "data/raw/users.dat"
    input_movies = "data/raw/movies.dat"

    output_rating = "data/processed/parsed_ratings"
    output_users = "data/processed/parsed_users"
    output_movies = "data/processed/parsed_movies"

    # Get line counts for raw data
    rating_lines_count = count_lines(input_rating)
    users_lines_count = count_lines(input_users)
    movies_lines_count = count_lines(input_movies)

    to_process = {
        "ratings": not file_exists_and_same(f"{output_rating}-00000-of-00001.csv", rating_lines_count),
        "users": not file_exists_and_same(f"{output_users}-00000-of-00001.csv", users_lines_count),
        "movies": not file_exists_and_same(f"{output_movies}-00000-of-00001.csv", movies_lines_count)
    }

    with beam.Pipeline() as pipeline:
        if to_process["ratings"]:
            logging.info("Processing ratings data...")
            rating_lines = (
                pipeline
                | "Read Ratings" >> beam.io.ReadFromText(input_rating, coder=BytesCoder())
                | "Decode Ratings (latin1)" >> beam.Map(lambda b: b.decode("latin-1"))
            )
            rating_parsed = (
                rating_lines
                | "Parse Ratings" >> beam.Map(parse_rating)
                | "Filter Invalid Ratings" >> beam.Filter(lambda r: r is not None)
            )
            rating_csv = rating_parsed | "Ratings to CSV" >> beam.Map(
                lambda d: f"{d['user_id']},{d['movie_id']},{d['rating']},{d['timestamp']}"
            )
            rating_csv | "Write Ratings" >> beam.io.WriteToText(output_rating, file_name_suffix=".csv")
        else:
            logging.info(f"{output_rating} already exists and is up-to-date. Skipping processing.")

        if to_process["users"]:
            logging.info("Processing users data...")
            users_lines = (
                pipeline
                | "Read Users" >> beam.io.ReadFromText(input_users, coder=BytesCoder())
                | "Decode Users (latin1)" >> beam.Map(lambda b: b.decode("latin-1"))
            )
            user_parsed = (
                users_lines
                | "Parse Users" >> beam.Map(parse_user)
                | "Filter Invalid Users" >> beam.Filter(lambda u: u is not None)
            )
            users_csv = user_parsed | "Users to CSV" >> beam.Map(
                lambda d: f"{d['user_id']},{d['gender']},{d['age']},{d['occupation']},{d['zip_code']}"
            )
            users_csv | "Write Users" >> beam.io.WriteToText(output_users, file_name_suffix=".csv")
        else:
            logging.info(f"{output_users} already exists and is up-to-date. Skipping processing.")

        if to_process["movies"]:
            logging.info("Processing movies data...")
            movies_lines = (
                pipeline
                | "Read Movies" >> beam.io.ReadFromText(input_movies, coder=BytesCoder())
                | "Decode Movies (latin1)" >> beam.Map(lambda b: b.decode("latin-1"))
            )
            movies_parsed = (
                movies_lines
                | "Parse Movies" >> beam.Map(parse_movie)
                | "Filter Invalid Movies" >> beam.Filter(lambda m: m is not None)
            )
            movies_csv = movies_parsed | "Movies to CSV" >> beam.Map(
                lambda d: f"{d['movie_id']},{d['title']},{'|'.join(d['genres'])}"
            )
            movies_csv | "Write Movies" >> beam.io.WriteToText(output_movies, file_name_suffix=".csv")
        else:
            logging.info(f"{output_movies} already exists and is up-to-date. Skipping processing.")

    logging.info("Pipeline completed successfully.")
