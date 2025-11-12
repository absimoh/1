# Joins users, movies, and ratings

import apache_beam as beam
import csv
from collections import defaultdict

def group_user_by_zip(joined_data):
    zip_groups = defaultdict(list)
    for row in joined_data:
        zip_groups[row['zip_code']].append(row['user_id'])
    return {zip_code: list(set(users)) for zip_code, users in zip_groups.items()}

def group_user_by_age(joined_data):
    age_group = defaultdict(list)
    for row in joined_data:
        age_group[row['age'].append(row['user_id'])]
    return {age: list(set(users)) for age, users in age_group.items()}

def group_users_by_favorite_genres(joined_data, min_rating = 1):
    genre_users = defaultdict(list)
    for row in joined_data:
        if float(row['rating']) >= min_rating:
            genres = row['genres'].split("|")
            for g in genres:
                genre_users[g].append(row["user_id"])
    return {genre: list(set(users)) for genre, users in genre_users.items()}
# output example:  {'Animation': [1,3], "Children's": [1,3], 'Comedy': [1,3], 'Adventure': [2]}

def join_ratings_with_movies(ratings_file, movies_file):
    movie_dict = {}
    with open(movies_file, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            movie_dict[row["movie_id"]] = row['title']
    
    joined_data = []
    with open(ratings_file, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            movie_title = movie_dict.get(row['movie_id'], "Unknown")
            joined_data.append({
                'movie_id': row['movie_id'],
                'movie_title': movie_title,
                'rating': float(row['rating'])
            })
    return joined_data