# scripts/make_joined.py
import os, glob, csv
import pandas as pd

os.makedirs("output", exist_ok=True)

ratings_parts = sorted(glob.glob("data/processed/parsed_ratings-*.csv"))
users_parts   = sorted(glob.glob("data/processed/parsed_users-*.csv"))
movies_parts  = sorted(glob.glob("data/processed/parsed_movies-*.csv"))

if not ratings_parts or not users_parts or not movies_parts:
    raise SystemExit("لم أعثر على ملفات parsed_* داخل data/processed/. تأكد من المسارات.")

# 1) اقرأ ratings و users بشكل عادي
ratings = pd.concat(
    (pd.read_csv(p, header=None, names=["user_id","movie_id","rating","timestamp"]) for p in ratings_parts),
    ignore_index=True
)
users = pd.concat(
    (pd.read_csv(p, header=None, names=["user_id","gender","age","occupation","zip_code"]) for p in users_parts),
    ignore_index=True
)

# 2) اقرأ movies بس بشكل "يدوي" يدعم العناوين اللي فيها فواصل
def read_movies_parts(paths):
    rows = []
    for path in paths:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                # نفصل أول فاصلة (movie_id, باقي السطر)
                try:
                    movie_id_str, rest = line.split(",", 1)
                except ValueError:
                    # سطر غير متوقع
                    continue
                # نفصل آخر فاصلة (العنوان، الأنواع)
                if "," in rest:
                    title, genres = rest.rsplit(",", 1)
                else:
                    title, genres = rest, ""
                try:
                    movie_id = int(movie_id_str)
                except ValueError:
                    continue
                rows.append((movie_id, title, genres))
    return pd.DataFrame(rows, columns=["movie_id","title","genres"])

movies = read_movies_parts(movies_parts)

# 3) الدمج
df = ratings.merge(users,  on="user_id",  how="left") \
            .merge(movies, on="movie_id", how="left")

# 4) الترتيب والحفظ
cols = ["user_id","movie_id","rating","timestamp","title","genres","gender","age","occupation","zip_code"]
df = df[cols]
out_path = "output/joined.csv"
df.to_csv(out_path, index=False)
print(f"Wrote {out_path} with {len(df):,} rows")
