# scripts/export_recs.py
import os
import pandas as pd
from surprise import SVD, Dataset, Reader
from tqdm import tqdm

os.makedirs("output", exist_ok=True)

df = pd.read_csv(
    "output/joined.csv",
    low_memory=False,
    dtype={"user_id": "int32", "movie_id": "int32", "rating": "float32"}
)

reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(df[["user_id","movie_id","rating"]], reader)

# درّب على كامل البيانات لعمل توصيات نهائية
trainset = data.build_full_trainset()
model = SVD()
model.fit(trainset)

id2title = dict(zip(df["movie_id"], df["title"]))
all_items = df["movie_id"].drop_duplicates().tolist()

rows = []
users = df["user_id"].drop_duplicates().tolist()

for uid in tqdm(users, desc="Recommending"):
    seen = set(df.loc[df.user_id==uid, "movie_id"])
    # لتسريع التنفيذ ممكن تقيد عدد المرشحين (اختياري):
    # candidates = all_items[:5000]
    candidates = [iid for iid in all_items if iid not in seen]
    # تقديرات
    ests = [(iid, model.predict(uid=uid, iid=iid).est) for iid in candidates]
    ests.sort(key=lambda x: x[1], reverse=True)
    topn = ests[:10]
    for iid, score in topn:
        rows.append((uid, iid, id2title.get(iid, str(iid)), round(score, 4)))

out = pd.DataFrame(rows, columns=["user_id","movie_id","title","score"])
out.to_csv("output/recommendations_top10.csv", index=False)
print("Wrote output/recommendations_top10.csv with", len(out), "rows")
