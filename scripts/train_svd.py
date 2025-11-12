# scripts/train_svd.py
import pandas as pd
from surprise import SVD, Dataset, Reader
from surprise.model_selection import train_test_split
from surprise.accuracy import rmse

# 1) حمّل البيانات الموحّدة
df = pd.read_csv(
    "output/joined.csv",
    low_memory=False,
    dtype={"user_id": "int32", "movie_id": "int32", "rating": "float32"}
)


# 2) Surprise تحتاج أعمدة: user_id, movie_id, rating
reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(df[["user_id", "movie_id", "rating"]], reader)

# 3) قسّم البيانات ودرّب نموذج SVD
trainset, testset = train_test_split(data, test_size=0.2, random_state=42)
model = SVD()
model.fit(trainset)

# 4) قيّم النموذج
preds = model.test(testset)
rmse(preds, verbose=True)

# 5) مثال: توقّع تقييم لمستخدم/فيلم
print(model.predict(uid=1, iid=1193))

# 6) Top-N توصيات لمستخدم معيّن
id2title = dict(zip(df["movie_id"], df["title"]))

def top_n_for_user(user_id, n=10):
    all_items = df["movie_id"].unique()
    seen = set(df.loc[df.user_id==user_id, "movie_id"])
    candidates = [iid for iid in all_items if iid not in seen]
    ests = [(iid, model.predict(uid=user_id, iid=iid).est) for iid in candidates]
    ests.sort(key=lambda x: x[1], reverse=True)
    topn = ests[:n]
    # ارجع (العنوان، التقدير)
    return [(id2title.get(iid, str(iid)), est) for iid, est in topn]

print("Top-10 (titles) for user 1:", top_n_for_user(1, 10))