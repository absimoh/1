import pandas as pd
import streamlit as st

st.set_page_config(page_title="Movie Recommender (ML-1M)", layout="wide")

@st.cache_data
def load_data():
    # ملفاتك الناتجة من البايبلاين والـ Top-N
    df = pd.read_csv("output/joined.csv", low_memory=False, dtype={"user_id":"int32","movie_id":"int32","rating":"float32"})
    recs = pd.read_csv("output/recommendations_top10.csv")
    # قاموس id -> title
    id2title = dict(zip(df["movie_id"], df["title"]))
    recs["title"] = recs["title"].fillna(recs["movie_id"].astype(str))
    return df, recs, id2title

df, recs, id2title = load_data()

st.title("🎬 MovieLens 1M — توصيات الأفلام")
st.caption("نموذج توصية SVD مبني على MovieLens-1M (تقييمات 1..5)")

# اختيار المستخدم
users = sorted(df["user_id"].unique().tolist())
uid = st.selectbox("اختر مستخدم", options=users, index=0)

# عرض الأفلام التي شاهدها + تقييماته
st.subheader("📼 الأفلام التي قيّمها المستخدم")
watched = df.loc[df.user_id == uid, ["movie_id","title","rating"]].drop_duplicates()
st.write(f"عددها: {len(watched)}")
st.dataframe(watched.sort_values("rating", ascending=False).reset_index(drop=True), use_container_width=True)

# عرض أفضل 10 توصيات المخزّنة
st.subheader("⭐ أفضل 10 توصيات")
topn = recs.loc[recs.user_id == uid, ["movie_id","title","score"]].sort_values("score", ascending=False).head(10)
# عناوين احتياطية لو العنوان ناقص
topn["title"] = topn.apply(lambda r: r["title"] if pd.notnull(r["title"]) else id2title.get(r["movie_id"], str(r["movie_id"])), axis=1)
st.dataframe(topn.reset_index(drop=True), use_container_width=True)

# فلترة حسب نوع (genres) — اختياري لو حابّ
st.subheader("🎯 فلترة بالتاجز (Genres) - اختياري")
# لاحظ: genres نص متعدد بأنواع مفصولة |
all_genres = set()
for g in df["genres"].dropna().astype(str):
    for x in g.split("|"):
        if x.strip():
            all_genres.add(x.strip())
sel = st.multiselect("اختر نوع/أنواع", sorted(all_genres))
if sel:
    mask = topn["movie_id"].isin(df[df["genres"].fillna("").str.contains("|".join(sel), regex=True)]["movie_id"])
    st.write("نتائج بعد الفلترة:")
    st.dataframe(topn[mask].reset_index(drop=True), use_container_width=True)

st.caption("البيانات: MovieLens-1M. الخوارزمية: SVD من Surprise.")
