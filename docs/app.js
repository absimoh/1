async function loadRecs() {
  const res = await fetch("./data/recs.json", { cache: "no-store" });
  if (!res.ok) throw new Error("تعذر تحميل data/recs.json");
  return res.json();
}

function computeTop5(recs) {
  // تجميع أفلام: movie_id -> {title,sum,count}
  const agg = new Map();
  for (const uid of Object.keys(recs)) {
    for (const r of recs[uid]) {
      const key = String(r.movie_id);
      if (!agg.has(key)) agg.set(key, { title: r.title, sum: 0, count: 0 });
      const a = agg.get(key);
      a.sum += Number(r.score) || 0;
      a.count += 1;
    }
  }
  const arr = [];
  for (const [movie_id, a] of agg.entries()) {
    arr.push({
      movie_id: Number(movie_id),
      title: a.title,
      avg: a.sum / a.count,
      count: a.count
    });
  }
  // ترتيب تنازلي حسب المتوسط ثم حسب عدد مرات الظهور (لدعم الاستقرار)
  arr.sort((x, y) => y.avg - x.avg || y.count - x.count);
  return arr.slice(0, 5);
}

function renderTop5(list) {
  const host = document.getElementById("top5");
  host.innerHTML = "";
  list.forEach((m, i) => {
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <h3><span class="rank">#${i + 1}</span> ${m.title}</h3>
      <div class="badge">Movie ID: ${m.movie_id}</div>
      <p>متوسط التقييم المتوقع: <span class="score">${m.avg.toFixed(2)}</span>
         <span class="muted">— ظهر لدى ${m.count} مستخدم</span></p>
    `;
    host.appendChild(card);
  });
}

function setupUserUI(recs) {
  const users = Object.keys(recs).map(Number).sort((a, b) => a - b);
  const userSelect = document.getElementById("userSelect");
  const search = document.getElementById("search");
  const cards = document.getElementById("cards");

  users.forEach(u => {
    const opt = document.createElement("option");
    opt.value = u; opt.textContent = u;
    userSelect.appendChild(opt);
  });

  function render(uid) {
    const q = (search.value || "").trim().toLowerCase();
    cards.innerHTML = "";
    (recs[uid] || [])
      .filter(r => !q || String(r.title).toLowerCase().includes(q))
      .slice(0, 10) // أفضل 10 للمستخدم
      .forEach(r => {
        const el = document.createElement("div");
        el.className = "card";
        el.innerHTML = `
          <h3>${r.title}</h3>
          <div class="badge">Movie ID: ${r.movie_id}</div>
          <p>التقييم المتوقع: <span class="score">${Number(r.score).toFixed(2)}</span></p>
        `;
        cards.appendChild(el);
      });
  }

  userSelect.addEventListener("change", () => render(Number(userSelect.value)));
  search.addEventListener("input", () => render(Number(userSelect.value)));
  render(users[0]); // أول مستخدم افتراضيًا
}

async function main() {
  document.getElementById("y").textContent = new Date().getFullYear();
  const recs = await loadRecs();
  const top5 = computeTop5(recs);
  renderTop5(top5);
  setupUserUI(recs);
}

main().catch(err => {
  console.error(err);
  document.body.innerHTML = "<p style='padding:20px'>حدث خطأ في تحميل البيانات. تأكد من وجود <code>docs/data/recs.json</code>.</p>";
});
