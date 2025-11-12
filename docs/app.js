async function main() {
  document.getElementById("y").textContent = new Date().getFullYear();

  const res = await fetch("./data/recs.json", { cache: "no-store" });
  if (!res.ok) throw new Error("تعذّر تحميل data/recs.json");
  const recs = await res.json();

  const users = Object.keys(recs).map(Number).sort((a,b)=>a-b);
  const userSelect = document.getElementById("userSelect");
  users.forEach(u => {
    const opt = document.createElement("option");
    opt.value = u; opt.textContent = u;
    userSelect.appendChild(opt);
  });

  const cards = document.getElementById("cards");
  const search = document.getElementById("search");

  function render(uid) {
    const q = (search.value || "").trim().toLowerCase();
    cards.innerHTML = "";
    (recs[uid] || []).slice(0,50)
      .filter(r => !q || String(r.title).toLowerCase().includes(q))
      .slice(0,10)
      .forEach(r => {
        const el = document.createElement("div");
        el.className = "card";
        el.innerHTML = `
          <h3>${r.title}</h3>
          <div class="badge">Movie ID: ${r.movie_id}</div>
          <p>تقييم متوقّع: <span class="score">${Number(r.score).toFixed(2)}</span></p>`;
        cards.appendChild(el);
      });
  }

  userSelect.addEventListener("change", ()=>render(Number(userSelect.value)));
  search.addEventListener("input", ()=>render(Number(userSelect.value)));

  render(users[0]);
}

main().catch(err=>{
  console.error(err);
  document.body.innerHTML =
    "<p style='padding:20px'>حدث خطأ في تحميل البيانات. تأكد من وجود <code>docs/data/recs.json</code>.</p>";
});
async function main() {
  document.getElementById("y").textContent = new Date().getFullYear();

  const res = await fetch("./data/recs.json", { cache: "no-store" });
  if (!res.ok) throw new Error("تعذّر تحميل data/recs.json");
  const recs = await res.json();

  const users = Object.keys(recs).map(Number).sort((a,b)=>a-b);
  const userSelect = document.getElementById("userSelect");
  users.forEach(u => {
    const opt = document.createElement("option");
    opt.value = u; opt.textContent = u;
    userSelect.appendChild(opt);
  });

  const cards = document.getElementById("cards");
  const search = document.getElementById("search");

  function render(uid) {
    const q = (search.value || "").trim().toLowerCase();
    cards.innerHTML = "";
    (recs[uid] || []).slice(0,50)
      .filter(r => !q || String(r.title).toLowerCase().includes(q))
      .slice(0,10)
      .forEach(r => {
        const el = document.createElement("div");
        el.className = "card";
        el.innerHTML = `
          <h3>${r.title}</h3>
          <div class="badge">Movie ID: ${r.movie_id}</div>
          <p>تقييم متوقّع: <span class="score">${Number(r.score).toFixed(2)}</span></p>`;
        cards.appendChild(el);
      });
  }

  userSelect.addEventListener("change", ()=>render(Number(userSelect.value)));
  search.addEventListener("input", ()=>render(Number(userSelect.value)));

  render(users[0]);
}

main().catch(err=>{
  console.error(err);
  document.body.innerHTML =
    "<p style='padding:20px'>حدث خطأ في تحميل البيانات. تأكد من وجود <code>docs/data/recs.json</code>.</p>";
});
