import { useState, useEffect, useCallback, useMemo, useRef } from "react";

const gold = "#c9ab63", muted = "#8a9e8e", deep = "#0a1a12";
const sans = "'Source Sans 3', system-ui, sans-serif";
const serif = "'Cormorant Garamond', 'Amiri', Georgia, serif";
const arFont = "'Amiri', 'Traditional Arabic', serif";

const BATCH_SIZE = 12;

const SYSTEM_PROMPT = `You are an expert in Shia Islamic hadith sciences (ʿilm al-rijāl). You are processing entries from "al-Mufīd min Muʿjam Rijāl al-Ḥadīth" by Muḥammad al-Jawāhirī, which summarizes Sayyid al-Khoei's rijāl assessments.

For each entry, extract ALL of the following into a JSON object:

- "name_ar": Full Arabic name as written
- "name_en": Best English transliteration of the name
- "father": Father's name if mentioned (via "بن" = ibn/son of)
- "grandfather": Grandfather's name if mentioned
- "nasab": Full lineage chain if more than father (e.g., "ibn X ibn Y ibn Z")
- "kunyah": Kunyah/teknonym if present (e.g., "أبو الحسين" = Abu al-Husayn)
- "laqab": Nickname/title if present (e.g., "اللؤلؤي", "الأحمر", "الكوفي" as nisba)
- "nisba": Geographic or tribal attribution (e.g., "الكوفي" = Kufi, "القمي" = Qummi, "الأشعري" = Ash'ari)
- "status": One of: "thiqah" (ثقة), "majhul" (مجهول), "daif" (ضعيف), "mamduh" (ممدوح/praised), "hasan" (حسن), "muwaththaq" (موثق), or "unspecified" if no explicit ruling
- "status_detail": The exact Arabic phrase for their status ruling if given
- "narrates_from_imams": Array of Imam names they narrate from (e.g., ["الصادق", "الباقر"])
- "companions_of": Which Imam they are listed as companion of, if stated
- "books": Array of hadith books they appear in (e.g., ["الكافي", "التهذيب", "الفقيه", "الاستبصار"])
- "hadith_count": Number of narrations if mentioned (e.g., "روى ٤٢ رواية" → 42)
- "aliases": Array of other names/entries this person is identical to (from "متحد مع" = united with)
- "cross_refs": Array of entry numbers they are cross-referenced with
- "has_book": true if "له كتاب" or "له أصل" is mentioned
- "tariq_status": Status of the chain to them if mentioned (e.g., "ضعيف" = weak, "صحيح" = sound)
- "notes": Any other important information (brief)

Respond ONLY with a JSON array of objects. No explanation, no markdown, no backticks.
If a field has no data, use null for strings, [] for arrays, null for numbers, false for booleans.`;

function buildPrompt(entries) {
  const items = entries.map((e, i) =>
    `[${i}] Entry ${e.n3}: ${e.text}`
  ).join("\n\n");
  return `Extract structured rijāl data from these ${entries.length} entries:\n\n${items}`;
}

async function processAIBatch(entries) {
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "claude-sonnet-4-20250514",
      max_tokens: 4000,
      system: SYSTEM_PROMPT,
      messages: [{ role: "user", content: buildPrompt(entries) }],
    }),
  });
  const data = await res.json();
  const text = data.content?.map(c => c.text || "").join("") || "";
  try {
    return JSON.parse(text.replace(/```json|```/g, "").trim());
  } catch (e) {
    console.error("Parse error:", e, "Raw:", text.slice(0, 500));
    return null;
  }
}

// Storage
const DB_KEY = "rijal-db";
const PROGRESS_KEY = "rijal-progress";

async function loadDB() {
  try { const r = await window.storage.get(DB_KEY); return r ? JSON.parse(r.value) : {}; } catch { return {}; }
}
async function saveDB(db) {
  try { await window.storage.set(DB_KEY, JSON.stringify(db)); } catch (e) { console.error("Save error:", e); }
}
async function loadProgress() {
  try { const r = await window.storage.get(PROGRESS_KEY); return r ? JSON.parse(r.value) : { lastIdx: -1 }; } catch { return { lastIdx: -1 }; }
}
async function saveProgress(p) {
  try { await window.storage.set(PROGRESS_KEY, JSON.stringify(p)); } catch {}
}

// Status colors
const STATUS_COLORS = {
  thiqah: { bg: "#0d7a3e", label: "Thiqah (Trustworthy)" },
  majhul: { bg: "#6b6b6b", label: "Majhūl (Unknown)" },
  daif: { bg: "#a0522d", label: "Ḍaʿīf (Weak)" },
  mamduh: { bg: "#2a7ab5", label: "Mamdūḥ (Praised)" },
  hasan: { bg: "#2a7ab5", label: "Ḥasan (Good)" },
  muwaththaq: { bg: "#7b6b2e", label: "Muwaththaq (Reliable)" },
  unspecified: { bg: "#444", label: "Unspecified" },
};

/* ════════════════════ MAIN APP ════════════════════ */
export default function RijalBuilder() {
  const [entries, setEntries] = useState(null);
  const [db, setDb] = useState({});
  const [progress, setProgress] = useState({ lastIdx: -1 });
  const [loading, setLoading] = useState(true);
  const [view, setView] = useState("process"); // process, browse
  const [error, setError] = useState("");

  useEffect(() => {
    const l = document.createElement("link");
    l.href = "https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,600;0,700;1,400&family=Source+Sans+3:wght@300;400;600;700&family=Amiri:wght@400;700&display=swap";
    l.rel = "stylesheet"; document.head.appendChild(l);
    return () => document.head.removeChild(l);
  }, []);

  // Load entries JSON and existing DB
  useEffect(() => {
    (async () => {
      try {
        // Load the pre-extracted entries
        const res = await fetch("/api/files/rijal_entries.json");
        let data;
        if (res.ok) {
          data = await res.json();
        } else {
          // Try alternative paths
          setError("Could not load rijal_entries.json. Make sure the file is in outputs.");
          setLoading(false);
          return;
        }
        setEntries(data);
        
        // Load existing DB and progress
        const existingDb = await loadDB();
        const existingProgress = await loadProgress();
        setDb(existingDb);
        setProgress(existingProgress);
      } catch (e) {
        console.error(e);
        setError("Error loading data: " + e.message);
      }
      setLoading(false);
    })();
  }, []);

  if (loading) return <Shell><div style={{ textAlign: "center", padding: "80px" }}><div style={{ fontSize: "28px", color: gold, fontFamily: arFont, marginBottom: "14px" }}>بِسْمِ ٱللَّٰهِ</div><div style={{ color: muted, fontFamily: serif }}>Loading Rijāl Database…</div></div></Shell>;

  const dbCount = Object.keys(db).length;
  const totalEntries = entries?.length || 0;

  return (
    <Shell>
      <div style={{ padding: "22px 24px 14px", borderBottom: "1px solid rgba(201,171,99,0.15)", background: "rgba(10,26,18,0.92)", backdropFilter: "blur(12px)", position: "sticky", top: 0, zIndex: 100 }}>
        <div style={{ maxWidth: "1000px", margin: "0 auto" }}>
          <h1 style={{ fontSize: "24px", fontWeight: 700, color: gold, margin: 0 }}>Rijāl Database Builder</h1>
          <div style={{ fontSize: "11px", color: muted, fontFamily: sans, letterSpacing: "1.5px", textTransform: "uppercase", marginTop: "2px" }}>
            al-Mufīd min Muʿjam Rijāl al-Ḥadīth · {dbCount.toLocaleString()} / {totalEntries.toLocaleString()} narrators processed
          </div>
          <div style={{ display: "flex", gap: "6px", marginTop: "12px", alignItems: "center" }}>
            <NavBtn active={view === "process"} onClick={() => setView("process")}>Process Entries</NavBtn>
            <NavBtn active={view === "browse"} onClick={() => setView("browse")}>Browse Database ({dbCount})</NavBtn>
            {totalEntries > 0 && (
              <div style={{ marginLeft: "auto", width: "120px", height: "6px", borderRadius: "3px", background: "rgba(255,255,255,0.06)", overflow: "hidden" }}>
                <div style={{ width: `${(dbCount / totalEntries) * 100}%`, height: "100%", background: gold, borderRadius: "3px" }} />
              </div>
            )}
          </div>
        </div>
      </div>
      <div style={{ maxWidth: "1000px", margin: "0 auto", padding: "24px 16px 80px" }}>
        {error && <div style={{ color: "#a05050", fontSize: "14px", fontFamily: sans, padding: "16px", background: "rgba(160,80,80,0.1)", borderRadius: "8px", marginBottom: "16px" }}>{error}</div>}
        {view === "process" ? (
          <ProcessView entries={entries || []} db={db} setDb={setDb} progress={progress} setProgress={setProgress} />
        ) : (
          <BrowseView db={db} />
        )}
      </div>
    </Shell>
  );
}

function Shell({ children }) {
  return <div style={{ minHeight: "100vh", background: `linear-gradient(170deg, ${deep} 0%, #0f2318 30%, #132b1e 60%, ${deep} 100%)`, color: "#e8e0d0", fontFamily: serif }}>{children}</div>;
}
function NavBtn({ active, onClick, children }) {
  return <button onClick={onClick} style={{ padding: "6px 16px", borderRadius: "20px", cursor: "pointer", fontSize: "13px", fontFamily: sans, border: active ? `1px solid ${gold}` : "1px solid rgba(201,171,99,0.2)", background: active ? "rgba(201,171,99,0.12)" : "transparent", color: active ? gold : muted, fontWeight: active ? 600 : 400 }}>{children}</button>;
}

/* ════════════════════ PROCESS VIEW ════════════════════ */
function ProcessView({ entries, db, setDb, progress, setProgress }) {
  const [processing, setProcessing] = useState(false);
  const [status, setStatus] = useState("");
  const [batchCount, setBatchCount] = useState(50); // How many entries per run
  const cancelRef = useRef(false);
  const [lastResults, setLastResults] = useState([]);

  const dbCount = Object.keys(db).length;
  const nextIdx = progress.lastIdx + 1;
  const remaining = entries.length - nextIdx;

  const startProcessing = useCallback(async () => {
    if (entries.length === 0) return;
    setProcessing(true); cancelRef.current = false;
    setLastResults([]);

    const endIdx = Math.min(nextIdx + batchCount, entries.length);
    const newDb = { ...db };
    let currentIdx = nextIdx;
    let processed = 0;
    const batchResults = [];

    while (currentIdx < endIdx && !cancelRef.current) {
      const batch = entries.slice(currentIdx, Math.min(currentIdx + BATCH_SIZE, endIdx));
      setStatus(`Processing entries ${currentIdx + 1}–${currentIdx + batch.length} of ${entries.length}…`);

      try {
        const results = await processAIBatch(batch);
        if (results && Array.isArray(results)) {
          for (let i = 0; i < results.length; i++) {
            const entry = batch[i];
            const result = results[i];
            if (entry && result) {
              const key = entry.n3; // Use Tehran edition number as key
              newDb[key] = {
                ...result,
                _entryIdx: entry.idx,
                _num_najaf: entry.n1,
                _num_beirut: entry.n2,
                _num_tehran: entry.n3,
                _raw: entry.text,
              };
              batchResults.push(newDb[key]);
            }
          }
        }
      } catch (e) {
        console.error("Batch error:", e);
        setStatus(`Error at entry ${currentIdx}: ${e.message}. Continuing…`);
        await new Promise(r => setTimeout(r, 1000));
      }

      currentIdx += batch.length;
      processed += batch.length;

      // Save progress every batch
      const newProgress = { lastIdx: currentIdx - 1 };
      setDb({ ...newDb });
      setProgress(newProgress);
      await saveDB(newDb);
      await saveProgress(newProgress);

      // Rate limiting
      await new Promise(r => setTimeout(r, 500));
    }

    setLastResults(batchResults);
    setStatus(`Done! Processed ${processed} entries. ${Object.keys(newDb).length} total in database.`);
    setProcessing(false);
  }, [entries, db, nextIdx, batchCount, setDb, setProgress]);

  const handleReset = useCallback(async () => {
    if (!confirm("This will clear all processed data. Are you sure?")) return;
    setDb({});
    setProgress({ lastIdx: -1 });
    await saveDB({});
    await saveProgress({ lastIdx: -1 });
    setLastResults([]);
    setStatus("Database cleared.");
  }, [setDb, setProgress]);

  return (
    <div>
      <div style={{ marginBottom: "24px" }}>
        <div style={{ fontSize: "22px", color: gold, fontWeight: 600 }}>Process Narrator Entries</div>
        <div style={{ fontSize: "13px", color: muted, fontFamily: sans, marginTop: "4px" }}>
          {entries.length.toLocaleString()} entries extracted from al-Mufīd. AI processes each entry to extract structured narrator data.
          Progress is saved automatically — you can stop and resume anytime.
        </div>
      </div>

      {/* Stats */}
      <div style={{ display: "flex", gap: "16px", flexWrap: "wrap", marginBottom: "20px" }}>
        <StatBox label="Total Entries" value={entries.length.toLocaleString()} />
        <StatBox label="Processed" value={dbCount.toLocaleString()} color={gold} />
        <StatBox label="Remaining" value={remaining.toLocaleString()} />
        <StatBox label="Progress" value={`${entries.length > 0 ? ((dbCount / entries.length) * 100).toFixed(1) : 0}%`} color={gold} />
      </div>

      {/* Progress bar */}
      <div style={{ width: "100%", height: "10px", borderRadius: "5px", background: "rgba(255,255,255,0.06)", marginBottom: "20px", overflow: "hidden" }}>
        <div style={{ width: `${entries.length > 0 ? (dbCount / entries.length) * 100 : 0}%`, height: "100%", background: `linear-gradient(90deg, ${gold}, #a68b3e)`, borderRadius: "5px", transition: "width 0.3s" }} />
      </div>

      {/* Controls */}
      <div style={{ display: "flex", gap: "10px", marginBottom: "20px", flexWrap: "wrap", alignItems: "center" }}>
        <span style={{ fontSize: "12px", color: muted, fontFamily: sans }}>Process next:</span>
        {[50, 100, 200, 500].map(n => (
          <span key={n} onClick={() => !processing && setBatchCount(n)} style={{
            padding: "4px 12px", borderRadius: "14px", cursor: processing ? "default" : "pointer",
            fontSize: "12px", fontFamily: sans,
            border: batchCount === n ? `1px solid ${gold}` : "1px solid rgba(138,158,142,0.25)",
            background: batchCount === n ? `${gold}22` : "transparent",
            color: batchCount === n ? gold : muted,
            fontWeight: batchCount === n ? 600 : 400,
          }}>{n} entries</span>
        ))}

        <button onClick={startProcessing} disabled={processing || remaining === 0} style={{
          padding: "10px 28px", borderRadius: "8px", border: "none",
          background: (processing || remaining === 0) ? "#333" : `linear-gradient(135deg, ${gold}, #a68b3e)`,
          color: (processing || remaining === 0) ? "#666" : deep,
          fontWeight: 700, cursor: (processing || remaining === 0) ? "default" : "pointer",
          fontSize: "14px", fontFamily: sans, marginLeft: "8px",
        }}>
          {processing ? "Processing…" : remaining === 0 ? "All Done!" : `Process ${Math.min(batchCount, remaining)} Entries`}
        </button>

        {processing && <button onClick={() => { cancelRef.current = true; }} style={{ padding: "8px 16px", borderRadius: "8px", border: "1px solid #a05050", background: "transparent", color: "#a05050", fontSize: "13px", fontFamily: sans, cursor: "pointer" }}>Stop</button>}

        <button onClick={handleReset} style={{ padding: "8px 16px", borderRadius: "8px", border: "1px solid rgba(160,80,80,0.3)", background: "transparent", color: "#a05050", fontSize: "12px", fontFamily: sans, cursor: "pointer", marginLeft: "auto" }}>Reset All</button>
      </div>

      {status && <div style={{ fontSize: "13px", color: gold, fontFamily: sans, marginBottom: "16px", padding: "10px 14px", background: "rgba(201,171,99,0.06)", borderRadius: "8px" }}>{status}</div>}

      {/* Last processed results preview */}
      {lastResults.length > 0 && (
        <div style={{ marginTop: "20px" }}>
          <div style={{ fontSize: "14px", color: gold, fontWeight: 600, marginBottom: "12px" }}>Last Processed ({lastResults.length})</div>
          {lastResults.slice(0, 10).map((r, i) => <NarratorCard key={i} narrator={r} compact />)}
          {lastResults.length > 10 && <div style={{ fontSize: "12px", color: muted, fontFamily: sans }}>…and {lastResults.length - 10} more</div>}
        </div>
      )}
    </div>
  );
}

function StatBox({ label, value, color }) {
  return (
    <div style={{ background: "rgba(20,40,28,0.5)", border: "1px solid rgba(201,171,99,0.1)", borderRadius: "10px", padding: "14px 20px", minWidth: "100px" }}>
      <div style={{ fontSize: "22px", fontWeight: 700, color: color || "#e8e0d0", fontFamily: sans }}>{value}</div>
      <div style={{ fontSize: "11px", color: muted, fontFamily: sans }}>{label}</div>
    </div>
  );
}

/* ════════════════════ BROWSE VIEW ════════════════════ */
function BrowseView({ db }) {
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [selected, setSelected] = useState(null);
  const [page, setPage] = useState(0);

  const allNarrators = useMemo(() => Object.values(db), [db]);

  // Status counts
  const statusCounts = useMemo(() => {
    const c = {};
    for (const n of allNarrators) {
      const s = n.status || "unspecified";
      c[s] = (c[s] || 0) + 1;
    }
    return c;
  }, [allNarrators]);

  const filtered = useMemo(() => {
    let r = allNarrators;
    if (statusFilter) r = r.filter(n => n.status === statusFilter);
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      r = r.filter(n =>
        (n.name_ar || "").includes(q) ||
        (n.name_en || "").toLowerCase().includes(q) ||
        (n._raw || "").includes(q) ||
        (n.laqab || "").toLowerCase().includes(q) ||
        (n.kunyah || "").toLowerCase().includes(q)
      );
    }
    return r;
  }, [allNarrators, statusFilter, search]);

  const pageSize = 20;
  const totalPages = Math.ceil(filtered.length / pageSize);
  const pageItems = filtered.slice(page * pageSize, (page + 1) * pageSize);

  if (allNarrators.length === 0) return (
    <div style={{ textAlign: "center", padding: "60px" }}>
      <div style={{ fontSize: "18px", color: gold, fontFamily: serif }}>No narrators processed yet</div>
      <div style={{ fontSize: "14px", color: muted, fontFamily: sans, marginTop: "8px" }}>Go to "Process Entries" to start building the database.</div>
    </div>
  );

  return (
    <div>
      <div style={{ marginBottom: "24px" }}>
        <div style={{ fontSize: "22px", color: gold, fontWeight: 600 }}>Rijāl Database</div>
        <div style={{ fontSize: "13px", color: muted, fontFamily: sans, marginTop: "4px" }}>
          {allNarrators.length.toLocaleString()} narrators · Search by Arabic or English name
        </div>
      </div>

      {/* Status filter chips */}
      <div style={{ display: "flex", gap: "6px", marginBottom: "16px", flexWrap: "wrap" }}>
        <FilterChip active={!statusFilter} onClick={() => { setStatusFilter(""); setPage(0); }}>
          All ({allNarrators.length})
        </FilterChip>
        {Object.entries(statusCounts).sort((a, b) => b[1] - a[1]).map(([s, count]) => (
          <FilterChip key={s} active={statusFilter === s} color={STATUS_COLORS[s]?.bg}
            onClick={() => { setStatusFilter(statusFilter === s ? "" : s); setPage(0); }}>
            {STATUS_COLORS[s]?.label || s} ({count})
          </FilterChip>
        ))}
      </div>

      {/* Search */}
      <input value={search} onChange={e => { setSearch(e.target.value); setPage(0); }}
        placeholder="Search by name (Arabic or English)…" dir="auto"
        style={{
          width: "100%", padding: "12px 18px", borderRadius: "8px", marginBottom: "16px",
          border: "1px solid rgba(201,171,99,0.3)", background: "rgba(20,40,28,0.8)",
          color: "#e8e0d0", fontSize: "15px", fontFamily: sans, outline: "none", boxSizing: "border-box",
        }} />

      <div style={{ fontSize: "12px", color: muted, fontFamily: sans, marginBottom: "12px" }}>
        {filtered.length.toLocaleString()} results · Page {page + 1}/{totalPages || 1}
      </div>

      {/* Selected narrator detail */}
      {selected && <NarratorDetail narrator={selected} onClose={() => setSelected(null)} allNarrators={allNarrators} onSelect={setSelected} />}

      {/* List */}
      {pageItems.map((n, i) => (
        <NarratorCard key={n._num_tehran || i} narrator={n} onClick={() => setSelected(n)} />
      ))}

      {totalPages > 1 && (
        <div style={{ display: "flex", justifyContent: "center", gap: "10px", marginTop: "20px", alignItems: "center" }}>
          <button disabled={page === 0} onClick={() => setPage(p => p - 1)} style={{ padding: "8px 20px", borderRadius: "8px", border: "1px solid rgba(201,171,99,0.3)", background: page === 0 ? "transparent" : "rgba(201,171,99,0.1)", color: page === 0 ? "#444" : gold, cursor: page === 0 ? "default" : "pointer", fontSize: "13px", fontFamily: sans }}>← Prev</button>
          <span style={{ color: muted, fontSize: "13px", fontFamily: sans }}>{page + 1}/{totalPages}</span>
          <button disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)} style={{ padding: "8px 20px", borderRadius: "8px", border: "1px solid rgba(201,171,99,0.3)", background: page >= totalPages - 1 ? "transparent" : "rgba(201,171,99,0.1)", color: page >= totalPages - 1 ? "#444" : gold, cursor: page >= totalPages - 1 ? "default" : "pointer", fontSize: "13px", fontFamily: sans }}>Next →</button>
        </div>
      )}
    </div>
  );
}

/* ════════════════════ NARRATOR CARD ════════════════════ */
function NarratorCard({ narrator: n, onClick, compact }) {
  const sc = STATUS_COLORS[n.status] || STATUS_COLORS.unspecified;
  return (
    <div onClick={onClick} style={{
      background: "rgba(20,40,28,0.6)", border: "1px solid rgba(201,171,99,0.1)",
      borderRadius: "10px", padding: compact ? "12px 16px" : "16px 20px", marginBottom: "8px",
      cursor: onClick ? "pointer" : "default", transition: "border-color 0.2s",
    }} onMouseEnter={e => onClick && (e.currentTarget.style.borderColor = "rgba(201,171,99,0.3)")}
       onMouseLeave={e => onClick && (e.currentTarget.style.borderColor = "rgba(201,171,99,0.1)")}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: "12px" }}>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: compact ? "16px" : "18px", color: "#e8e0d0", fontFamily: arFont, direction: "rtl", textAlign: "right" }}>{n.name_ar || "—"}</div>
          <div style={{ fontSize: "13px", color: muted, fontFamily: sans, marginTop: "2px" }}>{n.name_en || "—"}</div>
        </div>
        <span style={{ padding: "3px 10px", borderRadius: "8px", background: sc.bg, color: "#fff", fontSize: "11px", fontFamily: sans, fontWeight: 600, whiteSpace: "nowrap", flexShrink: 0 }}>
          {n.status || "?"}
        </span>
      </div>
      {!compact && (
        <div style={{ display: "flex", gap: "6px", flexWrap: "wrap", marginTop: "8px" }}>
          {n.nisba && <Tag>{n.nisba}</Tag>}
          {n.kunyah && <Tag color="#6bc4c4">{n.kunyah}</Tag>}
          {(n.narrates_from_imams || []).map((im, i) => <Tag key={i} color={gold}>{im}</Tag>)}
          {(n.books || []).map((b, i) => <Tag key={`b${i}`} color="#8ac98a">{b}</Tag>)}
          {n.has_book && <Tag color="#d4845a">له كتاب</Tag>}
          {n.hadith_count && <Tag>{n.hadith_count} narrations</Tag>}
        </div>
      )}
    </div>
  );
}

function Tag({ children, color }) {
  return <span style={{ padding: "2px 8px", borderRadius: "6px", background: `${color || muted}15`, border: `1px solid ${color || muted}30`, color: color || muted, fontSize: "10px", fontFamily: sans }}>{children}</span>;
}

/* ════════════════════ NARRATOR DETAIL ════════════════════ */
function NarratorDetail({ narrator: n, onClose, allNarrators, onSelect }) {
  const sc = STATUS_COLORS[n.status] || STATUS_COLORS.unspecified;

  // Find cross-referenced narrators
  const aliases = useMemo(() => {
    if (!n.aliases?.length) return [];
    return n.aliases.map(a => {
      const found = allNarrators.find(x => (x.name_ar || "").includes(a) || (x.name_en || "").toLowerCase().includes(a.toLowerCase()));
      return { name: a, narrator: found };
    });
  }, [n, allNarrators]);

  return (
    <div style={{ background: "rgba(20,40,28,0.8)", border: `1px solid ${gold}44`, borderRadius: "12px", padding: "24px", marginBottom: "20px" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "16px" }}>
        <div>
          <div style={{ fontSize: "24px", color: gold, fontFamily: arFont, direction: "rtl", textAlign: "right" }}>{n.name_ar}</div>
          <div style={{ fontSize: "16px", color: "#c8d0ca", fontFamily: sans, marginTop: "4px" }}>{n.name_en}</div>
        </div>
        <button onClick={onClose} style={{ background: "none", border: "1px solid rgba(201,171,99,0.2)", borderRadius: "6px", color: muted, fontSize: "12px", fontFamily: sans, padding: "4px 12px", cursor: "pointer", height: "fit-content" }}>✕</button>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", marginBottom: "16px" }}>
        <Field label="Status" value={<span style={{ color: sc.bg, fontWeight: 600 }}>{sc.label}</span>} />
        {n.status_detail && <Field label="Status (Arabic)" value={<span style={{ fontFamily: arFont, direction: "rtl" }}>{n.status_detail}</span>} />}
        {n.father && <Field label="Father" value={n.father} />}
        {n.grandfather && <Field label="Grandfather" value={n.grandfather} />}
        {n.nasab && <Field label="Full Nasab" value={n.nasab} />}
        {n.kunyah && <Field label="Kunyah" value={n.kunyah} />}
        {n.laqab && <Field label="Laqab/Title" value={n.laqab} />}
        {n.nisba && <Field label="Nisba (Origin)" value={n.nisba} />}
        {n.companions_of && <Field label="Companion of" value={n.companions_of} />}
        {n.has_book && <Field label="Has Book/Aṣl" value="Yes" />}
        {n.hadith_count && <Field label="Narration Count" value={n.hadith_count} />}
        {n.tariq_status && <Field label="Ṭarīq Status" value={n.tariq_status} />}
      </div>

      {(n.narrates_from_imams || []).length > 0 && (
        <div style={{ marginBottom: "12px" }}>
          <div style={{ fontSize: "12px", color: gold, fontFamily: sans, fontWeight: 600, marginBottom: "6px" }}>Narrates from Imams</div>
          <div style={{ display: "flex", gap: "6px", flexWrap: "wrap" }}>
            {n.narrates_from_imams.map((im, i) => <Tag key={i} color={gold}>{im}</Tag>)}
          </div>
        </div>
      )}

      {(n.books || []).length > 0 && (
        <div style={{ marginBottom: "12px" }}>
          <div style={{ fontSize: "12px", color: "#8ac98a", fontFamily: sans, fontWeight: 600, marginBottom: "6px" }}>Appears in Books</div>
          <div style={{ display: "flex", gap: "6px", flexWrap: "wrap" }}>
            {n.books.map((b, i) => <Tag key={i} color="#8ac98a">{b}</Tag>)}
          </div>
        </div>
      )}

      {aliases.length > 0 && (
        <div style={{ marginBottom: "12px" }}>
          <div style={{ fontSize: "12px", color: "#d48aaa", fontFamily: sans, fontWeight: 600, marginBottom: "6px" }}>Also Known As / Identical To</div>
          <div style={{ display: "flex", gap: "6px", flexWrap: "wrap" }}>
            {aliases.map((a, i) => (
              <span key={i} onClick={() => a.narrator && onSelect(a.narrator)} style={{
                padding: "4px 10px", borderRadius: "8px", background: "rgba(212,138,170,0.1)",
                border: "1px solid rgba(212,138,170,0.2)", color: "#d48aaa",
                fontSize: "12px", fontFamily: sans, cursor: a.narrator ? "pointer" : "default",
              }}>{a.name}</span>
            ))}
          </div>
        </div>
      )}

      {n.notes && (
        <div style={{ marginBottom: "12px" }}>
          <div style={{ fontSize: "12px", color: muted, fontFamily: sans, fontWeight: 600, marginBottom: "4px" }}>Notes</div>
          <div style={{ fontSize: "13px", color: "#b8c4ba", fontFamily: sans, lineHeight: 1.6 }}>{n.notes}</div>
        </div>
      )}

      {/* Raw entry */}
      <details style={{ marginTop: "12px" }}>
        <summary style={{ fontSize: "11px", color: "#5a6e5e", fontFamily: sans, cursor: "pointer" }}>Raw entry text</summary>
        <div style={{ marginTop: "8px", padding: "10px 14px", background: "rgba(255,255,255,0.03)", borderRadius: "6px", fontSize: "13px", fontFamily: arFont, direction: "rtl", textAlign: "right", lineHeight: 1.8, color: "#8a9e8e" }}>
          {n._raw}
        </div>
      </details>

      <div style={{ fontSize: "10px", color: "#4a5e4e", fontFamily: sans, marginTop: "10px" }}>
        Entry #{n._num_tehran} (Tehran) · #{n._num_beirut} (Beirut) · #{n._num_najaf} (Najaf)
      </div>
    </div>
  );
}

function Field({ label, value }) {
  return (
    <div>
      <div style={{ fontSize: "10px", color: muted, fontFamily: sans, textTransform: "uppercase", letterSpacing: "1px" }}>{label}</div>
      <div style={{ fontSize: "14px", color: "#c8d0ca", fontFamily: sans, marginTop: "2px" }}>{value}</div>
    </div>
  );
}

function FilterChip({ active, color, onClick, children }) {
  return <span onClick={onClick} style={{ padding: "4px 12px", borderRadius: "14px", cursor: "pointer", fontSize: "12px", fontFamily: sans, fontWeight: active ? 600 : 400, border: `1px solid ${active ? (color || gold) : "rgba(138,158,142,0.25)"}`, background: active ? `${color || gold}22` : "transparent", color: active ? (color || gold) : muted }}>{children}</span>;
}
