import { useState, useEffect, useCallback, useRef, useMemo } from "react";

const API_BASE = "https://www.thaqalayn-api.net/api/v2";

// Imam detection patterns for English text
const IMAM_PATTERNS = [
  { key: "prophet", label: "Prophet Muhammad ﷺ", patterns: [/prophet/i, /messenger\s+of\s+allah/i, /rasul/i, /muhammad\s*\(s/i] },
  { key: "ali", label: "Imam Ali (a.s)", patterns: [/imam\s+ali/i, /amir\s+al-mu/i, /commander\s+of\s+the\s+faithful/i, /ali\s+ibn\s+abi/i, /\bali\s*\(a/i, /amīr/i] },
  { key: "fatima", label: "Sayyida Fatima (a.s)", patterns: [/fatim/i, /fāṭim/i, /al-zahra/i] },
  { key: "hasan", label: "Imam al-Hasan (a.s)", patterns: [/imam.*hasan(?!\s+al-['\u02BBaskar])/i, /al-ḥasan\b/i, /hasan\s+ibn\s+ali/i] },
  { key: "husayn", label: "Imam al-Husayn (a.s)", patterns: [/husayn/i, /hussain/i, /ḥusayn/i, /husain/i] },
  { key: "sajjad", label: "Imam al-Sajjad (a.s)", patterns: [/sajjad/i, /zayn\s+al-['\u02BBa]abidin/i, /ali\s+ibn\s+al-husayn/i, /ali\s+ibn\s+husayn/i] },
  { key: "baqir", label: "Imam al-Baqir (a.s)", patterns: [/baqir/i, /bāqir/i, /muhammad\s+ibn\s+ali/i] },
  { key: "sadiq", label: "Imam al-Sadiq (a.s)", patterns: [/sadiq/i, /sādiq/i, /ṣādiq/i, /ja['\u02BBa]far/i] },
  { key: "kadhim", label: "Imam al-Kadhim (a.s)", patterns: [/ka[dḍ][hḥ]im/i, /musa\s+ibn\s+ja/i, /mūsā/i] },
  { key: "rida", label: "Imam al-Rida (a.s)", patterns: [/ri[dḍ][aā]/i, /ali\s+ibn\s+musa/i] },
  { key: "jawad", label: "Imam al-Jawad (a.s)", patterns: [/jawad/i, /jawād/i, /muhammad\s+ibn\s+ali\s+ibn/i, /taqī/i] },
  { key: "hadi", label: "Imam al-Hadi (a.s)", patterns: [/al-hadi/i, /al-hādī/i, /ali\s+ibn\s+muhammad/i, /naqī/i] },
  { key: "askari", label: "Imam al-Askari (a.s)", patterns: [/['\u02BBa]askar[iī]/i, /hasan\s+ibn\s+ali\s+ibn/i] },
  { key: "mahdi", label: "Imam al-Mahdi (a.j)", patterns: [/mahd[iī]/i, /qa['\u02BBa]im/i, /ṣāḥib\s+al-zamān/i, /master\s+of\s+the\s+age/i] },
];

const GRADING_COLORS = {
  sahih: { bg: "#0d7a3e", text: "#fff", label: "Sahih (Authentic)" },
  hasan: { bg: "#2a7ab5", text: "#fff", label: "Hasan (Good)" },
  muwaththaq: { bg: "#7b6b2e", text: "#fff", label: "Muwaththaq (Reliable)" },
  qawi: { bg: "#5a4e8a", text: "#fff", label: "Qawi (Strong)" },
  daif: { bg: "#a0522d", text: "#fff", label: "Da'if (Weak)" },
  majhul: { bg: "#6b6b6b", text: "#fff", label: "Majhul (Unknown)" },
  mursal: { bg: "#8b7355", text: "#fff", label: "Mursal (Incomplete Chain)" },
  unknown: { bg: "#999", text: "#fff", label: "Grading N/A" },
};

function detectImams(text) {
  if (!text) return [];
  const found = [];
  for (const imam of IMAM_PATTERNS) {
    for (const p of imam.patterns) {
      if (p.test(text)) { found.push(imam); break; }
    }
  }
  return found;
}

function detectGrading(hadith) {
  const fields = [
    hadith.grading, hadith.grade, hadith.english_grade,
    hadith.gradings, hadith.behbudi_grade, hadith.majlisi_grade,
    hadith.mohseni_grade, hadith.source_grading,
  ];
  for (const f of fields) {
    if (f && typeof f === "string" && f.trim()) {
      const lower = f.toLowerCase();
      if (lower.includes("صحيح") || lower.includes("sahih") || lower.includes("ṣaḥīḥ")) return { ...GRADING_COLORS.sahih, raw: f };
      if (lower.includes("حسن") || lower.includes("hasan") || lower.includes("ḥasan")) return { ...GRADING_COLORS.hasan, raw: f };
      if (lower.includes("موثق") || lower.includes("muwaththaq") || lower.includes("reliable")) return { ...GRADING_COLORS.muwaththaq, raw: f };
      if (lower.includes("قوي") || lower.includes("qawi") || lower.includes("strong")) return { ...GRADING_COLORS.qawi, raw: f };
      if (lower.includes("ضعيف") || lower.includes("da'if") || lower.includes("daif") || lower.includes("weak") || lower.includes("ḍaʿīf")) return { ...GRADING_COLORS.daif, raw: f };
      if (lower.includes("مجهول") || lower.includes("majhul") || lower.includes("unknown")) return { ...GRADING_COLORS.majhul, raw: f };
      if (lower.includes("مرسل") || lower.includes("mursal")) return { ...GRADING_COLORS.mursal, raw: f };
      return { ...GRADING_COLORS.unknown, raw: f, label: f };
    }
  }
  return null;
}

function getArabicText(h) {
  return h.arabicText || h.arabic || h.Arabic || h.arabictext || h.arab || h.text_ar || h.hadith_ar || "";
}
function getEnglishText(h) {
  return h.englishText || h.english || h.English || h.englishtext || h.eng || h.text_en || h.hadith_en || h.translation || "";
}
function getChapter(h) {
  return h.chapter || h.Chapter || h.bab || h.section || h.chapterEnglish || h.chapter_english || "";
}
function getChapterAr(h) {
  return h.chapterArabic || h.chapter_arabic || h.babArabic || h.arabicChapter || "";
}

const PAGE_SIZE = 20;

// ─── Styles ───
const S = {
  app: {
    minHeight: "100vh",
    background: "linear-gradient(170deg, #0a1a12 0%, #0f2318 30%, #132b1e 60%, #0a1a12 100%)",
    color: "#e8e0d0",
    fontFamily: "'Cormorant Garamond', 'Amiri', Georgia, serif",
  },
  header: {
    padding: "28px 24px 20px",
    borderBottom: "1px solid rgba(201, 171, 99, 0.2)",
    background: "rgba(10, 26, 18, 0.85)",
    backdropFilter: "blur(12px)",
    position: "sticky",
    top: 0,
    zIndex: 100,
  },
  title: {
    fontSize: "28px",
    fontWeight: 700,
    color: "#c9ab63",
    letterSpacing: "1px",
    margin: 0,
    fontFamily: "'Cormorant Garamond', 'Amiri', serif",
  },
  subtitle: {
    fontSize: "13px",
    color: "#8a9e8e",
    marginTop: "4px",
    fontFamily: "'Source Sans 3', sans-serif",
    letterSpacing: "2px",
    textTransform: "uppercase",
  },
  nav: {
    display: "flex",
    gap: "6px",
    marginTop: "16px",
    flexWrap: "wrap",
  },
  navBtn: (active) => ({
    padding: "7px 16px",
    borderRadius: "20px",
    border: active ? "1px solid #c9ab63" : "1px solid rgba(201, 171, 99, 0.25)",
    background: active ? "rgba(201, 171, 99, 0.15)" : "transparent",
    color: active ? "#c9ab63" : "#8a9e8e",
    cursor: "pointer",
    fontSize: "13px",
    fontFamily: "'Source Sans 3', sans-serif",
    fontWeight: active ? 600 : 400,
    transition: "all 0.2s",
  }),
  content: {
    maxWidth: "960px",
    margin: "0 auto",
    padding: "24px 16px 80px",
  },
  searchBox: {
    display: "flex",
    gap: "10px",
    marginBottom: "20px",
    flexWrap: "wrap",
  },
  input: {
    flex: 1,
    minWidth: "200px",
    padding: "12px 18px",
    borderRadius: "8px",
    border: "1px solid rgba(201, 171, 99, 0.3)",
    background: "rgba(20, 40, 28, 0.8)",
    color: "#e8e0d0",
    fontSize: "15px",
    fontFamily: "'Source Sans 3', sans-serif",
    outline: "none",
  },
  searchBtn: {
    padding: "12px 24px",
    borderRadius: "8px",
    border: "none",
    background: "linear-gradient(135deg, #c9ab63, #a68b3e)",
    color: "#0a1a12",
    fontWeight: 700,
    cursor: "pointer",
    fontSize: "14px",
    fontFamily: "'Source Sans 3', sans-serif",
  },
  filters: {
    display: "flex",
    gap: "8px",
    marginBottom: "20px",
    flexWrap: "wrap",
    alignItems: "center",
  },
  filterLabel: {
    fontSize: "12px",
    color: "#8a9e8e",
    fontFamily: "'Source Sans 3', sans-serif",
    textTransform: "uppercase",
    letterSpacing: "1.5px",
    marginRight: "4px",
  },
  chip: (active, color) => ({
    padding: "5px 12px",
    borderRadius: "14px",
    border: `1px solid ${active ? (color || "#c9ab63") : "rgba(138, 158, 142, 0.3)"}`,
    background: active ? `${color || "#c9ab63"}22` : "transparent",
    color: active ? (color || "#c9ab63") : "#8a9e8e",
    cursor: "pointer",
    fontSize: "12px",
    fontFamily: "'Source Sans 3', sans-serif",
    fontWeight: active ? 600 : 400,
    transition: "all 0.2s",
    whiteSpace: "nowrap",
  }),
  card: {
    background: "rgba(20, 40, 28, 0.6)",
    border: "1px solid rgba(201, 171, 99, 0.12)",
    borderRadius: "12px",
    padding: "24px",
    marginBottom: "16px",
    transition: "border-color 0.3s",
  },
  arabic: {
    fontSize: "20px",
    lineHeight: 2,
    direction: "rtl",
    textAlign: "right",
    fontFamily: "'Amiri', 'Traditional Arabic', 'Scheherazade New', serif",
    color: "#e8e0d0",
    marginBottom: "16px",
    paddingBottom: "16px",
    borderBottom: "1px solid rgba(201, 171, 99, 0.1)",
  },
  english: {
    fontSize: "15px",
    lineHeight: 1.8,
    color: "#b8c4ba",
    fontFamily: "'Cormorant Garamond', Georgia, serif",
  },
  meta: {
    display: "flex",
    gap: "8px",
    flexWrap: "wrap",
    marginTop: "14px",
    alignItems: "center",
  },
  badge: (bg, color) => ({
    padding: "3px 10px",
    borderRadius: "10px",
    background: bg,
    color: color,
    fontSize: "11px",
    fontFamily: "'Source Sans 3', sans-serif",
    fontWeight: 600,
    whiteSpace: "nowrap",
  }),
  bookCard: {
    background: "rgba(20, 40, 28, 0.5)",
    border: "1px solid rgba(201, 171, 99, 0.15)",
    borderRadius: "12px",
    padding: "20px",
    cursor: "pointer",
    transition: "all 0.3s",
  },
  select: {
    padding: "8px 14px",
    borderRadius: "8px",
    border: "1px solid rgba(201, 171, 99, 0.3)",
    background: "rgba(20, 40, 28, 0.8)",
    color: "#e8e0d0",
    fontSize: "13px",
    fontFamily: "'Source Sans 3', sans-serif",
    outline: "none",
    maxWidth: "220px",
  },
  loader: {
    textAlign: "center",
    padding: "60px 20px",
    color: "#c9ab63",
    fontSize: "16px",
    fontFamily: "'Cormorant Garamond', serif",
  },
  pager: {
    display: "flex",
    justifyContent: "center",
    gap: "10px",
    marginTop: "24px",
  },
  pageBtn: (disabled) => ({
    padding: "8px 20px",
    borderRadius: "8px",
    border: "1px solid rgba(201, 171, 99, 0.3)",
    background: disabled ? "transparent" : "rgba(201, 171, 99, 0.1)",
    color: disabled ? "#555" : "#c9ab63",
    cursor: disabled ? "default" : "pointer",
    fontSize: "13px",
    fontFamily: "'Source Sans 3', sans-serif",
  }),
  stat: {
    fontSize: "12px",
    color: "#8a9e8e",
    fontFamily: "'Source Sans 3', sans-serif",
    marginBottom: "16px",
  },
};

// ─── Hadith Card ───
function HadithCard({ hadith, showBook }) {
  const ar = getArabicText(hadith);
  const en = getEnglishText(hadith);
  const ch = getChapter(hadith);
  const chAr = getChapterAr(hadith);
  const grading = detectGrading(hadith);
  const imams = detectImams(en + " " + ar);
  const [expanded, setExpanded] = useState(false);

  const isLong = (ar && ar.length > 500) || (en && en.length > 400);
  const displayAr = isLong && !expanded ? ar.slice(0, 500) + "..." : ar;
  const displayEn = isLong && !expanded ? en.slice(0, 400) + "..." : en;

  return (
    <div style={S.card}>
      {(ch || chAr) && (
        <div style={{ marginBottom: "12px", paddingBottom: "8px", borderBottom: "1px solid rgba(201,171,99,0.08)" }}>
          {chAr && <div style={{ fontSize: "14px", color: "#c9ab63", direction: "rtl", textAlign: "right", fontFamily: "'Amiri', serif", marginBottom: "2px" }}>{chAr}</div>}
          {ch && <div style={{ fontSize: "13px", color: "#8a9e8e", fontFamily: "'Source Sans 3', sans-serif" }}>{ch}</div>}
        </div>
      )}
      {ar && <div style={S.arabic}>{displayAr}</div>}
      {en && <div style={S.english}>{displayEn}</div>}
      {isLong && (
        <button onClick={() => setExpanded(!expanded)} style={{ ...S.navBtn(false), marginTop: "10px", fontSize: "12px" }}>
          {expanded ? "Show less" : "Read full hadith"}
        </button>
      )}
      <div style={S.meta}>
        {grading && (
          <span style={S.badge(grading.bg, grading.text)} title={grading.raw || grading.label}>
            {grading.raw && grading.raw.length < 40 ? grading.raw : grading.label}
          </span>
        )}
        {imams.map((im) => (
          <span key={im.key} style={S.badge("rgba(201,171,99,0.15)", "#c9ab63")}>{im.label}</span>
        ))}
        {showBook && hadith._bookName && (
          <span style={S.badge("rgba(138,158,142,0.15)", "#8a9e8e")}>{hadith._bookName} #{hadith.id || hadith.number || ""}</span>
        )}
        {!showBook && (hadith.id || hadith.number) && (
          <span style={{ fontSize: "11px", color: "#5a6e5e", fontFamily: "'Source Sans 3', sans-serif" }}>
            #{hadith.id || hadith.number}
          </span>
        )}
      </div>
    </div>
  );
}

// ─── Daily Hadith ───
function DailyHadith() {
  const [hadith, setHadith] = useState(null);
  const [loading, setLoading] = useState(true);
  const [bookName, setBookName] = useState("");

  const fetchRandom = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/random`);
      const data = await res.json();
      const h = Array.isArray(data) ? data[0] : data;
      setHadith(h);
      setBookName(h?.bookId?.replace(/-/g, " ") || "");
    } catch (e) { console.error(e); }
    setLoading(false);
  }, []);

  useEffect(() => { fetchRandom(); }, [fetchRandom]);

  if (loading) return <div style={S.loader}>Seeking a pearl of wisdom...</div>;
  if (!hadith) return <div style={S.loader}>Could not fetch hadith. Check your connection.</div>;

  return (
    <div>
      <div style={{ textAlign: "center", marginBottom: "24px" }}>
        <div style={{ fontSize: "22px", color: "#c9ab63", fontWeight: 600, marginBottom: "6px" }}>Hadith of the Moment</div>
        <div style={{ fontSize: "13px", color: "#8a9e8e", fontFamily: "'Source Sans 3', sans-serif" }}>
          From {bookName || "the Thaqalayn corpus"}
        </div>
      </div>
      <HadithCard hadith={hadith} showBook />
      <div style={{ textAlign: "center", marginTop: "16px" }}>
        <button onClick={fetchRandom} style={S.searchBtn}>Another Hadith</button>
      </div>
    </div>
  );
}

// ─── Book Browser (Topical) ───
function BookBrowser({ books, onSelectBook }) {
  const grouped = useMemo(() => {
    const groups = {};
    for (const b of books) {
      const author = b.author || "Unknown";
      if (!groups[author]) groups[author] = [];
      groups[author].push(b);
    }
    return groups;
  }, [books]);

  return (
    <div>
      <div style={{ marginBottom: "24px" }}>
        <div style={{ fontSize: "20px", color: "#c9ab63", fontWeight: 600, marginBottom: "6px" }}>Library — Browse by Book</div>
        <div style={{ fontSize: "13px", color: "#8a9e8e", fontFamily: "'Source Sans 3', sans-serif" }}>
          {books.length} volumes across the classical Shia hadith canon. Select a book to explore its chapters and ahadith.
        </div>
      </div>
      {Object.entries(grouped).map(([author, authorBooks]) => (
        <div key={author} style={{ marginBottom: "28px" }}>
          <div style={{ fontSize: "14px", color: "#c9ab63", fontFamily: "'Source Sans 3', sans-serif", textTransform: "uppercase", letterSpacing: "1.5px", marginBottom: "12px", paddingBottom: "6px", borderBottom: "1px solid rgba(201,171,99,0.1)" }}>
            {author}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: "12px" }}>
            {authorBooks.map((b) => {
              const count = (b.idRangeMax || 0) - (b.idRangeMin || 1) + 1;
              return (
                <div key={b.bookId} style={S.bookCard} onClick={() => onSelectBook(b)}
                  onMouseEnter={(e) => { e.currentTarget.style.borderColor = "rgba(201,171,99,0.4)"; e.currentTarget.style.transform = "translateY(-2px)"; }}
                  onMouseLeave={(e) => { e.currentTarget.style.borderColor = "rgba(201,171,99,0.15)"; e.currentTarget.style.transform = "none"; }}>
                  <div style={{ fontSize: "17px", color: "#e8e0d0", fontWeight: 600, marginBottom: "4px" }}>{b.BookName || b.bookName}</div>
                  {b.englishName && <div style={{ fontSize: "13px", color: "#c9ab63", marginBottom: "8px", fontStyle: "italic" }}>{b.englishName}</div>}
                  <div style={{ fontSize: "12px", color: "#8a9e8e", fontFamily: "'Source Sans 3', sans-serif" }}>
                    {b.volume && `Vol. ${b.volume} · `}{count} ahadith{b.translator ? ` · Tr: ${b.translator}` : ""}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Book Detail View ───
function BookDetail({ book, onBack }) {
  const [hadiths, setHadiths] = useState([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(0);
  const [imamFilter, setImamFilter] = useState(null);
  const [gradingFilter, setGradingFilter] = useState(null);
  const [chapterFilter, setChapterFilter] = useState("");
  const [fieldMap, setFieldMap] = useState(null);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const res = await fetch(`${API_BASE}/${book.bookId}`);
        const data = await res.json();
        const arr = Array.isArray(data) ? data : (data.hadiths || data.data || [data]);
        // Inspect first hadith to log fields
        if (arr.length > 0) {
          console.log("Hadith fields:", Object.keys(arr[0]));
          console.log("Sample hadith:", arr[0]);
          setFieldMap(Object.keys(arr[0]));
        }
        setHadiths(arr);
      } catch (e) { console.error(e); }
      setLoading(false);
    })();
  }, [book.bookId]);

  // Collect chapters
  const chapters = useMemo(() => {
    const set = new Set();
    for (const h of hadiths) {
      const ch = getChapter(h);
      if (ch) set.add(ch);
    }
    return [...set];
  }, [hadiths]);

  // Filtered
  const filtered = useMemo(() => {
    let result = hadiths;
    if (imamFilter) {
      result = result.filter((h) => {
        const text = getEnglishText(h) + " " + getArabicText(h);
        return IMAM_PATTERNS.find((p) => p.key === imamFilter)?.patterns.some((p) => p.test(text));
      });
    }
    if (gradingFilter) {
      result = result.filter((h) => {
        const g = detectGrading(h);
        if (gradingFilter === "graded") return g !== null;
        if (gradingFilter === "ungraded") return g === null;
        return g?.label?.toLowerCase().includes(gradingFilter);
      });
    }
    if (chapterFilter) {
      result = result.filter((h) => getChapter(h) === chapterFilter);
    }
    return result;
  }, [hadiths, imamFilter, gradingFilter, chapterFilter]);

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const pageHadiths = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  // Grading stats
  const gradingStats = useMemo(() => {
    const stats = { total: hadiths.length, graded: 0, sahih: 0, hasan: 0, daif: 0, other: 0 };
    for (const h of hadiths) {
      const g = detectGrading(h);
      if (g) {
        stats.graded++;
        if (g.label?.includes("Sahih") || g.label?.includes("Authentic")) stats.sahih++;
        else if (g.label?.includes("Hasan") || g.label?.includes("Good")) stats.hasan++;
        else if (g.label?.includes("Da'if") || g.label?.includes("Weak")) stats.daif++;
        else stats.other++;
      }
    }
    return stats;
  }, [hadiths]);

  if (loading) return <div style={S.loader}>Loading {book.BookName || book.bookName}...</div>;

  return (
    <div>
      <button onClick={onBack} style={{ ...S.navBtn(false), marginBottom: "16px" }}>← Back to Library</button>
      <div style={{ marginBottom: "20px" }}>
        <div style={{ fontSize: "22px", color: "#c9ab63", fontWeight: 600 }}>{book.BookName || book.bookName}</div>
        {book.englishName && <div style={{ fontSize: "15px", color: "#8a9e8e", fontStyle: "italic" }}>{book.englishName}</div>}
        <div style={{ fontSize: "13px", color: "#5a6e5e", fontFamily: "'Source Sans 3', sans-serif", marginTop: "4px" }}>
          {book.author}{book.volume ? ` · Volume ${book.volume}` : ""} · {hadiths.length} ahadith · {chapters.length} chapters
        </div>
        {gradingStats.graded > 0 && (
          <div style={{ ...S.stat, marginTop: "8px" }}>
            Gradings available: {gradingStats.graded}/{gradingStats.total} —
            Sahih: {gradingStats.sahih} · Hasan: {gradingStats.hasan} · Da'if: {gradingStats.daif} · Other: {gradingStats.other}
          </div>
        )}
      </div>

      {/* Filters */}
      <div style={S.filters}>
        <span style={S.filterLabel}>Imam:</span>
        <span style={S.chip(imamFilter === null, "#c9ab63")} onClick={() => { setImamFilter(null); setPage(0); }}>All</span>
        {IMAM_PATTERNS.slice(0, 10).map((im) => (
          <span key={im.key} style={S.chip(imamFilter === im.key, "#c9ab63")} onClick={() => { setImamFilter(imamFilter === im.key ? null : im.key); setPage(0); }}>
            {im.label.replace(/ \(.*/, "")}
          </span>
        ))}
      </div>

      {gradingStats.graded > 0 && (
        <div style={S.filters}>
          <span style={S.filterLabel}>Grading:</span>
          <span style={S.chip(gradingFilter === null)} onClick={() => { setGradingFilter(null); setPage(0); }}>All</span>
          <span style={S.chip(gradingFilter === "sahih", GRADING_COLORS.sahih.bg)} onClick={() => { setGradingFilter(gradingFilter === "sahih" ? null : "sahih"); setPage(0); }}>Sahih</span>
          <span style={S.chip(gradingFilter === "hasan", GRADING_COLORS.hasan.bg)} onClick={() => { setGradingFilter(gradingFilter === "hasan" ? null : "hasan"); setPage(0); }}>Hasan</span>
          <span style={S.chip(gradingFilter === "muwaththaq", GRADING_COLORS.muwaththaq.bg)} onClick={() => { setGradingFilter(gradingFilter === "muwaththaq" ? null : "muwaththaq"); setPage(0); }}>Muwaththaq</span>
          <span style={S.chip(gradingFilter === "da'if", GRADING_COLORS.daif.bg)} onClick={() => { setGradingFilter(gradingFilter === "da'if" ? null : "da'if"); setPage(0); }}>Da'if</span>
          <span style={S.chip(gradingFilter === "graded")} onClick={() => { setGradingFilter(gradingFilter === "graded" ? null : "graded"); setPage(0); }}>All Graded</span>
          <span style={S.chip(gradingFilter === "ungraded")} onClick={() => { setGradingFilter(gradingFilter === "ungraded" ? null : "ungraded"); setPage(0); }}>Ungraded</span>
        </div>
      )}

      {chapters.length > 0 && (
        <div style={{ ...S.filters, marginBottom: "20px" }}>
          <span style={S.filterLabel}>Chapter:</span>
          <select style={S.select} value={chapterFilter} onChange={(e) => { setChapterFilter(e.target.value); setPage(0); }}>
            <option value="">All Chapters ({chapters.length})</option>
            {chapters.map((ch, i) => <option key={i} value={ch}>{ch.length > 60 ? ch.slice(0, 60) + "..." : ch}</option>)}
          </select>
        </div>
      )}

      <div style={S.stat}>{filtered.length} ahadith{filtered.length !== hadiths.length ? ` (filtered from ${hadiths.length})` : ""} · Page {page + 1} of {totalPages || 1}</div>

      {pageHadiths.map((h, i) => <HadithCard key={h._id || h.id || i} hadith={h} />)}

      {totalPages > 1 && (
        <div style={S.pager}>
          <button style={S.pageBtn(page === 0)} onClick={() => setPage(Math.max(0, page - 1))} disabled={page === 0}>Previous</button>
          <span style={{ color: "#8a9e8e", fontSize: "13px", fontFamily: "'Source Sans 3', sans-serif", alignSelf: "center" }}>
            {page + 1} / {totalPages}
          </span>
          <button style={S.pageBtn(page >= totalPages - 1)} onClick={() => setPage(Math.min(totalPages - 1, page + 1))} disabled={page >= totalPages - 1}>Next</button>
        </div>
      )}

      {/* Debug: show raw field names */}
      {fieldMap && (
        <details style={{ marginTop: "32px", fontSize: "12px", color: "#5a6e5e", fontFamily: "monospace" }}>
          <summary style={{ cursor: "pointer" }}>Debug: Hadith fields from API</summary>
          <pre style={{ marginTop: "8px", whiteSpace: "pre-wrap" }}>{JSON.stringify(fieldMap, null, 2)}</pre>
        </details>
      )}
    </div>
  );
}

// ─── Search View ───
function SearchView({ books }) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [bookFilter, setBookFilter] = useState("");
  const [imamFilter, setImamFilter] = useState(null);
  const [gradingFilter, setGradingFilter] = useState(null);
  const [page, setPage] = useState(0);

  const doSearch = useCallback(async () => {
    if (!query.trim()) return;
    setLoading(true);
    setSearched(true);
    setPage(0);
    try {
      const endpoint = bookFilter
        ? `${API_BASE}/query/${bookFilter}?q=${encodeURIComponent(query)}`
        : `${API_BASE}/query?q=${encodeURIComponent(query)}`;
      const res = await fetch(endpoint);
      const data = await res.json();
      const arr = Array.isArray(data) ? data : (data.hadiths || data.data || data.results || []);
      // Attach book name for display
      const bookMap = {};
      for (const b of books) bookMap[b.bookId] = b.BookName || b.bookName || b.bookId;
      for (const h of arr) {
        h._bookName = bookMap[h.bookId] || h.bookId || "";
      }
      setResults(arr);
    } catch (e) { console.error(e); setResults([]); }
    setLoading(false);
  }, [query, bookFilter, books]);

  const filtered = useMemo(() => {
    let r = results;
    if (imamFilter) {
      r = r.filter((h) => {
        const text = getEnglishText(h) + " " + getArabicText(h);
        return IMAM_PATTERNS.find((p) => p.key === imamFilter)?.patterns.some((p) => p.test(text));
      });
    }
    if (gradingFilter) {
      r = r.filter((h) => {
        const g = detectGrading(h);
        if (gradingFilter === "graded") return g !== null;
        if (gradingFilter === "ungraded") return g === null;
        return g?.label?.toLowerCase().includes(gradingFilter);
      });
    }
    return r;
  }, [results, imamFilter, gradingFilter]);

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const pageResults = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <div>
      <div style={{ fontSize: "20px", color: "#c9ab63", fontWeight: 600, marginBottom: "16px" }}>Search the Hadith Corpus</div>
      <div style={S.searchBox}>
        <input
          style={S.input}
          placeholder="Search in Arabic or English..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && doSearch()}
          dir="auto"
        />
        <select style={S.select} value={bookFilter} onChange={(e) => setBookFilter(e.target.value)}>
          <option value="">All Books</option>
          {books.map((b) => <option key={b.bookId} value={b.bookId}>{(b.BookName || b.bookName)} {b.volume ? `V${b.volume}` : ""}</option>)}
        </select>
        <button style={S.searchBtn} onClick={doSearch}>Search</button>
      </div>

      {searched && !loading && (
        <>
          <div style={S.filters}>
            <span style={S.filterLabel}>Imam:</span>
            <span style={S.chip(imamFilter === null)} onClick={() => { setImamFilter(null); setPage(0); }}>All</span>
            {IMAM_PATTERNS.slice(0, 8).map((im) => (
              <span key={im.key} style={S.chip(imamFilter === im.key)} onClick={() => { setImamFilter(imamFilter === im.key ? null : im.key); setPage(0); }}>
                {im.label.replace(/ \(.*/, "")}
              </span>
            ))}
          </div>
          <div style={S.filters}>
            <span style={S.filterLabel}>Grading:</span>
            <span style={S.chip(gradingFilter === null)} onClick={() => { setGradingFilter(null); setPage(0); }}>All</span>
            <span style={S.chip(gradingFilter === "sahih", GRADING_COLORS.sahih.bg)} onClick={() => { setGradingFilter(gradingFilter === "sahih" ? null : "sahih"); setPage(0); }}>Sahih</span>
            <span style={S.chip(gradingFilter === "hasan", GRADING_COLORS.hasan.bg)} onClick={() => { setGradingFilter(gradingFilter === "hasan" ? null : "hasan"); setPage(0); }}>Hasan</span>
            <span style={S.chip(gradingFilter === "da'if", GRADING_COLORS.daif.bg)} onClick={() => { setGradingFilter(gradingFilter === "da'if" ? null : "da'if"); setPage(0); }}>Da'if</span>
            <span style={S.chip(gradingFilter === "graded")} onClick={() => { setGradingFilter(gradingFilter === "graded" ? null : "graded"); setPage(0); }}>Graded Only</span>
          </div>
        </>
      )}

      {loading && <div style={S.loader}>Searching across the hadith corpus...</div>}

      {searched && !loading && (
        <div style={S.stat}>
          {filtered.length} result{filtered.length !== 1 ? "s" : ""}
          {filtered.length !== results.length ? ` (filtered from ${results.length})` : ""}
          {query && ` for "${query}"`}
        </div>
      )}

      {pageResults.map((h, i) => <HadithCard key={h._id || h.id || i} hadith={h} showBook />)}

      {totalPages > 1 && (
        <div style={S.pager}>
          <button style={S.pageBtn(page === 0)} onClick={() => setPage(Math.max(0, page - 1))} disabled={page === 0}>Previous</button>
          <span style={{ color: "#8a9e8e", fontSize: "13px", fontFamily: "'Source Sans 3', sans-serif", alignSelf: "center" }}>{page + 1} / {totalPages}</span>
          <button style={S.pageBtn(page >= totalPages - 1)} onClick={() => setPage(Math.min(totalPages - 1, page + 1))} disabled={page >= totalPages - 1}>Next</button>
        </div>
      )}
    </div>
  );
}

// ─── App ───
export default function ThaqalaynExplorer() {
  const [books, setBooks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [view, setView] = useState("daily"); // daily, browse, search
  const [selectedBook, setSelectedBook] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/allbooks`);
        const data = await res.json();
        setBooks(Array.isArray(data) ? data : []);
      } catch (e) {
        console.error(e);
        setError("Could not connect to the Thaqalayn API. Please check your internet connection.");
      }
      setLoading(false);
    })();
  }, []);

  // Google Fonts
  useEffect(() => {
    const link = document.createElement("link");
    link.href = "https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,600;0,700;1,400&family=Source+Sans+3:wght@300;400;600;700&family=Amiri:wght@400;700&display=swap";
    link.rel = "stylesheet";
    document.head.appendChild(link);
    return () => document.head.removeChild(link);
  }, []);

  if (loading) return (
    <div style={{ ...S.app, display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div style={S.loader}>
        <div style={{ fontSize: "32px", color: "#c9ab63", marginBottom: "12px" }}>بسم الله الرحمن الرحيم</div>
        <div>Loading the Thaqalayn Hadith Library...</div>
      </div>
    </div>
  );

  if (error) return (
    <div style={{ ...S.app, display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div style={{ ...S.loader, color: "#a05" }}>{error}</div>
    </div>
  );

  return (
    <div style={S.app}>
      <div style={S.header}>
        <h1 style={S.title}>Thaqalayn Explorer</h1>
        <div style={S.subtitle}>Topical Hadith Engine · Grading-Aware Search · Arabic-English Corpus</div>
        <div style={S.nav}>
          <button style={S.navBtn(view === "daily" && !selectedBook)} onClick={() => { setView("daily"); setSelectedBook(null); }}>Daily Hadith</button>
          <button style={S.navBtn(view === "browse" && !selectedBook)} onClick={() => { setView("browse"); setSelectedBook(null); }}>Browse Library</button>
          <button style={S.navBtn(view === "search" && !selectedBook)} onClick={() => { setView("search"); setSelectedBook(null); }}>Search</button>
          <span style={{ fontSize: "12px", color: "#5a6e5e", fontFamily: "'Source Sans 3', sans-serif", alignSelf: "center", marginLeft: "auto" }}>
            {books.length} volumes · {books.reduce((s, b) => s + ((b.idRangeMax || 0) - (b.idRangeMin || 1) + 1), 0).toLocaleString()} ahadith
          </span>
        </div>
      </div>

      <div style={S.content}>
        {selectedBook ? (
          <BookDetail book={selectedBook} onBack={() => setSelectedBook(null)} />
        ) : view === "daily" ? (
          <DailyHadith />
        ) : view === "browse" ? (
          <BookBrowser books={books} onSelectBook={(b) => { setSelectedBook(b); }} />
        ) : (
          <SearchView books={books} />
        )}
      </div>
    </div>
  );
}
