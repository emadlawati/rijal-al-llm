"""
Isnad Analyzer — Streamlit Web UI
Run with:  streamlit run app.py
"""
import streamlit as st
import sys
import os

# Make sure imports resolve from the project directory
sys.path.insert(0, os.path.dirname(__file__))

from isnad_analyzer import IsnadAnalyzer, PRINCIPLES

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Isnad Analyzer | محلل الإسناد",
    page_icon="📿",
    layout="wide",
)

# ── CSS: RTL support + custom card styles ────────────────────────────────────
st.markdown("""
<style>
/* Global RTL for Arabic text */
.arabic {
    direction: rtl;
    text-align: right;
    font-family: 'Segoe UI', 'Noto Naskh Arabic', 'Amiri', Arial, sans-serif;
}

/* Narrator link cards */
.narrator-card {
    direction: rtl;
    text-align: right;
    padding: 14px 18px;
    border-radius: 10px;
    margin-bottom: 10px;
    border-right: 6px solid #ccc;
    background: #1e1e2e;
    font-family: 'Segoe UI', 'Noto Naskh Arabic', Arial, sans-serif;
}
.narrator-card.thiqah   { border-right-color: #2ecc71; background: #0f2a1a; }
.narrator-card.hasan    { border-right-color: #3498db; background: #0d1f2d; }
.narrator-card.mamduh   { border-right-color: #3498db; background: #0d1f2d; }
.narrator-card.muwaththaq { border-right-color: #f39c12; background: #2a1f0a; }
.narrator-card.daif     { border-right-color: #e74c3c; background: #2a0d0d; }
.narrator-card.majhul   { border-right-color: #e74c3c; background: #2a0d0d; }
.narrator-card.unresolved { border-right-color: #95a5a6; background: #1a1a1a; }
.narrator-card.virtual  { border-right-color: #9b59b6; background: #1a0f2a; }

/* Status badges */
.badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.78rem;
    font-weight: bold;
    letter-spacing: 0.5px;
    margin-left: 8px;
}
.badge-thiqah     { background: #2ecc71; color: #000; }
.badge-hasan      { background: #3498db; color: #fff; }
.badge-mamduh     { background: #3498db; color: #fff; }
.badge-muwaththaq { background: #f39c12; color: #000; }
.badge-daif       { background: #e74c3c; color: #fff; }
.badge-majhul     { background: #e74c3c; color: #fff; }
.badge-unresolved { background: #7f8c8d; color: #fff; }
.badge-virtual    { background: #9b59b6; color: #fff; }
.badge-imam       { background: #9b59b6; color: #fff; }
.badge-mursal     { background: #e67e22; color: #fff; }

/* Final grade banner */
.grade-banner {
    direction: rtl;
    text-align: center;
    padding: 20px;
    border-radius: 12px;
    font-size: 1.5rem;
    font-weight: bold;
    margin: 20px 0;
    font-family: 'Segoe UI', Arial, sans-serif;
}
.grade-sahih    { background: #145a32; color: #2ecc71; border: 2px solid #2ecc71; }
.grade-hasan    { background: #1a3a5c; color: #3498db; border: 2px solid #3498db; }
.grade-muwath   { background: #5c3d1a; color: #f39c12; border: 2px solid #f39c12; }
.grade-daif     { background: #5c1a1a; color: #e74c3c; border: 2px solid #e74c3c; }
.grade-undet    { background: #2a2a2a; color: #95a5a6; border: 2px solid #95a5a6; }

.narrator-name { font-size: 1.15rem; font-weight: bold; color: #f0f0f0; }
.match-name    { font-size: 0.95rem; color: #aaaaaa; margin-top: 4px; }
.score-text    { font-size: 0.8rem;  color: #888888; }
.reason-text   { font-size: 0.82rem; color: #aaaaaa; font-style: italic; }
.alt-text      { font-size: 0.82rem; color: #888888; }

/* Tabaqah badges */
.badge-tabaqah { background: #2c3e50; color: #bdc3c7; border: 1px solid #4a6278; }

/* Tabaqah gap warnings */
.gap-warning {
    direction: rtl;
    text-align: right;
    padding: 10px 14px;
    border-radius: 8px;
    margin-bottom: 8px;
    font-family: 'Segoe UI', 'Noto Naskh Arabic', Arial, sans-serif;
    font-size: 0.88rem;
}
.gap-warning.warning { background: #2a2200; border-right: 4px solid #f39c12; color: #f5c842; }
.gap-warning.major   { background: #2a0d0d; border-right: 4px solid #e74c3c; color: #e87070; }

/* Input area RTL */
textarea { direction: rtl !important; text-align: right !important; }

/* Hide Streamlit branding */
#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Load analyzer (cached so DB only loads once) ──────────────────────────────
@st.cache_resource(show_spinner="جاري تحميل قاعدة بيانات الرجال...", max_entries=1)
def load_analyzer(_version: int = 3):
    return IsnadAnalyzer()

analyzer = load_analyzer(_version=3)

# ── Helper: map status → badge HTML ──────────────────────────────────────────
STATUS_LABEL = {
    'thiqah':      ('ثقة',       'thiqah'),
    'hasan':       ('حسن',       'hasan'),
    'mamduh':      ('ممدوح',     'mamduh'),
    'muwaththaq':  ('موثّق',     'muwaththaq'),
    'daif':        ('ضعيف',      'daif'),
    'majhul':      ('مجهول',     'majhul'),
    'unspecified': ('غير محدد',  'unresolved'),
}

def badge(status: str, is_virtual=False, is_imam=False, is_mursal=False) -> str:
    if is_imam:
        return '<span class="badge badge-imam">إمام ✓</span>'
    if is_mursal:
        return '<span class="badge badge-mursal">مرسل</span>'
    if is_virtual:
        return '<span class="badge badge-virtual">جماعة ثقات</span>'
    ar_label, css = STATUS_LABEL.get(status, (status, 'unresolved'))
    return f'<span class="badge badge-{css}">{ar_label}</span>'

def grade_banner(grade: str) -> str:
    label_map = {
        'Sahih':       ('صحيح ✓',           'sahih',  'Sahih (Authentic)'),
        'Hasan':       ('حسن',               'hasan',  'Hasan (Good)'),
        'Muwaththaq':  ('موثّق',             'muwath', 'Muwaththaq (Reliable - Non-Imami)'),
        "Da'if":       ('ضعيف ✗',           'daif',   "Da'if (Weak)"),
        'Majhul':      ('مجهول / ضعيف ✗',   'daif',   'Majhul'),
        'Undetermined':('غير محدد ⚠',        'undet',  'Undetermined'),
    }
    css = 'undet'
    ar = grade
    for key, (ar_label, css_key, _) in label_map.items():
        if key in grade:
            ar = ar_label
            css = css_key
            break
    return f'<div class="grade-banner grade-{css}">الحكم النهائي: {ar}<br><small style="font-size:0.9rem;opacity:0.8">{grade}</small></div>'

def card_class(status: str, is_virtual=False, is_imam=False) -> str:
    if is_imam:   return 'virtual'
    if is_virtual: return 'virtual'
    return STATUS_LABEL.get(status, ('', 'unresolved'))[1]

def tabaqah_badge(tabaqah: int | None, label: str | None, source: str | None) -> str:
    if not tabaqah:
        return ''
    short = label.split(' — ')[0] if label else f"T{tabaqah}"
    full  = label or f"T{tabaqah}"
    src_icon = "★" if source == 'imam' else ("◆" if source == 'alf_rajul' else "◇")
    return (
        f'<span class="badge badge-tabaqah" title="{full} | المصدر: {source}">'
        f'{src_icon} {short}'
        f'</span>'
    )

# ── Sidebar: example isnads ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📿 محلل الإسناد")
    st.markdown("---")
    st.markdown("**أمثلة جاهزة**")

    examples = [
        ("سند صحيح",         "عدة من أصحابنا، عن أحمد بن محمد، عن ابن أبي عمير، عن هشام بن سالم، عن أبي عبد الله (ع)"),
        ("سند فيه مجهول",    "محمد بن يحيى، عن أحمد بن محمد بن عيسى، عن الحسن بن علي الوشاء، عن رجل، عن أبي عبد الله (ع)"),
        ("سند فيه ضعيف",     "علي بن إبراهيم، عن أبيه، عن ابن أبي عمير، عن يونس بن ظبيان، عن أبي عبد الله (ع)"),
        ("سند حسن",          "محمد بن يحيى، عن محمد بن الحسين، عن ابن محبوب، عن إبراهيم بن أبي البلاد، عن أبي عبد الله (ع)"),
    ]

    for label, isnad in examples:
        if st.button(label, use_container_width=True):
            st.session_state['isnad_input'] = isnad

    st.markdown("---")
    st.markdown("**⚖️ المبادئ الرجالية**")

    active_principles: set = set()
    for pk, pdata in PRINCIPLES.items():
        checked = st.checkbox(
            pdata['name_ar'],
            value=False,
            key=f"principle_{pk}",
            help=pdata['description_ar'],
        )
        if checked:
            active_principles.add(pk)
            uplift = analyzer.count_principle_uplift(pk)
            st.caption(
                f"يشمل **{uplift['total_vouched']}** راوياً — "
                f"**{uplift['newly_thiqah']}** منهم يرتفع حكمه إلى ثقة"
            )

    st.markdown("---")
    st.markdown("""
**رموز الحكم:**

🟢 **ثقة** — صحيح
🔵 **حسن / ممدوح** — حسن
🟠 **موثّق** — غير إمامي
🔴 **ضعيف / مجهول** — ضعيف
🟣 **إمام / جماعة ثقات**
""")

    st.markdown("---")
    with st.expander("⚠️ تنبيه شرعي وعلمي"):
        st.markdown("""
<div class="arabic">
هذا المحلِّل أداة بحثية حاسوبية لتسهيل دراسة الأسانيد، وليس بديلاً عن
نظر الفقيه أو رجالي مختصّ. الأحكام الصادرة عن البرنامج ناتجة عن خوارزميات
وقواعد بيانات قابلة للخطأ، وقد يصدر الحكم خطأً بسبب:

- تصحيف في اسم الراوي أو خطأ في تطابق الأسماء
- ضعف الإسناد المُدرَج في قاعدة البيانات
- خطأ في استنباط الطبقة (بالشبكة أو بالنموذج اللغوي)
- عدم احتواء قاعدة البيانات على الراوي

يُستحسن مراجعة الأسانيد الحساسة على المصادر الأصلية: معجم رجال الحديث
للسيد الخوئي، والفهرست للشيخ الطوسي، ورجال النجاشي.
</div>
""", unsafe_allow_html=True)

    with st.expander("📚 المصادر والمراجع"):
        st.markdown("""
**قاعدة بيانات الرجال:**
مستخرجة من *المفيد من معجم رجال الحديث* للشيخ محمد الجواهري
(تلخيص لمعجم السيد أبي القاسم الخوئي ﷺ).

**نظام الطبقات:**
*معجم الألف رجل* للسيد غيث الشبر (٩٨٨ راوياً مَأْخوذة طبقاتهم
من نظام السيد حسين البروجردي ﷺ ذي الإثنتي عشرة طبقة).

**نصوص الأحاديث:**
[ثقلين API](https://www.thaqalayn-api.net) — مكتبة رقمية مفتوحة
لـ ٣١٠٠٠+ حديث من ٣٣ كتاباً شيعيّاً كلاسيكياً.

**البرنامج:**
[GitHub: HakiNinth/Rijal-al-LLM](https://github.com/emadlawati)
— مفتوح المصدر تحت رخصة MIT.
""")

# ── Main page ─────────────────────────────────────────────────────────────────
st.markdown('<h1 style="direction:rtl;text-align:right;">📿 محلل الإسناد الشيعي</h1>', unsafe_allow_html=True)
st.markdown('<p class="arabic" style="color:#888">أدخل الإسناد كاملاً — يُحلَّل كل راوٍ ويُحكم على السند</p>', unsafe_allow_html=True)

# Input
default_val = st.session_state.get('isnad_input', '')
isnad_input = st.text_area(
    label="الإسناد",
    value=default_val,
    height=100,
    placeholder="مثال: عدة من أصحابنا، عن أحمد بن محمد، عن ابن أبي عمير، عن أبي عبد الله (ع)",
    label_visibility="collapsed",
)

col1, col2 = st.columns([1, 5])
with col1:
    run = st.button("تحليل ▶", type="primary", use_container_width=True)
with col2:
    if st.button("مسح", use_container_width=False):
        st.session_state['isnad_input'] = ''
        st.rerun()

# ── Analysis ──────────────────────────────────────────────────────────────────
if run and isnad_input.strip():
    with st.spinner("جاري التحليل..."):
        names  = analyzer.parse_isnad_string(isnad_input.strip())
        result = analyzer.analyze(names, active_principles=active_principles or None)

    chain        = result['chain']
    final_status = result['final_status']
    tabaqah_gaps = result.get('tabaqah_gaps', [])

    # Final grade banner
    st.markdown(grade_banner(final_status['grade']), unsafe_allow_html=True)

    # Parsed names row
    st.markdown("**الرواة المُستخرجون من الإسناد:**")
    tags = " &nbsp;←&nbsp; ".join(
        f'<span style="direction:rtl;font-family:Segoe UI,Arial">{n}</span>'
        for n in names
    )
    st.markdown(f'<div style="direction:rtl;text-align:right;padding:8px;background:#111;border-radius:8px">{tags}</div>', unsafe_allow_html=True)
    st.markdown("---")

    # Per-narrator cards
    st.markdown("### تفاصيل الرواة")
    for idx, item in enumerate(chain):
        name_query   = item['original_query']
        res          = item['resolution']
        tm           = res.get('top_match')
        others       = res.get('other_candidates', [])
        match_case   = res.get('match_case', '')
        item_tabaqah = item.get('tabaqah')
        item_tab_lbl = item.get('tabaqah_label')
        item_tab_src = item.get('tabaqah_source')

        is_imam    = bool(tm and tm.get('canonical_key', '').startswith('IMAM_'))
        is_mursal  = bool(tm and tm.get('canonical_key', '') == 'VIRTUAL_MURSAL')
        is_virtual = bool(tm and tm.get('canonical_key', '').startswith('VIRTUAL')) and not is_imam and not is_mursal
        effective_status  = item.get('effective_status')
        principle_applied = item.get('principle_applied')

        if tm:
            raw_status = tm.get('status', 'unspecified')
            status     = effective_status or raw_status
            css        = card_class(status, is_virtual or is_mursal, is_imam)
            bdg        = badge(status, is_virtual, is_imam, is_mursal)
            matched  = tm.get('name_ar', '')
            score    = tm.get('confidence_score', 0)
            reasons  = tm.get('match_reasons', [])
            n3       = tm.get('n3_display', tm.get('canonical_key', ''))

            reasons_html = ""
            if reasons:
                reasons_html = f'<div class="reason-text">({"; ".join(reasons)})</div>'

            alt_html = ""
            near = [c for c in others if c.get('confidence_score', 0) >= score - 1.0]
            if near:
                alt_lines = []
                for c in near[:3]:
                    c_ar     = c.get('name_ar', '')
                    c_status = c.get('status', '')
                    c_score  = c.get('confidence_score', 0)
                    c_n3     = c.get('n3_display', c.get('canonical_key', ''))
                    c_badge  = badge(c_status)
                    c_tab    = c.get('tabaqah')
                    c_tab_str = f'<span class="badge badge-tabaqah" style="font-size:0.7rem">T{c_tab}</span>' if c_tab else ''
                    alt_lines.append(
                        f'<span class="alt-text">{c_ar} (n={c_n3}) {c_badge} {c_tab_str} — {c_score:.2f}</span>'
                    )
                alt_html = (
                    '<details style="margin-top:6px">'
                    '<summary style="font-size:0.8rem;color:#888;cursor:pointer">بدائل محتملة</summary>'
                    '<div style="padding:4px 0">' + '<br>'.join(alt_lines) + '</div>'
                    '</details>'
                )

            tab_bdg = tabaqah_badge(item_tabaqah, item_tab_lbl, item_tab_src)
            principle_html = ""
            if principle_applied:
                pname = PRINCIPLES[principle_applied]['name_ar']
                orig_label = STATUS_LABEL.get(raw_status, (raw_status, ''))[0]
                principle_html = (
                    f'<div style="margin-top:5px;font-size:0.8rem;color:#a0c878">'
                    f'⚖️ وُثِّق بمبدأ: {pname}'
                    f' <span style="color:#888">(الأصل: {orig_label})</span>'
                    f'</div>'
                )
            html = f"""
<div class="narrator-card {css}">
  <div style="display:flex;justify-content:space-between;align-items:center">
    <span class="score-text">({score:.2f}) n={n3} {tab_bdg}</span>
    <div>
      {bdg}
      <span class="narrator-name">{idx+1}. {name_query}</span>
    </div>
  </div>
  <div class="match-name">← {matched}</div>
  {reasons_html}
  {principle_html}
  {alt_html}
</div>
"""
        else:
            tab_bdg = tabaqah_badge(item_tabaqah, item_tab_lbl, item_tab_src)
            html = f"""
<div class="narrator-card unresolved">
  <div style="display:flex;justify-content:space-between;align-items:center">
    <span class="score-text">لم يُعثر عليه {tab_bdg}</span>
    <div>
      <span class="badge badge-unresolved">غير محلول ⚠</span>
      <span class="narrator-name">{idx+1}. {name_query}</span>
    </div>
  </div>
  <div class="match-name" style="color:#e74c3c">{res.get('message', 'لا يوجد تطابق في قاعدة البيانات')}</div>
</div>
"""
        st.markdown(html, unsafe_allow_html=True)

    # ── Tabaqah gap warnings ──────────────────────────────────────────────────
    if tabaqah_gaps:
        st.markdown("---")
        st.markdown("### تحليل الطبقات")
        for gap_info in tabaqah_gaps:
            sev = gap_info['severity']
            icon = "🔴" if sev == "major" else "🟡"
            a_name = gap_info['narrator_a']
            b_name = gap_info['narrator_b']
            t_a = gap_info['tabaqah_a']
            t_b = gap_info['tabaqah_b']
            note = gap_info['note']
            gap_html = f"""
<div class="gap-warning {sev}">
  {icon} <strong>{a_name}</strong> (T{t_a}) &larr; <strong>{b_name}</strong> (T{t_b}) &nbsp;|&nbsp; {note}
</div>
"""
            st.markdown(gap_html, unsafe_allow_html=True)
    else:
        # No gaps — show quiet confirmation only if we had at least some tabaqah data
        has_any_tabaqah = any(item.get('tabaqah') for item in chain)
        if has_any_tabaqah:
            st.markdown("---")
            st.markdown("### تحليل الطبقات")
            st.success("تسلسل الطبقات طبيعي — لا فجوات مشبوهة")

elif run and not isnad_input.strip():
    st.warning("الرجاء إدخال الإسناد أولاً")
