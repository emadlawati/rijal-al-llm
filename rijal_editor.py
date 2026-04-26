import streamlit as st
import json
import os

st.set_page_config(page_title="Rijal Database Editor", page_icon="📝", layout="wide")

DB_PATH = "rijal_database.json"

@st.cache_data
def load_db():
    if os.path.exists(DB_PATH):
        with open(DB_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_db(db_data):
    with open(DB_PATH, 'w', encoding='utf-8') as f:
        json.dump(db_data, f, ensure_ascii=False, indent=2)
    st.success("تم الحفظ بنجاح! / Saved successfully!")

st.markdown("""
<style>
    .stTextInput>div>div>input { direction: rtl; }
    .stTextArea>div>div>textarea { direction: rtl; }
</style>
""", unsafe_allow_html=True)

st.title("📝 Rijal Database Editor / محرر قاعدة بيانات الرجال")

# Load data into session state if not there
if 'db' not in st.session_state:
    st.session_state.db = load_db()

db = st.session_state.db

# Sidebar for navigation
st.sidebar.header("Navigation / التنقل")
action = st.sidebar.radio("Action / الإجراء", ["Edit Existing / تعديل موجود", "Add New / إضافة جديد"])

def get_empty_entry():
    return {
        "name_ar": "",
        "name_en": "",
        "father": None,
        "grandfather": None,
        "nasab": None,
        "kunyah": None,
        "laqab": None,
        "nisba": None,
        "status": "unspecified",
        "status_detail": None,
        "status_source": None,
        "sect": None,
        "narrates_from_imams": [],
        "companions_of": None,
        "narrates_from_narrators": [],
        "narrated_from_by": [],
        "books": [],
        "hadith_count": None,
        "has_book": False,
        "tariq_status": None,
        "aliases": [],
        "alias_entry_nums": [],
        "same_as_entry_nums": [],
        "same_as_names": [],
        "identity_confidence": "unique",
        "scribal_error_noted": False,
        "disambiguation_notes": None,
        "period_hint": None,
        "notes": None,
        "_entry_idx": None,
        "_num_najaf": None,
        "_num_beirut": None,
        "_num_tehran": None,
        "_raw": "",
        "_raw_full": "",
        "tabaqah": None,
        "tabaqah_detail": None,
        "tabaqah_sub": None,
        "tabaqah_source": None,
        "tabaqah_confidence": None
    }

if action == "Edit Existing / تعديل موجود":
    st.sidebar.subheader("Select Narrator / اختر राوی")
    
    # Create search options
    # We'll use id + name
    narrator_options = [f"{k} - {v.get('name_ar', 'Unknown')}" for k, v in db.items()]
    
    selected_option = st.sidebar.selectbox("Search Narrator / ابحث عن الراوي", options=narrator_options)
    
    if selected_option:
        entry_key = selected_option.split(" - ")[0]
        entry_data = db[entry_key]
        
        st.subheader(f"Editing: {entry_data.get('name_ar', '')} (ID: {entry_key})")
        
        with st.form("edit_form"):
            col1, col2 = st.columns(2)
            with col1:
                name_ar = st.text_input("Name (Arabic) / الاسم", value=entry_data.get("name_ar", ""))
                name_en = st.text_input("Name (English)", value=entry_data.get("name_en", ""))
                
                col_f1, col_f2 = st.columns(2)
                with col_f1:
                    father = st.text_input("Father / الأب", value=entry_data.get("father", "") or "")
                with col_f2:
                    grandfather = st.text_input("Grandfather / الجد", value=entry_data.get("grandfather", "") or "")
                    
                status = st.selectbox("Status / الحالة", 
                                      ["thiqah", "hasan", "mamduh", "muwaththaq", "daif", "majhul", "unspecified"],
                                      index=["thiqah", "hasan", "mamduh", "muwaththaq", "daif", "majhul", "unspecified"].index(entry_data.get("status", "unspecified")) if entry_data.get("status") in ["thiqah", "hasan", "mamduh", "muwaththaq", "daif", "majhul", "unspecified"] else 6)
                status_detail = st.text_input("Status Detail / تفاصيل ثقة", value=entry_data.get("status_detail", "") or "")
                
                # Convert ints/strs safely
                tabaqah_val = entry_data.get("tabaqah")
                tabaqah = st.number_input("Tabaqah / الطبقة", value=int(tabaqah_val) if tabaqah_val is not None else 0, min_value=0, max_value=20)
                tabaqah_detail = st.text_input("Tabaqah Detail / تفصيل الطبقة", value=entry_data.get("tabaqah_detail", "") or "")
            
            with col2:
                imams = st.text_input("Narrates from Imams (comma separated) / يروي عن الأئمة", value=", ".join(entry_data.get("narrates_from_imams", [])))
                narrates_from = st.text_area("Narrates from Narrators (one per line) / يروي عن", value="\n".join(entry_data.get("narrates_from_narrators", [])), height=80)
                narrated_by = st.text_area("Narrated from by Narrators (one per line) / روى عنه", value="\n".join(entry_data.get("narrated_from_by", [])), height=80)
                
                notes = st.text_area("Notes / ملاحظات", value=entry_data.get("notes", "") or "", height=80)
                raw_full = st.text_area("Raw Full Text / النص الأصلي", value=entry_data.get("_raw_full", "") or "", height=80)
            
            submitted = st.form_submit_button("Save Changes / حفظ التغييرات")
            
            if submitted:
                db[entry_key]["name_ar"] = name_ar
                db[entry_key]["name_en"] = name_en
                db[entry_key]["father"] = father if father else None
                db[entry_key]["grandfather"] = grandfather if grandfather else None
                db[entry_key]["status"] = status
                db[entry_key]["status_detail"] = status_detail if status_detail else None
                db[entry_key]["tabaqah"] = int(tabaqah) if tabaqah > 0 else None
                db[entry_key]["tabaqah_detail"] = tabaqah_detail if tabaqah_detail else None
                
                db[entry_key]["narrates_from_imams"] = [x.strip() for x in imams.split(",") if x.strip()]
                db[entry_key]["narrates_from_narrators"] = [x.strip() for x in narrates_from.split("\n") if x.strip()]
                db[entry_key]["narrated_from_by"] = [x.strip() for x in narrated_by.split("\n") if x.strip()]
                
                db[entry_key]["notes"] = notes if notes else None
                db[entry_key]["_raw_full"] = raw_full
                
                # Update state and save
                st.session_state.db = db
                save_db(db)

elif action == "Add New / إضافة جديد":
    st.subheader("Add New Narrator / إضافة راوٍ جديد")
    
    with st.form("add_form"):
        col1, col2 = st.columns(2)
        with col1:
            name_ar = st.text_input("Name (Arabic) / الاسم")
            name_en = st.text_input("Name (English)")
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                father = st.text_input("Father / الأب")
            with col_f2:
                grandfather = st.text_input("Grandfather / الجد")
            status = st.selectbox("Status / الحالة", ["thiqah", "hasan", "mamduh", "muwaththaq", "daif", "majhul", "unspecified"], index=6)
            tabaqah = st.number_input("Tabaqah / الطبقة", value=0, min_value=0, max_value=20)
        
        with col2:
            imams = st.text_input("Narrates from Imams (comma separated) / يروي عن الأئمة")
            narrates_from = st.text_area("Narrates from Narrators (one per line) / يروي عن", height=80)
            narrated_by = st.text_area("Narrated from by Narrators (one per line) / روى عنه", height=80)
            notes = st.text_area("Notes / ملاحظات", height=80)
            raw_full = st.text_area("Raw Full Text / النص الأصلي", height=80)
            
        submitted = st.form_submit_button("Add Narrator / إضافة")
        
        if submitted:
            if not name_ar:
                st.error("Arabic name is required! / الاسم العربي مطلوب!")
            else:
                # generate key (max key + 1)
                existing_keys = []
                for k in db.keys():
                    try:
                        existing_keys.append(int(k))
                    except ValueError:
                        pass
                new_key = str(max(existing_keys) + 1 if existing_keys else 0)
                
                new_entry = get_empty_entry()
                new_entry["name_ar"] = name_ar
                new_entry["name_en"] = name_en
                new_entry["father"] = father if father else None
                new_entry["grandfather"] = grandfather if grandfather else None
                new_entry["status"] = status
                new_entry["tabaqah"] = int(tabaqah) if tabaqah > 0 else None
                
                new_entry["narrates_from_imams"] = [x.strip() for x in imams.split(",") if x.strip()]
                new_entry["narrates_from_narrators"] = [x.strip() for x in narrates_from.split("\n") if x.strip()]
                new_entry["narrated_from_by"] = [x.strip() for x in narrated_by.split("\n") if x.strip()]
                
                new_entry["notes"] = notes if notes else None
                new_entry["_raw_full"] = raw_full
                new_entry["_entry_idx"] = int(new_key)
                
                db[new_key] = new_entry
                st.session_state.db = db
                save_db(db)
                st.success(f"Added successfully with ID: {new_key} / تمت الإضافة بنجاح برقم: {new_key}")
