# Deploying Rijal-al-LLM to Streamlit Community Cloud

End-to-end guide: from a fresh `c:\Imad Works\Religious - Copy - Copy - Copy\`
folder to a public site at **rijal-al-llm.org**.

---

## What you'll have when you're done

- Public GitHub repo at `github.com/emadlawati/rijal-al-llm`
- Streamlit Cloud auto-deploying every push to `main`
- Default URL: `rijal-al-llm.streamlit.app`
- Custom domain: `rijal-al-llm.org` → CNAME to Streamlit
- Automatic HTTPS via Let's Encrypt (managed by Streamlit Cloud)

Time required: ~30 minutes once you have the GitHub account ready and the
domain purchased.

---

## Phase 1 — Pre-flight check (5 min)

Verify everything you need is in place. Open PowerShell and run from the
project root:

```powershell
# Confirm key files exist
Get-Item app.py, requirements.txt, .gitignore, .streamlit/config.toml, LICENSE

# Confirm key data files exist (and check size — must each be < 100 MB
# for GitHub without LFS)
Get-ChildItem rijal_database_merged.json, rijal_resolver_index.json,
              rijal_identities.json, alf_rajul_database.json,
              alf_rajul_disambiguation_llm.json, mufid_statuses.json,
              tabaqah_overrides.json |
  Select-Object Name, @{N='SizeMB';E={[math]::Round($_.Length/1MB,1)}}
```

Expected result: every file present, no individual file over 100 MB.

If `allBooks.json` (~99 MB) is also there, decide whether to include it. The
**isnad analyzer doesn't need it** — only the translator/tagger does. Adding
it doubles the repo size and adds ~30 s to cold start. Recommendation:
exclude it for v1 (already covered by the data-file logic in your analyzer).

---

## Phase 2 — Initialize Git and push to GitHub (10 min)

### 2.1 Create the GitHub repo

1. Go to https://github.com/new
2. Owner: `emadlawati`
3. Repository name: `rijal-al-llm`
4. Description: `محلل الإسناد الشيعي — Computational analyzer for Shia hadith chains, grading by classical rijal science`
5. Public ✓
6. **Do NOT** initialize with README/license/.gitignore (we already have them)
7. Click **Create repository**

You'll see a "Quick setup" page with your repo URL. Copy the HTTPS URL —
something like `https://github.com/emadlawati/rijal-al-llm.git`.

### 2.2 Initialize git locally and push

```powershell
cd "C:\Imad Works\Religious - Copy - Copy - Copy"

# First-time identity (if not already configured globally)
git config user.name "HakiNinth"
git config user.email "your-github-email@example.com"

# Initialize repo
git init -b main
git add .

# Sanity-check what's staged BEFORE committing — you don't want to push
# the 144 MB tagger_backend/ or 103 MB tagger_frontend/ by accident.
# Look for any line listing those folders or any *.pdf / *.xlsx file:
git status | findstr /R "tagger_backend tagger_frontend frontend\\node \.pdf$ \.xlsx$ summary_pages drive-download venv __pycache__"
# If that prints anything, the .gitignore isn't catching it. Stop and fix
# the .gitignore before continuing. (If empty, you're clean.)

git commit -m "Initial commit: isnad analyzer with rijal database and tabaqah inference"

# Connect to GitHub and push
git remote add origin https://github.com/emadlawati/rijal-al-llm.git
git push -u origin main
```

The first push will take a couple of minutes — you're sending ~110 MB of
data files. If it stalls, the most common cause is GitHub blocking a single
file over 100 MB. To check:

```powershell
Get-ChildItem -Recurse | Where-Object { $_.Length -gt 100MB } |
  Select-Object FullName, @{N='SizeMB';E={[math]::Round($_.Length/1MB,1)}}
```

If anything is listed and it's a data file you actually need, install Git
LFS and convert that file:

```powershell
git lfs install
git lfs track "filename-over-100mb.json"
git add .gitattributes filename-over-100mb.json
git commit --amend
git push --force-with-lease origin main
```

---

## Phase 3 — Connect Streamlit Cloud (5 min)

1. Go to https://share.streamlit.io and sign in with GitHub. Authorize the
   Streamlit GitHub app to read your repos when prompted.

2. Click **Create app** → **Deploy a public app from GitHub**.

3. Fill in:
   - **Repository**: `emadlawati/rijal-al-llm`
   - **Branch**: `main`
   - **Main file path**: `app.py`
   - **App URL**: choose `rijal-al-llm` (gives you `rijal-al-llm.streamlit.app`)
   - **Python version**: 3.11 (the closest to what you used locally)

4. Click **Advanced settings** to set Python version explicitly. No secrets
   to add for the v1 (the analyzer needs no API keys at runtime — those
   are only used by the offline tabaqah inference and matcher tools).

5. Click **Deploy**.

Streamlit Cloud will:
- Clone your repo
- Install `requirements.txt`
- Run `streamlit run app.py`
- Stream the build log live

First deploy takes 3–5 minutes (most of it is the initial pull of 110 MB).
If the build log shows red errors, the most likely culprits are:

| Error | Fix |
|---|---|
| `ModuleNotFoundError: rijal_resolver` | A `.py` file got excluded by `.gitignore`. Check `git ls-files | grep rijal_resolver` — if missing, remove the matching line from `.gitignore` and re-commit. |
| `FileNotFoundError: rijal_database_merged.json` | The data file got gitignored. Run `git ls-files | grep rijal_database_merged.json` — if missing, force-add: `git add -f rijal_database_merged.json && git commit -m "Force-add data" && git push`. |
| `MemoryError` during analyzer load | The free tier's 1 GB RAM is exceeded. Strip `_raw_full` text from the merged DB before deploying (saves ~30 % of memory). I can write a one-time stripping script if needed. |
| `streamlit not found` | `requirements.txt` corrupted again. Confirm the file is plain UTF-8 with no BOM by opening it in VS Code / Notepad++. |

When the build succeeds, you'll see a green `running` badge and a public
URL. Visit it to confirm the analyzer loads and runs.

---

## Phase 4 — Custom domain `rijal-al-llm.org` (10 min)

You need to:
1. **Buy the domain** (if you haven't yet)
2. **Tell Streamlit you want to use it**
3. **Point the domain at Streamlit via DNS**
4. **Wait for HTTPS to provision**

### 4.1 Buy the domain

Any registrar works — Namecheap, Cloudflare, Porkbun, GoDaddy. `.org` for
Rijal-al-LLM is around $10–15/year. **Cloudflare is recommended** because
it gives you free DNS, free DDoS protection, and a clean dashboard.

If using Cloudflare:
1. Sign up → Add Site → enter `rijal-al-llm.org`
2. Cloudflare will tell you to update your registrar's nameservers to
   theirs (e.g., `xena.ns.cloudflare.com` and `kirk.ns.cloudflare.com`).
   Do that at your registrar.
3. Wait 10–60 minutes for nameserver propagation.

### 4.2 Tell Streamlit Cloud about the domain

1. In Streamlit Cloud, click your app → **Settings** → **General**.
2. Scroll to **Custom subdomain**. Add `rijal-al-llm.org` and click Save.
3. Streamlit shows you a verification CNAME target like
   `c1234567890abcdef.streamlit.app` (specific to your app).

### 4.3 Add the DNS records

In your registrar's DNS panel (Cloudflare in this example):

| Type   | Name           | Target                              | Proxy/TTL |
|--------|----------------|-------------------------------------|-----------|
| CNAME  | `@`            | `<your-app>.streamlit.app`          | DNS only / Auto |
| CNAME  | `www`          | `<your-app>.streamlit.app`          | DNS only / Auto |

If your registrar doesn't support CNAME on the apex (`@`), use the
"ANAME" / "ALIAS" / "flattening" feature — Cloudflare supports CNAME-flattening
on apex automatically.

**Important on Cloudflare:** turn the orange-cloud (proxy) **OFF** for
these records (set them to "DNS only" / grey cloud). Streamlit Cloud
provisions HTTPS via Let's Encrypt, and the proxied path can interfere
with that. Once HTTPS is live and stable (usually 24 hours), you can
turn the proxy back on if you want.

### 4.4 Wait for HTTPS

Streamlit will detect the CNAME, request a Let's Encrypt certificate, and
flip the green padlock on. Most CNAMEs propagate within minutes; HTTPS
provisioning takes 5–30 min after that. If it's still pending after 2 hours,
double-check the CNAME target in `dig` or `nslookup`:

```powershell
nslookup rijal-al-llm.org
# Expected: shows a CNAME pointing to *.streamlit.app or its A records.
```

### 4.5 Done

Visit https://rijal-al-llm.org and confirm:
- Page loads with the analyzer UI
- HTTPS green padlock visible
- The example sidebar buttons populate the input
- Clicking تحليل ▶ on an example produces the chain analysis

---

## Phase 5 — Operations: keeping it running

**Cold starts.** Streamlit Cloud apps idle out after ~30 min of no traffic.
The next visitor will see a "App is starting" splash for ~60–90 s while
the analyzer reloads the 100+ MB database. This is fine for low traffic;
for sustained use, consider upgrading to a paid tier.

**Pushing updates.**

```powershell
# After making code or data changes:
git add .
git commit -m "Improvements to kunya disambiguator"
git push origin main
# Streamlit Cloud auto-rebuilds within a minute.
```

**Rolling back a bad deploy.**

```powershell
git revert HEAD
git push origin main
# Streamlit Cloud auto-rebuilds the previous version.
```

**Monitoring.** Streamlit Cloud → app → **Manage app** shows live logs and
restart history. Watch for memory-pressure warnings on the free tier.

---

## Phase 6 — When traffic outgrows the free tier

The free Streamlit Cloud tier covers maybe 50–100 daily users on a small
dataset like this. If you exceed:

| Bottleneck | Symptom | Fix |
|---|---|---|
| Memory | "App ran out of resources" errors | Strip `_raw_full` from the deployed DB; use `@st.cache_resource` for heavy objects (already done); upgrade to Pro ($20/mo, 4 GB RAM) |
| Cold starts | Users see splash for 90+ s | Pro tier keeps app warm; or switch to Hugging Face Spaces (similar limits) or a small VPS |
| Concurrency | Multiple chains-in-progress slow each other down | Pro tier has more CPU; or split into FastAPI backend on Railway + frontend on Vercel |

For the foreseeable future, the free tier is fine.

---

## Quick reference — where everything is

```
rijal-al-llm/
├── app.py                       # Streamlit UI (entry point)
├── isnad_analyzer.py            # Core 2900-line analyzer
├── isnad_extractor.py           # Sanad extraction
├── isnad_parser.py              # Chain parsing
├── isnad_rules.py               # Rijal principles
├── irsal_detector.py            # Mursal detection
├── rijal_resolver.py            # Narrator resolution
├── database_loader.py           # In-memory DB + LRU cache
├── tabaqah_inference.py         # Generation inference (4 tiers)
├── transmission_validator.py    # Lifetime feasibility check
├── kunya_disambiguator.py       # أبو بصير → which one?
├── alf_rajul_extractor.py       # Mu'jam al-Alf Rajul parser
├── alf_rajul_llm_matcher.py     # LLM disambiguation tool
├── rijal_lifetime_extractor.py  # Birth/death year extraction
│
├── rijal_database_merged.json   # ~15k narrator entries (canonical)
├── rijal_resolver_index.json    # Pre-computed resolution index
├── rijal_identities.json        # Identity clusters
├── alf_rajul_database.json      # 988 authoritative tabaqāt
├── alf_rajul_disambiguation_llm.json
├── mufid_statuses.json          # Per-scholar verdicts
├── tabaqah_overrides.json       # Manual corrections
│
├── .streamlit/config.toml       # Theme + RTL
├── .gitignore
├── requirements.txt
├── README.md
├── LICENSE
└── DEPLOY.md                    # ← this file
```
