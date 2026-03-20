# Portal JUST Downloader - PRD

## Problem Statement
Aplicatie full-stack (React, FastAPI, MongoDB) pentru descarcarea automata si gestionarea datelor despre firmele romanesti din surse guvernamentale (Portal Justitie, ONRC, ANAF, MFinante).

## User Persona
- Utilizator care monitorizeaza dosarele juridice ale firmelor din Romania
- Necesita date structurate pentru asignare CUI si analiza

## Core Requirements
1. Descarcare automata dosare pentru firme (optional: fara firma, pe perioada)
2. Extragere doar companii (SC, SRL, SA, etc.) - nu persoane fizice
3. Stocare in MongoDB cu relatii: firme -> dosare -> timeline
4. Fiecare firma poate avea CUI pentru identificare
5. Cron job zilnic configurabil
6. Filtrare pe perioada (date_start / date_end)
7. Cautare FARA firma specificata (doar pe perioada de timp)
8. Sync ANAF pentru imbogatire date
9. Scraper MFinante cu CAPTCHA automatic (Tesseract OCR + Gemini fallback)
10. Bilanturi istorice (tabel separat)
11. BPI PDF Parser cu LiteParse, export CUI-uri la CSV si import in DB
12. Cloud Sync (MongoDB local -> Atlas)
13. Crawler MFirme.ro

## Tech Stack
- Backend: FastAPI + MongoDB (motor async driver)
- Frontend: React + Shadcn UI + Tailwind CSS
- Database: MongoDB (local Docker + Atlas cloud sync)
- Local Processing: Tesseract OCR, LiteParse, aiohttp

## Code Architecture
```
/app
├── backend/
│   ├── server.py           # FastAPI entry point
│   ├── mongo_db.py         # MongoDB collections and connection
│   ├── routes/             # Modular endpoints
│   │   ├── anaf.py, bpi.py, crawler.py, diagnostics.py
│   │   ├── firme.py, jobs.py, localitati.py, mfinante.py
│   │   └── atlas_sync.py
│   └── downloads/bpi/      # BPI CSV exports
└── frontend/
    ├── src/
    │   ├── App.js          # Main component
    │   ├── pages/          # Tab pages (AnafPage, BpiPage, etc.)
    │   └── components/     # Reusable UI + modals
    └── .env
```

## Key DB Schema (MongoDB)
- **firme**: Main collection, company data with nested anaf_data/mf_data
- **dosare**: Court case details
- **bilanturi**: Historical financial statements
- **job_config/job_runs**: Scraper configuration and history
- **bpi_records**: Parsed BPI PDF records
- **judete/localitati**: Romanian locations data

## Completed Features
- [x] Full PostgreSQL to MongoDB migration
- [x] Justice Portal scraper with date-range and case-category filters
- [x] ANAF scraper (handles 2.5M+ records)
- [x] MFinante scraper with auto-CAPTCHA (Tesseract OCR + Gemini fallback)
- [x] MFirme.ro crawler
- [x] BPI PDF Parser (LiteParse) with single/multi/folder uploads
- [x] BPI CUI Export to CSV + Import to firme DB (VERIFIED 2026-03-20)
- [x] MongoDB Atlas cloud sync
- [x] Backend refactoring (routes modulare)
- [x] Frontend refactoring (pages separate)
- [x] Localitati DB import + normalization

## Backlog (Prioritized)
### P1
- [ ] Multi-select filter for institutions (courts) in Justice Portal scraper
### P2
- [ ] Native Excel (.xlsx) export
- [ ] Link BPI records to dosare from Justice Portal
- [ ] Filter firme by judet/region
### P3
- [ ] Financial evolution chart in Company Profile modal
- [ ] Fix nr_reg_com formatting (J19 56 2017 -> J19/56/2017)

## Test Reports
- iteration_5.json: Profile Modal, Inline CUI edit, DB Final, ANAF test - 23/23 OK
- iteration_6.json: All 5 tabs + Export CSV - 31/31 OK
- iteration_7.json: BPI CUI Exports (list, download, import, dedup) - 14/14 OK (2026-03-20)
