# Portal JUST Downloader - PRD

## Problem Statement
Aplicație pentru descărcarea automată a dosarelor de firme din portalquery.just.ro, cu stocare în PostgreSQL pentru procesare ulterioară. Suportă filtrare pe perioadă de timp și/sau firmă, plus îmbogățire date din ANAF și MFinante.

## User Persona
- Utilizator care monitorizează dosarele juridice ale firmelor din România
- Necesită date structurate pentru asignare CUI și analiză

## Core Requirements
1. ✅ Descărcare automată dosare pentru firme (opțional: fără firmă, pe perioadă)
2. ✅ Extragere doar companii (SC, SRL, SA, etc.) - nu persoane fizice
3. ✅ Stocare în PostgreSQL cu relații: firme → dosare → timeline
4. ✅ Fiecare firmă poate avea CUI pentru identificare
5. ✅ Cron job zilnic configurabil
6. ✅ Filtrare pe perioadă (date_start / date_end)
7. ✅ Căutare FĂRĂ firmă specificată (doar pe perioadă de timp)
8. ✅ Sync ANAF pentru îmbogățire date
9. ✅ Scraper MFinante cu CAPTCHA interactiv
10. ✅ Bilanțuri istorice (tabel separat)

## Database Schema (PostgreSQL)

```
firme
├── id (PK, BIGINT)
├── cui (VARCHAR, UNIQUE, NULL)
├── denumire, denumire_normalized
├── date juridice (cod_inregistrare, forma_juridica, etc.)
├── date ANAF (anaf_denumire, anaf_adresa, anaf_stare, etc.)
├── date MFinante (mf_cifra_afaceri, mf_profit_net, etc.)
├── created_at, updated_at

dosare
├── id, firma_id (FK), numar_dosar
├── institutie, obiect, stadiu, categorie, materie, data_dosar
├── raw_data (JSON)

timeline
├── id, dosar_id (FK), tip, data, descriere, detalii (JSON)

bilanturi (NEW)
├── id, firma_id (FK), an_bilant
├── cifra_afaceri_neta, profit_brut, pierdere_bruta
├── numar_mediu_salariati, active_imobilizate, etc.
```

## API Endpoints
- `GET/PUT /api/config` - Configurare job
- `POST /api/run` - Rulare manuală (search_term opțional, date_range obligatoriu dacă nu e search_term)
- `POST /api/search` - Preview dosare (company_name opțional)
- `GET /api/db/stats` - Statistici DB
- `GET /api/db/firme` - Lista firme (cu paginare)
- `GET /api/firma/{id}` - Profil firmă complet
- `PUT /api/db/firme/{id}` - Actualizare CUI
- `GET /api/db/firme-cu-cui` - Firme cu CUI (tab DB Final)
- `GET /api/anaf/sync` - Sync ANAF
- `POST /api/anaf/test-cui-details` - Test CUI cu detalii raw
- `GET /api/mfinante/captcha/init|image|solve` - CAPTCHA MFinante
- `GET /api/bilanturi/{firma_id}` - Bilanțuri istorice
- `POST /api/db/reconnect` - Reconectare PostgreSQL

## Tech Stack
- Backend: FastAPI + SQLAlchemy + asyncpg + APScheduler + aiohttp
- Database: PostgreSQL (firme, dosare, timeline, bilanturi) + MongoDB (config, logs)
- Frontend: React + Shadcn UI + Tailwind CSS
- Deployment: Docker Compose + Nginx

## Current Stats (2026-03-19)
- Aplicație funcțională în mediul preview
- Backend + Frontend + MongoDB running

## Backlog (Prioritizat)
### P1 - Completat 2026-03-19
- [x] Completare UI modal "Company Profile" cu toate datele (ANAF, bilanțuri, dosare + editare CUI)
- [x] Completare tab "DB Final" cu tabel + paginare + filtre
- [x] Log detaliat "Verifică CUI" — rezumat vizual + JSON Raw complet expandabil
- [x] Editare manuală CUI inline în tabel și în modalul de profil

### P2 - Important
- [ ] Export CSV/Excel date firme
- [ ] Selector instituție multi-select pentru Justice Portal

### P3 - Completat 2026-03-19
- [x] Refactorizare server.py (3879 linii → server.py 82 linii + 5 module routes/ + 5 module shared)
- [x] Refactorizare App.js (3008 linii → App.js 1180 linii + 5 pages/ + 2 components/modals)

## Test Reports
- iteration_5.json: Profile Modal, Inline CUI edit, DB Final, ANAF test — 23/23 OK
- iteration_6.json: Validare completă toate 5 taburi + Export CSV — 31/31 OK
  - Bug fix: AnafPage.jsx destructuring (7 variabile lipsă ctx)
