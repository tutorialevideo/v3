# Portal JUST Downloader - PRD

## Problem Statement
Aplicație pentru descărcarea automată a dosarelor de firme din portalquery.just.ro zilnic, cu stocare în PostgreSQL pentru procesare ulterioară.

## User Persona
- Utilizator care monitorizează dosarele juridice ale firmelor din România
- Necesită date structurate pentru asignare CUI și analiză

## Core Requirements
1. ✅ Descărcare automată dosare pentru firme
2. ✅ Extragere doar companii (SC, SRL, SA, etc.) - nu persoane fizice
3. ✅ Stocare în PostgreSQL cu relații: firme → dosare → timeline
4. ✅ Fiecare firmă poate avea CUI pentru identificare
5. ✅ Cron job zilnic configurabil
6. ✅ Filtrare pe perioadă

## Database Schema (PostgreSQL)

```
firme
├── id (PK, BIGINT)
├── cui (VARCHAR, UNIQUE, NULL) -- completat manual
├── denumire (VARCHAR)
├── denumire_normalized (VARCHAR) -- pentru căutare
├── created_at, updated_at

dosare  
├── id (PK, BIGINT)
├── firma_id (FK → firme)
├── numar_dosar
├── institutie, obiect, stadiu, categorie, materie
├── data_dosar
├── raw_data (JSON) -- date originale complete

timeline
├── id (PK, BIGINT)
├── dosar_id (FK → dosare)
├── tip (sedinta, hotarare, cale_atac)
├── data, descriere
├── detalii (JSON)
```

## API Endpoints

### Configurare & Rulare
- `GET/PUT /api/config` - Configurare job
- `POST /api/run` - Rulare manuală
- `GET /api/cron/status` - Status cron

### PostgreSQL Data
- `GET /api/db/stats` - Statistici DB
- `GET /api/db/firme` - Lista firme (cu paginare)
- `GET /api/db/firme/{id}` - Detalii firmă + dosare
- `PUT /api/db/firme/{id}` - Actualizare CUI
- `GET /api/db/dosare/{id}` - Detalii dosar + timeline

## Current Stats (2026-03-18)
- **287 firme** extrase
- **298 dosare** salvate
- **547 evenimente timeline**
- **246 instituții** verificate

## Tech Stack
- Backend: FastAPI + SQLAlchemy + asyncpg + APScheduler
- Database: PostgreSQL (firme, dosare, timeline) + MongoDB (config, logs)
- Frontend: React + Shadcn UI

## Next Steps (P1)
1. [ ] Import CSV cu CUI-uri pentru firme
2. [ ] Export date pentru analiză
3. [ ] Dashboard cu vizualizare dosare pe firmă
