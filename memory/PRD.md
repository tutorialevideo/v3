# Portal JUST Downloader - PRD

## Original Problem Statement
Aplicație pentru descărcarea automată a dosarelor de firme din portalquery.just.ro zilnic.

## User Persona
- Utilizator care dorește să monitorizeze dosarele juridice ale firmelor din România
- Are nevoie de acces la date din toate cele 62 de instituții judiciare

## Core Requirements
1. Descărcare automată dosare pentru firme
2. Salvare locală în format JSON
3. Interfață web pentru monitorizare și configurare
4. Ora de execuție setabilă
5. Fără notificări email

## What's Been Implemented (2026-03-18)
- ✅ Backend API cu FastAPI pentru comunicare SOAP cu portalquery.just.ro
- ✅ Endpoint-uri: /api/config, /api/run, /api/search, /api/files, /api/stats, /api/runs
- ✅ Descărcare din toate cele 62 instituții judiciare din România
- ✅ Salvare locală în /app/backend/downloads/ în format JSON
- ✅ Frontend dashboard cu statistici, configurare, istoric
- ✅ Preview căutare înainte de descărcare
- ✅ Rulare manuală job
- ✅ Descărcare/ștergere fișiere din interfață

## Technical Stack
- Backend: FastAPI + aiohttp (SOAP client) + MongoDB
- Frontend: React + Tailwind CSS + Shadcn UI
- Storage: Local JSON files + MongoDB pentru config/logs

## Prioritized Backlog
### P0 (Done)
- [x] API SOAP integration
- [x] Download job pentru toate instituțiile
- [x] Frontend dashboard
- [x] Config persistentă

### P1 (Next)
- [ ] Cron job real cu APScheduler pentru descărcare automată la ora setată
- [ ] Export în alte formate (CSV, Excel)
- [ ] Căutare în dosarele descărcate

### P2 (Future)
- [ ] Notificări când apar dosare noi
- [ ] Comparație între rulări
- [ ] API pentru acces extern
