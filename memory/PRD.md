# Portal JUST Downloader - PRD

## Original Problem Statement
Aplicație pentru descărcarea automată a dosarelor de firme din portalquery.just.ro zilnic.

## User Persona
- Utilizator care dorește să monitorizeze dosarele juridice ale firmelor din România
- Are nevoie de acces la date din toate cele 246 de instituții judiciare

## Core Requirements
1. Descărcare automată dosare pentru firme
2. Salvare locală în format JSON
3. Interfață web pentru monitorizare și configurare
4. Ora de execuție setabilă
5. Fără notificări email
6. **NOU:** Selectare perioadă (data început/sfârșit)
7. **NOU:** Cron job zilnic activ

## What's Been Implemented (2026-03-18)
- ✅ Backend API cu FastAPI pentru comunicare SOAP cu portalquery.just.ro
- ✅ Suport pentru toate cele 246 instituții (16 Curți de Apel, 52 Tribunale, 178 Judecătorii)
- ✅ Endpoint-uri: /api/config, /api/run, /api/search, /api/files, /api/stats, /api/runs, /api/cron/status
- ✅ Filtrare dosare pe perioadă (CautareDosare2 cu dataStart/dataStop)
- ✅ Cron job zilnic cu APScheduler
- ✅ Frontend dashboard cu statistici, configurare, istoric
- ✅ Calendar picker pentru selectare dată început/sfârșit
- ✅ Switch pentru activare/dezactivare cron
- ✅ Afișare următoarea rulare programată
- ✅ Preview căutare cu filtrare pe date
- ✅ Salvare locală în /app/backend/downloads/ în format JSON

## Technical Stack
- Backend: FastAPI + aiohttp (SOAP client) + MongoDB + APScheduler
- Frontend: React + Tailwind CSS + Shadcn UI (Calendar, Popover, Switch)
- Storage: Local JSON files + MongoDB pentru config/logs

## Prioritized Backlog
### P0 (Done)
- [x] API SOAP integration cu CautareDosare2
- [x] Download job pentru toate cele 246 instituții
- [x] Frontend dashboard cu date pickers și cron toggle
- [x] Config persistentă
- [x] Cron job zilnic cu APScheduler

### P1 (Next)
- [ ] Selectare specifică de instituții
- [ ] Export în alte formate (CSV, Excel)
- [ ] Căutare în dosarele descărcate

### P2 (Future)
- [ ] Notificări când apar dosare noi
- [ ] Comparație între rulări
- [ ] API pentru acces extern
