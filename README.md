# Portal JUST Downloader - Docker Setup

Aplicație pentru descărcarea automată a dosarelor de firme din portalquery.just.ro

## Cerințe

- Docker 20.10+
- Docker Compose 2.0+
- Minim 4GB RAM
- 10GB spațiu disk (pentru date)

## Pornire Rapidă

```bash
# 1. Clonează repository-ul
git clone <repository-url>
cd portal-just-downloader

# 2. Pornește toate serviciile
docker-compose up -d --build

# 3. Verifică că totul rulează
docker-compose ps

# 4. Accesează aplicația
# Frontend: http://localhost:3080
# Backend API: http://localhost:8001/api/
```

## Servicii

| Serviciu | Port Intern | Port Extern | Descriere |
|----------|-------------|-------------|-----------|
| Frontend | 80 | **3080** | React UI |
| Backend | 8001 | 8001 | FastAPI API |
| PostgreSQL | 5432 | **5433** | Baza de date firme/dosare |
| MongoDB | 27017 | **27018** | Config & job logs |

> **Notă:** Porturile externe sunt remapate pentru a evita conflicte cu servicii locale.

## Structura Proiectului

```
.
├── docker-compose.yml      # Orchestrare servicii
├── backend/
│   ├── Dockerfile          # Backend container
│   ├── server.py           # FastAPI application
│   └── requirements.txt    # Python dependencies
├── frontend/
│   ├── Dockerfile          # Frontend container
│   ├── nginx.conf          # Nginx config
│   └── src/                # React source code
└── README.md
```

## Utilizare

### 1. Import Firme din ONRC

1. Accesează http://localhost:3080
2. În secțiunea "Import CUI pentru Firme", click "Import CSV"
3. Selectează fișierul ONRC (format: `DENUMIRE^CUI^...`)
4. Așteaptă procesarea (poate dura câteva minute pentru fișiere mari)

### 2. Descărcare Dosare

1. În "Configurare", introdu numele firmei (ex: "DEDEMAN")
2. Opțional: setează perioada (Data început / Data sfârșit)
3. Click "Rulează Acum" pentru descărcare manuală
4. SAU activează "Cron Zilnic Activ" pentru descărcare automată

### 3. Vizualizare Firme

1. Click pe tab-ul "Firme"
2. Caută după denumire sau CUI
3. Vezi dosarele asociate fiecărei firme

## Comenzi Docker

```bash
# Pornire
docker-compose up -d

# Oprire
docker-compose down

# Logs
docker-compose logs -f backend
docker-compose logs -f frontend

# Rebuild după modificări
docker-compose up -d --build

# Resetare completă (șterge datele!)
docker-compose down -v
docker-compose up -d
```

## Backup Date

```bash
# Backup PostgreSQL
docker exec just_postgres pg_dump -U justapp justportal > backup.sql

# Restore PostgreSQL
cat backup.sql | docker exec -i just_postgres psql -U justapp justportal

# Backup downloads
docker cp just_backend:/app/downloads ./downloads_backup
```

## Configurare Producție

Pentru deployment în producție, modifică `docker-compose.yml`:

```yaml
frontend:
  build:
    args:
      - REACT_APP_BACKEND_URL=https://api.your-domain.com
```

Și adaugă un reverse proxy (nginx/traefik) pentru HTTPS.

## Troubleshooting

### Backend nu pornește
```bash
# Verifică logs
docker-compose logs backend

# Verifică conexiunea la PostgreSQL
docker exec just_backend curl -f http://localhost:8001/api/
```

### Import CSV eșuează
- Verifică formatul fișierului (delimitator `^`)
- Verifică dimensiunea (max 300MB)
- Verifică encoding-ul (UTF-8 sau Latin-1)

### Memory issues
- Crește limita de memorie în Docker
- Pentru fișiere > 100MB, procesarea poate dura mai mult

## API Endpoints

| Endpoint | Metodă | Descriere |
|----------|--------|-----------|
| `/api/config` | GET/PUT | Configurare job |
| `/api/run` | POST | Pornire manuală |
| `/api/db/import-cui` | POST | Import CSV firme |
| `/api/db/firme` | GET | Lista firme |
| `/api/db/stats` | GET | Statistici DB |
| `/api/cron/status` | GET | Status cron |

## Licență

MIT
