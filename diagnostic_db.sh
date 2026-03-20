#!/bin/bash
# diagnostic_db.sh - Exporta date diagnosticare din PostgreSQL Docker
# Rulare: bash diagnostic_db.sh
# Output: fisiere in ./diagnostic_output/

set -e

OUTPUT_DIR="./diagnostic_output"
mkdir -p "$OUTPUT_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo "================================================"
echo "  Diagnosticare Baza de Date - $TIMESTAMP"
echo "================================================"
echo ""

# Detecteaza containerul postgres
PG_CONTAINER=$(docker compose ps -q postgres 2>/dev/null || docker ps --filter "name=postgres" --filter "name=just" -q | head -1)
if [ -z "$PG_CONTAINER" ]; then
  echo "ERROR: Nu gasesc containerul PostgreSQL. Asigura-te ca docker compose e pornit."
  exit 1
fi
echo "Container PostgreSQL: $PG_CONTAINER"
echo ""

# Helper functie
run_query() {
  docker exec "$PG_CONTAINER" psql -U justapp -d justportal -t -A -F',' -c "$1" 2>/dev/null
}

run_query_csv() {
  docker exec "$PG_CONTAINER" psql -U justapp -d justportal -c "COPY ($1) TO STDOUT WITH CSV HEADER DELIMITER ',';" 2>/dev/null
}

# ─── 1. STATISTICI GENERALE ───────────────────────────────────────────────────
echo ">>> 1. Statistici generale..."
STATS_FILE="$OUTPUT_DIR/stats_${TIMESTAMP}.txt"

cat > "$STATS_FILE" << 'STATS_SQL'
STATISTICI BAZA DE DATE
=======================
STATS_SQL

docker exec "$PG_CONTAINER" psql -U justapp -d justportal << 'SQL' >> "$STATS_FILE" 2>/dev/null
\echo '--- FIRME ---'
SELECT 
  COUNT(*) as "Total firme",
  COUNT(cui) as "Cu CUI",
  COUNT(*) - COUNT(cui) as "Fara CUI"
FROM firme;

\echo ''
\echo '--- STATUS ANAF ---'
SELECT 
  COALESCE(anaf_sync_status, 'nesincronizat') as "Status",
  COUNT(*) as "Nr firme",
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as "Procent %"
FROM firme
GROUP BY anaf_sync_status
ORDER BY COUNT(*) DESC;

\echo ''
\echo '--- FIRME ACTIVE ANAF ---'
SELECT 
  COUNT(*) as "Total active ANAF",
  COUNT(mf_last_sync) as "Sincronizate MFinante",
  COUNT(mf_cifra_afaceri) as "Cu cifra afaceri"
FROM firme
WHERE anaf_sync_status = 'found'
  AND anaf_stare ILIKE '%ACTIV%'
  AND anaf_stare NOT ILIKE '%INACTIV%'
  AND anaf_stare NOT ILIKE '%RADIERE%';

\echo ''
\echo '--- JUDETE (top 15) ---'
SELECT 
  COALESCE(judet, 'NECUNOSCUT') as "Judet",
  COUNT(*) as "Nr firme"
FROM firme
WHERE anaf_sync_status = 'found'
GROUP BY judet
ORDER BY COUNT(*) DESC
LIMIT 15;

\echo ''
\echo '--- FORME JURIDICE (top 10) ---'
SELECT 
  COALESCE(anaf_forma_juridica, forma_juridica, 'NECUNOSCUT') as "Forma juridica",
  COUNT(*) as "Nr firme"
FROM firme
WHERE anaf_sync_status = 'found'
GROUP BY COALESCE(anaf_forma_juridica, forma_juridica)
ORDER BY COUNT(*) DESC
LIMIT 10;

\echo ''
\echo '--- CODURI CAEN (top 10) ---'
SELECT 
  COALESCE(anaf_cod_caen, 'FARA CAEN') as "Cod CAEN",
  COUNT(*) as "Nr firme"
FROM firme
WHERE anaf_sync_status = 'found'
  AND anaf_stare ILIKE '%ACTIV%'
GROUP BY anaf_cod_caen
ORDER BY COUNT(*) DESC
LIMIT 10;
SQL

echo "   Salvat: $STATS_FILE"

# ─── 2. EXPORT 10K FIRME ACTIVE ───────────────────────────────────────────────
echo ""
echo ">>> 2. Export 10.000 firme active (CSV)..."
ACTIVE_FILE="$OUTPUT_DIR/firme_active_10k_${TIMESTAMP}.csv"

run_query_csv "
SELECT 
  f.id,
  f.cui,
  COALESCE(f.anaf_denumire, f.denumire) as denumire,
  f.judet,
  COALESCE(f.anaf_sediu_localitate, f.localitate) as localitate,
  f.anaf_stare as stare,
  f.anaf_cod_caen as cod_caen,
  f.anaf_forma_juridica as forma_juridica,
  f.anaf_nr_reg_com as nr_reg_com,
  f.anaf_platitor_tva as platitor_tva,
  f.anaf_e_factura as e_factura,
  f.anaf_inactiv as inactiv,
  f.mf_an_bilant as an_bilant,
  f.mf_cifra_afaceri as cifra_afaceri,
  f.mf_profit_net as profit_net,
  f.mf_numar_angajati as angajati,
  f.anaf_organ_fiscal as organ_fiscal
FROM firme f
WHERE f.anaf_sync_status = 'found'
  AND f.anaf_stare ILIKE '%ACTIV%'
  AND f.anaf_stare NOT ILIKE '%INACTIV%'
  AND f.anaf_stare NOT ILIKE '%RADIERE%'
ORDER BY f.cui
LIMIT 10000
" > "$ACTIVE_FILE"

LINES=$(wc -l < "$ACTIVE_FILE")
SIZE=$(du -sh "$ACTIVE_FILE" | cut -f1)
echo "   Salvat: $ACTIVE_FILE ($LINES linii, $SIZE)"

# ─── 3. PROBLEME DETECTATE ────────────────────────────────────────────────────
echo ""
echo ">>> 3. Detectare probleme..."
PROBLEMS_FILE="$OUTPUT_DIR/probleme_${TIMESTAMP}.txt"

docker exec "$PG_CONTAINER" psql -U justapp -d justportal << 'SQL' > "$PROBLEMS_FILE" 2>/dev/null
\echo '=== PROBLEME DETECTATE ==='
\echo ''

\echo '--- CUI cu format invalid (non-numeric) ---'
SELECT cui, denumire, anaf_sync_status
FROM firme
WHERE cui !~ '^\d+$' AND cui IS NOT NULL
LIMIT 20;

\echo ''
\echo '--- Firme cu CUI duplicat ---'
SELECT cui, COUNT(*) as duplicates, array_agg(id ORDER BY id) as ids
FROM firme
WHERE cui IS NOT NULL
GROUP BY cui
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 20;

\echo ''
\echo '--- Firme ANAF found dar fara denumire ANAF ---'
SELECT id, cui, denumire, anaf_sync_status
FROM firme
WHERE anaf_sync_status = 'found' AND anaf_denumire IS NULL
LIMIT 10;

\echo ''
\echo '--- Firme cu status ACTIV dar si INACTIV in stare ---'
SELECT id, cui, anaf_denumire, anaf_stare
FROM firme
WHERE anaf_stare ILIKE '%ACTIV%' AND anaf_stare ILIKE '%INACTIV%'
LIMIT 10;

\echo ''
\echo '--- Firme fara judet (sincronizate ANAF) ---'
SELECT COUNT(*) as "Fara judet (sync ANAF)"
FROM firme
WHERE anaf_sync_status = 'found' AND judet IS NULL AND anaf_sediu_judet IS NULL;
SQL

echo "   Salvat: $PROBLEMS_FILE"

# ─── 4. SAMPLE DATE BRUTE ─────────────────────────────────────────────────────
echo ""
echo ">>> 4. Sample 100 firme (JSON pentru analiza)..."
SAMPLE_FILE="$OUTPUT_DIR/sample_100_${TIMESTAMP}.json"

docker exec "$PG_CONTAINER" psql -U justapp -d justportal -t -c "
SELECT json_agg(row_to_json(t)) FROM (
  SELECT 
    id, cui, denumire, anaf_denumire, judet, localitate,
    anaf_stare, anaf_sync_status, anaf_cod_caen, anaf_forma_juridica,
    anaf_platitor_tva, anaf_e_factura, anaf_inactiv,
    mf_cifra_afaceri, mf_profit_net, mf_numar_angajati, mf_an_bilant
  FROM firme
  WHERE anaf_sync_status = 'found'
  ORDER BY RANDOM()
  LIMIT 100
) t;" 2>/dev/null > "$SAMPLE_FILE"

echo "   Salvat: $SAMPLE_FILE"

# ─── 5. SUMAR FINAL ───────────────────────────────────────────────────────────
echo ""
echo "================================================"
echo "  SUMAR FISIERE GENERATE:"
echo "================================================"
ls -lh "$OUTPUT_DIR"/*"${TIMESTAMP}"*
echo ""
echo "  Comprima totul:"
echo "  tar -czf diagnostic_${TIMESTAMP}.tar.gz $OUTPUT_DIR/*${TIMESTAMP}*"
echo ""
echo "  Sau trimite doar CSV-ul cu firme active:"
echo "  $ACTIVE_FILE"
echo "================================================"
