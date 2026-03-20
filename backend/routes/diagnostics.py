"""
Database diagnostics routes: overview, duplicates, cleanup, optimize, indexes.
All operations use MongoDB (motor async driver).
"""
import logging

from fastapi import APIRouter, HTTPException

import mongo_db as mdb
import state

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/diagnostics/overview")
async def get_diagnostics_overview():
    try:
        firme_cnt = await mdb.firme_col.count_documents({})
        dosare_cnt = await mdb.dosare_col.count_documents({})
        timeline_cnt = await mdb.timeline_col.count_documents({})

        # Collection sizes via collStats
        table_sizes_list = []
        for col_name in ["firme", "dosare", "timeline_events", "bilanturi", "bpi_records"]:
            try:
                stats = await mdb.db.command("collStats", col_name)
                size_bytes = stats.get("storageSize", 0) + stats.get("totalIndexSize", 0)
                if size_bytes > 1024 * 1024:
                    size_str = f"{size_bytes / (1024*1024):.1f} MB"
                elif size_bytes > 1024:
                    size_str = f"{size_bytes / 1024:.1f} KB"
                else:
                    size_str = f"{size_bytes} B"
                table_sizes_list.append({"table": col_name, "size": size_str, "bytes": size_bytes})
            except Exception:
                pass
        table_sizes_list.sort(key=lambda x: x["bytes"], reverse=True)

        # Duplicate denumiri
        pipeline_dup_den = [
            {"$group": {"_id": "$denumire_normalized", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$count": "total"}
        ]
        dup_den_result = await mdb.firme_col.aggregate(pipeline_dup_den).to_list(1)
        dup_denumiri = dup_den_result[0]["total"] if dup_den_result else 0

        # Duplicate CUI
        pipeline_dup_cui = [
            {"$match": {"cui": {"$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$cui", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$count": "total"}
        ]
        dup_cui_result = await mdb.firme_col.aggregate(pipeline_dup_cui).to_list(1)
        dup_cui = dup_cui_result[0]["total"] if dup_cui_result else 0

        # Firme without CUI
        no_cui_cnt = await mdb.firme_col.count_documents({
            "$or": [{"cui": None}, {"cui": ""}, {"cui": {"$exists": False}}]
        })

        # Orphaned dosare (firma_id not matching any firma)
        pipeline_orphan = [
            {"$lookup": {
                "from": "firme",
                "localField": "firma_id",
                "foreignField": "id",
                "as": "firma"
            }},
            {"$match": {"firma": {"$size": 0}}},
            {"$count": "total"}
        ]
        orphan_result = await mdb.dosare_col.aggregate(pipeline_orphan).to_list(1)
        orphaned_cnt = orphan_result[0]["total"] if orphan_result else 0

        return {
            "counts": {"firme": firme_cnt, "dosare": dosare_cnt, "timeline_events": timeline_cnt},
            "table_sizes": table_sizes_list,
            "issues": {
                "duplicate_denumiri": dup_denumiri, "duplicate_cui": dup_cui,
                "firme_without_cui": no_cui_cnt, "orphaned_dosare": orphaned_cnt
            }
        }
    except Exception as e:
        logger.error(f"Diagnostics overview error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diagnostics/duplicates/denumire")
async def get_duplicate_denumiri(limit: int = 50):
    try:
        pipeline = [
            {"$group": {
                "_id": "$denumire_normalized",
                "count": {"$sum": 1},
                "ids": {"$push": "$id"},
                "denumiri": {"$push": "$denumire"},
                "cui_list": {"$push": {"$ifNull": ["$cui", ""]}}
            }},
            {"$match": {"count": {"$gt": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]
        results = await mdb.firme_col.aggregate(pipeline).to_list(limit)
        return [{
            "denumire_normalized": r["_id"],
            "count": r["count"],
            "ids": r["ids"][:5],
            "denumiri": r["denumiri"][:5],
            "cui_list": [c for c in r["cui_list"] if c][:3]
        } for r in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diagnostics/duplicates/cui")
async def get_duplicate_cui(limit: int = 50):
    try:
        pipeline = [
            {"$match": {"cui": {"$ne": None, "$ne": ""}}},
            {"$group": {
                "_id": "$cui",
                "count": {"$sum": 1},
                "ids": {"$push": "$id"},
                "denumiri": {"$push": "$denumire"}
            }},
            {"$match": {"count": {"$gt": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]
        results = await mdb.firme_col.aggregate(pipeline).to_list(limit)
        return [{
            "cui": r["_id"],
            "count": r["count"],
            "ids": r["ids"][:5],
            "denumiri": r["denumiri"][:5]
        } for r in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/cleanup/duplicates-denumire")
async def cleanup_duplicate_denumiri():
    try:
        pipeline = [
            {"$group": {
                "_id": "$denumire_normalized",
                "count": {"$sum": 1},
                "docs": {"$push": {"id": "$id", "cui": "$cui"}}
            }},
            {"$match": {"count": {"$gt": 1}}}
        ]
        groups = await mdb.firme_col.aggregate(pipeline).to_list(None)
        ids_to_delete = []
        for group in groups:
            docs = group["docs"]
            docs_with_cui = [d for d in docs if d.get("cui")]
            docs_without_cui = [d for d in docs if not d.get("cui")]
            if docs_with_cui:
                # Keep the first with CUI, delete others without CUI and duplicate CUI entries
                keep = docs_with_cui[0]
                for d in docs:
                    if d["id"] != keep["id"]:
                        ids_to_delete.append(d["id"])
            else:
                # No CUI — keep the first, delete rest
                for d in docs[1:]:
                    ids_to_delete.append(d["id"])

        deleted = 0
        if ids_to_delete:
            result = await mdb.firme_col.delete_many({"id": {"$in": ids_to_delete}})
            deleted = result.deleted_count

        return {"success": True, "deleted_count": deleted, "message": f"Deleted {deleted} duplicate entries"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/cleanup/duplicates-cui")
async def cleanup_duplicate_cui():
    try:
        pipeline = [
            {"$match": {"cui": {"$ne": None, "$ne": ""}}},
            {"$group": {
                "_id": "$cui",
                "count": {"$sum": 1},
                "ids": {"$push": "$id"}
            }},
            {"$match": {"count": {"$gt": 1}}}
        ]
        groups = await mdb.firme_col.aggregate(pipeline).to_list(None)
        ids_to_delete = []
        for group in groups:
            # Keep the first (lowest id), delete the rest
            sorted_ids = sorted(group["ids"])
            ids_to_delete.extend(sorted_ids[1:])

        deleted = 0
        if ids_to_delete:
            result = await mdb.firme_col.delete_many({"id": {"$in": ids_to_delete}})
            deleted = result.deleted_count

        return {"success": True, "deleted_count": deleted, "message": f"Deleted {deleted} duplicate CUI entries"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/cleanup/orphaned-dosare")
async def cleanup_orphaned_dosare():
    try:
        # Get all firma IDs
        firma_ids = set()
        async for doc in mdb.firme_col.find({}, {"_id": 0, "id": 1}):
            firma_ids.add(doc["id"])

        # Find orphaned dosare
        orphaned_ids = []
        async for doc in mdb.dosare_col.find({}, {"_id": 0, "id": 1, "firma_id": 1}):
            if doc.get("firma_id") not in firma_ids:
                orphaned_ids.append(doc["id"])

        deleted = 0
        if orphaned_ids:
            await mdb.timeline_col.delete_many({"dosar_id": {"$in": orphaned_ids}})
            result = await mdb.dosare_col.delete_many({"id": {"$in": orphaned_ids}})
            deleted = result.deleted_count

        return {"success": True, "deleted_count": deleted, "message": f"Deleted {deleted} orphaned dosare"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/optimize")
async def optimize_database():
    """Run compact on MongoDB collections."""
    results = []
    for col_name in ["firme", "dosare", "timeline_events", "bilanturi"]:
        try:
            await mdb.db.command("compact", col_name)
            results.append(col_name)
        except Exception:
            pass
    return {"success": True, "message": f"Compacted collections: {', '.join(results) or 'none'}"}


@router.post("/diagnostics/migrate-schema")
async def migrate_database_schema():
    """No-op for MongoDB — schema is flexible. Ensures indexes exist."""
    try:
        await mdb.create_indexes()
        return {"success": True, "columns_added": [], "message": "MongoDB schema is flexible. Indexes verified."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diagnostics/indexes")
async def get_database_indexes():
    try:
        indexes = []
        for col_name in ["firme", "dosare", "timeline_events", "bilanturi", "bpi_records"]:
            try:
                col = mdb.db[col_name]
                idx_list = await col.index_information()
                for idx_name, idx_info in idx_list.items():
                    keys = ", ".join(f"{k[0]} ({k[1]})" for k in idx_info.get("key", []))
                    unique = " UNIQUE" if idx_info.get("unique") else ""
                    sparse = " SPARSE" if idx_info.get("sparse") else ""
                    indexes.append({
                        "table": col_name,
                        "name": idx_name,
                        "definition": f"{keys}{unique}{sparse}"
                    })
            except Exception:
                pass
        return indexes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/create-indexes")
async def create_performance_indexes():
    try:
        await mdb.create_indexes()
        # Get the updated list
        indexes = await get_database_indexes()
        return {
            "success": True,
            "created_indexes": [idx["name"] for idx in indexes],
            "message": f"Created/verified {len(indexes)} indexes"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
