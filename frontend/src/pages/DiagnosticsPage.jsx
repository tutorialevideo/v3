import axios from "axios";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import { Badge } from "../components/ui/badge";
import { Switch } from "../components/ui/switch";
import { ScrollArea } from "../components/ui/scroll-area";
import { Separator } from "../components/ui/separator";
import { Progress } from "../components/ui/progress";
import { Calendar } from "../components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "../components/ui/popover";
import { toast } from "sonner";
import { format } from "date-fns";
import { ro } from "date-fns/locale";
import {
  Play, Settings, FileJson, Clock, Download, Trash2, Search, RefreshCw,
  CheckCircle2, XCircle, Loader2, Building2, FolderDown, Activity,
  CalendarIcon, Timer, Zap, Upload, FileSpreadsheet, AlertCircle,
  Database, HardDrive, AlertTriangle, Wrench, BarChart3, Shield, ChevronDown
} from "lucide-react";

const API = "/api";

export default function DiagnosticsPage({ ctx }) {
  const {
    // Dashboard/global
    config, stats, dbStats, files, runs, currentRun, loading,
    searchTerm, setSearchTerm, scheduleHour, setScheduleHour,
    scheduleMinute, setScheduleMinute, cronEnabled, setCronEnabled,
    dateStart, setDateStart, dateEnd, setDateEnd,
    searchPreview, setSearchPreview, searchLoading, showAdvancedConfig, setShowAdvancedConfig,
    downloadLogs, downloadProgress,
    importLoading, importResult, importLog, importError,
    setImportLog, setImportResult, setImportError,
    // Firme
    firmeList, firmeTotal, firmePage, setFirmePage, firmeSearch, setFirmeSearch, firmeLoading,
    inlineEditId, setInlineEditId, inlineEditValue, setInlineEditValue,
    // DB Final
    dbFinalStats, dbFinalList, dbFinalTotal, dbFinalPage, setDbFinalPage,
    dbFinalSearch, setDbFinalSearch, dbFinalLoading, dbFinalFilters,
    // ANAF
    anafStats, anafProgress, anafLoading, anafSyncRunning,
    anafTestResult, anafTestCui, setAnafTestCui, anafLogs,
    // MFinante
    mfStats, mfProgress, mfLoading, mfSession, mfTestCui, setMfTestCui, mfTestResult,
    // Diagnostics
    diagnosticsData, duplicateDenumiri, duplicateCui, indexes,
    diagnosticsLoading, cleanupLoading, dbAvailable, reconnecting,
    // Handlers
    saveConfig, triggerRun, stopDownloadJob, searchPreviewDosare,
    downloadFile, deleteFile, handleCsvImport, exportFirme,
    loadFirme, handleFirmeSearch, openFirmaProfile, saveInlineCui,
    loadDbFinalStats, loadDbFinal, handleDbFinalSearch, handleDbFinalFilterChange,
    testAnafCui, testAnafCuiFull, startAnafSync, stopAnafSync,
    loadAnafProgress, loadAnafStats, loadMfStats, loadMfProgress,
    openCaptchaModal, testMfCui,
    loadDiagnostics, cleanupDuplicateDenumiri, cleanupDuplicateCui,
    cleanupOrphanedDosare, optimizeDatabase, migrateSchema, createIndexes,
    reconnectDatabase,
    formatDate, formatBytes,
    localitatiStats, localitatiLoading, normalizeProgress,
    loadLocalitatiStats, importLocalitati, startNormalizare,
    // MFirme crawler
    mfirmeCrawlStatus, mfirmeCrawlLogs, mfirmeCrawling,
    startMfirmeCrawl, stopMfirmeCrawl, clearMfirmeCheckpoint,
  } = ctx;

  return (
          /* Diagnostics Tab */
          <div className="diagnostics-section" data-testid="diagnostics-section">
            {/* Overview Card */}
            <Card className="diagnostics-overview" data-testid="diagnostics-overview">
              <CardHeader>
                <div className="card-header-with-action">
                  <div>
                    <CardTitle className="card-title">
                      <Database size={20} />
                      Diagnosticare Bază de Date
                    </CardTitle>
                    <CardDescription>
                      Verifică starea bazei de date, duplicatele și optimizează performanța
                    </CardDescription>
                  </div>
                  <div className="header-actions">
                    <Button 
                      variant="outline" 
                      onClick={loadDiagnostics}
                      disabled={diagnosticsLoading}
                    >
                      <RefreshCw size={16} className={diagnosticsLoading ? 'animate-spin' : ''} />
                      Reîncarcă
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {diagnosticsLoading && !diagnosticsData ? (
                  <div className="loading-center">
                    <Loader2 className="animate-spin" size={32} />
                    <p>Se încarcă diagnosticele...</p>
                  </div>
                ) : diagnosticsData ? (
                  <div className="diagnostics-grid">
                    {/* Table Counts */}
                    <div className="diag-card">
                      <h4><BarChart3 size={18} /> Statistici Tabele</h4>
                      <div className="diag-stats">
                        <div className="diag-stat">
                          <span className="stat-value">{diagnosticsData.counts.firme?.toLocaleString() || 0}</span>
                          <span className="stat-label">Firme</span>
                        </div>
                        <div className="diag-stat">
                          <span className="stat-value">{diagnosticsData.counts.dosare?.toLocaleString() || 0}</span>
                          <span className="stat-label">Dosare</span>
                        </div>
                        <div className="diag-stat">
                          <span className="stat-value">{diagnosticsData.counts.timeline_events?.toLocaleString() || 0}</span>
                          <span className="stat-label">Evenimente</span>
                        </div>
                      </div>
                    </div>

                    {/* Table Sizes */}
                    <div className="diag-card">
                      <h4><HardDrive size={18} /> Dimensiune pe Disc</h4>
                      <div className="diag-sizes">
                        {diagnosticsData.table_sizes?.map((t, i) => (
                          <div key={i} className="size-row">
                            <span className="table-name">{t.table}</span>
                            <span className="table-size">{t.size}</span>
                          </div>
                        ))}
                      </div>
                    </div>

                    {/* Issues */}
                    <div className="diag-card issues-card">
                      <h4><AlertTriangle size={18} /> Probleme Detectate</h4>
                      <div className="issues-list">
                        <div className={`issue-row ${diagnosticsData.issues.duplicate_denumiri > 0 ? 'has-issue' : 'no-issue'}`}>
                          <span>Denumiri duplicate:</span>
                          <Badge variant={diagnosticsData.issues.duplicate_denumiri > 0 ? "destructive" : "secondary"}>
                            {diagnosticsData.issues.duplicate_denumiri}
                          </Badge>
                        </div>
                        <div className={`issue-row ${diagnosticsData.issues.duplicate_cui > 0 ? 'has-issue' : 'no-issue'}`}>
                          <span>CUI duplicate:</span>
                          <Badge variant={diagnosticsData.issues.duplicate_cui > 0 ? "destructive" : "secondary"}>
                            {diagnosticsData.issues.duplicate_cui}
                          </Badge>
                        </div>
                        <div className={`issue-row ${diagnosticsData.issues.firme_without_cui > 0 ? 'has-warning' : 'no-issue'}`}>
                          <span>Firme fără CUI:</span>
                          <Badge variant={diagnosticsData.issues.firme_without_cui > 0 ? "outline" : "secondary"}>
                            {diagnosticsData.issues.firme_without_cui}
                          </Badge>
                        </div>
                        <div className={`issue-row ${diagnosticsData.issues.orphaned_dosare > 0 ? 'has-issue' : 'no-issue'}`}>
                          <span>Dosare orfane:</span>
                          <Badge variant={diagnosticsData.issues.orphaned_dosare > 0 ? "destructive" : "secondary"}>
                            {diagnosticsData.issues.orphaned_dosare}
                          </Badge>
                        </div>
                      </div>
                    </div>

                    {/* Actions */}
                    <div className="diag-card actions-card">
                      <h4><Wrench size={18} /> Acțiuni de Întreținere</h4>
                      <div className="actions-list">
                        <Button 
                          variant="default" 
                          onClick={migrateSchema}
                          disabled={cleanupLoading}
                          className="action-btn"
                        >
                          <Database size={16} />
                          Migrare Schemă (coloane noi)
                        </Button>
                        <Button 
                          variant="outline" 
                          onClick={createIndexes}
                          disabled={cleanupLoading}
                          className="action-btn"
                        >
                          <Zap size={16} />
                          Creează Indexuri
                        </Button>
                        <Button 
                          variant="outline" 
                          onClick={optimizeDatabase}
                          disabled={cleanupLoading}
                          className="action-btn"
                        >
                          <RefreshCw size={16} />
                          Optimizează DB
                        </Button>
                        <Button 
                          variant="destructive" 
                          onClick={cleanupDuplicateDenumiri}
                          disabled={cleanupLoading || !diagnosticsData?.issues?.duplicate_denumiri}
                          className="action-btn"
                        >
                          <Trash2 size={16} />
                          Șterge Duplicate Denumiri
                        </Button>
                        <Button 
                          variant="destructive" 
                          onClick={cleanupDuplicateCui}
                          disabled={cleanupLoading || !diagnosticsData?.issues?.duplicate_cui}
                          className="action-btn"
                        >
                          <Trash2 size={16} />
                          Șterge Duplicate CUI
                        </Button>
                        <Button 
                          variant="destructive" 
                          onClick={cleanupOrphanedDosare}
                          disabled={cleanupLoading || !diagnosticsData?.issues?.orphaned_dosare}
                          className="action-btn"
                        >
                          <Trash2 size={16} />
                          Șterge Dosare Orfane
                        </Button>
                      </div>
                      {cleanupLoading && (
                        <div className="cleanup-loading">
                          <Loader2 className="animate-spin" size={16} />
                          <span>Se procesează...</span>
                        </div>
                      )}
                    </div>
                  </div>
                ) : (
                  <p>Nu s-au putut încărca diagnosticele.</p>
                )}
              </CardContent>
            </Card>

            {/* Duplicate Denumiri */}
            {duplicateDenumiri.length > 0 && (
              <Card className="duplicates-card" data-testid="duplicate-denumiri-card">
                <CardHeader>
                  <CardTitle className="card-title">
                    <AlertTriangle size={20} className="text-warning" />
                    Denumiri Duplicate ({duplicateDenumiri.length})
                  </CardTitle>
                  <CardDescription>
                    Firme cu aceeași denumire normalizată
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="duplicates-table-container">
                    <table className="duplicates-table">
                      <thead>
                        <tr>
                          <th>Denumire</th>
                          <th>Duplicări</th>
                          <th>IDs</th>
                          <th>CUI-uri</th>
                        </tr>
                      </thead>
                      <tbody>
                        {duplicateDenumiri.map((dup, i) => (
                          <tr key={i}>
                            <td className="denumire-cell">{dup.denumire_normalized}</td>
                            <td><Badge variant="destructive">{dup.count}</Badge></td>
                            <td className="ids-cell">{dup.ids?.slice(0, 5).join(', ')}{dup.ids?.length > 5 ? '...' : ''}</td>
                            <td className="cui-cell">
                              {dup.cui_list?.filter(c => c).slice(0, 3).map((c, j) => (
                                <Badge key={j} variant="outline" className="mr-1">{c}</Badge>
                              ))}
                              {dup.cui_list?.filter(c => c).length > 3 ? '...' : ''}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Duplicate CUI */}
            {duplicateCui.length > 0 && (
              <Card className="duplicates-card" data-testid="duplicate-cui-card">
                <CardHeader>
                  <CardTitle className="card-title">
                    <AlertTriangle size={20} className="text-danger" />
                    CUI Duplicate ({duplicateCui.length})
                  </CardTitle>
                  <CardDescription>
                    Firme cu același CUI (ar trebui să fie unic)
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="duplicates-table-container">
                    <table className="duplicates-table">
                      <thead>
                        <tr>
                          <th>CUI</th>
                          <th>Duplicări</th>
                          <th>IDs</th>
                          <th>Denumiri</th>
                        </tr>
                      </thead>
                      <tbody>
                        {duplicateCui.map((dup, i) => (
                          <tr key={i}>
                            <td><Badge variant="outline">{dup.cui}</Badge></td>
                            <td><Badge variant="destructive">{dup.count}</Badge></td>
                            <td className="ids-cell">{dup.ids?.slice(0, 5).join(', ')}{dup.ids?.length > 5 ? '...' : ''}</td>
                            <td className="denumire-cell">
                              {dup.denumiri?.slice(0, 2).join(', ')}{dup.denumiri?.length > 2 ? '...' : ''}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Indexes */}
            <Card className="indexes-card" data-testid="indexes-card">
              <CardHeader>
                <CardTitle className="card-title">
                  <Zap size={20} />
                  Indexuri ({indexes.length})
                </CardTitle>
                <CardDescription>
                  Indexurile existente în baza de date pentru performanță
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="indexes-table-container">
                  <table className="indexes-table">
                    <thead>
                      <tr>
                        <th>Tabel</th>
                        <th>Index</th>
                        <th>Definiție</th>
                      </tr>
                    </thead>
                    <tbody>
                      {indexes.map((idx, i) => (
                        <tr key={i}>
                          <td><Badge variant="secondary">{idx.table}</Badge></td>
                          <td className="index-name">{idx.name}</td>
                          <td className="index-def"><code>{idx.definition}</code></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>

            {/* Localitati Card */}
            <Card data-testid="localitati-card">
              <CardHeader>
                <CardTitle className="card-title">
                  <HardDrive size={20} />
                  Baza de Date Localități România
                </CardTitle>
                <CardDescription>
                  42 județe + 13.749 localități cu cod SIRUTA și coordonate GPS — normalizează adresele firmelor
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="diag-stats" style={{marginBottom: '14px'}}>
                  <div className="diag-stat">
                    <span className="stat-value">{localitatiStats?.judete_count || 0}</span>
                    <span className="stat-label">Județe</span>
                  </div>
                  <div className="diag-stat">
                    <span className="stat-value">{localitatiStats?.localitati_count?.toLocaleString() || 0}</span>
                    <span className="stat-label">Localități</span>
                  </div>
                  <div className="diag-stat" style={{color: 'var(--primary)'}}>
                    <span className="stat-value">{localitatiStats?.firme_cu_siruta?.toLocaleString() || 0}</span>
                    <span className="stat-label">Firme normalizate</span>
                  </div>
                  <div className="diag-stat">
                    <span className="stat-value">{localitatiStats?.firme_fara_siruta?.toLocaleString() || 0}</span>
                    <span className="stat-label">Firme ne-normalizate</span>
                  </div>
                </div>

                {/* Progress during normalization */}
                {normalizeProgress?.active && (
                  <div style={{marginBottom:'12px', padding:'10px', background:'var(--bg-secondary)', borderRadius:'8px'}}>
                    <div style={{fontSize:'0.82rem', marginBottom:'6px', color:'var(--text-muted)'}}>
                      Normalizare în progres: {normalizeProgress.processed?.toLocaleString()} / {normalizeProgress.total?.toLocaleString()} firme
                      &nbsp;·&nbsp; {normalizeProgress.matched_judet?.toLocaleString()} județe găsite
                      &nbsp;·&nbsp; {normalizeProgress.matched_localitate?.toLocaleString()} localități găsite
                    </div>
                    <Progress value={normalizeProgress.total > 0 ? (normalizeProgress.processed / normalizeProgress.total * 100) : 0} />
                  </div>
                )}

                <div style={{display:'flex', gap:'8px', flexWrap:'wrap'}}>
                  <Button
                    variant="outline"
                    onClick={loadLocalitatiStats}
                    size="sm"
                  >
                    <RefreshCw size={14} />
                    Reîncarcă stats
                  </Button>

                  {!localitatiStats?.available ? (
                    <Button
                      onClick={importLocalitati}
                      disabled={localitatiLoading}
                      data-testid="import-localitati-btn"
                    >
                      {localitatiLoading ? <Loader2 className="animate-spin" size={14} /> : <Download size={14} />}
                      Importă Localități (GitHub)
                    </Button>
                  ) : (
                    <>
                      <Button
                        onClick={startNormalizare}
                        disabled={localitatiLoading || normalizeProgress?.active}
                        data-testid="normalize-adrese-btn"
                      >
                        {localitatiLoading ? <Loader2 className="animate-spin" size={14} /> : <Wrench size={14} />}
                        Normalizează Adrese ({localitatiStats?.firme_fara_siruta?.toLocaleString()} firme)
                      </Button>
                      <Button
                        variant="outline"
                        onClick={importLocalitati}
                        disabled={localitatiLoading}
                        size="sm"
                        data-testid="reimport-localitati-btn"
                      >
                        <Download size={14} />
                        Re-importă
                      </Button>
                    </>
                  )}
                </div>

                {localitatiStats?.available && localitatiStats.firme_cu_siruta > 0 && (
                  <p style={{fontSize:'0.78rem', color:'var(--text-muted)', marginTop:'8px'}}>
                    Firmele cu cod SIRUTA au județ și localitate normalizate conform bazei oficiale de localități.
                  </p>
                )}
              </CardContent>
            </Card>

            {/* MFirme Crawler Card */}
            <Card data-testid="mfirme-crawler-card">
              <CardHeader>
                <CardTitle className="card-title">
                  <Database size={20} />
                  Crawler MFirme — Import CUI-uri
                </CardTitle>
                <CardDescription>
                  Descarcă automat toate CUI-urile de pe mfirme.ro (~1.7M firme), le compară cu DB-ul local și adaugă ce lipsește.
                </CardDescription>
              </CardHeader>
              <CardContent>
                {/* Status card */}
                {mfirmeCrawlStatus && (
                  <div style={{display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(130px,1fr))', gap:'8px', marginBottom:'14px'}}>
                    {[
                      { label: 'Pagina curentă', value: mfirmeCrawlStatus.progress?.current_page?.toLocaleString() },
                      { label: 'Total pagini', value: mfirmeCrawlStatus.progress?.total_pages?.toLocaleString() },
                      { label: '✅ CUI-uri noi', value: mfirmeCrawlStatus.progress?.cuis_new?.toLocaleString(), color: '#22c55e' },
                      { label: 'Skip (existente)', value: mfirmeCrawlStatus.progress?.cuis_skipped?.toLocaleString() },
                    ].map((s, i) => (
                      <div key={i} style={{background:'var(--bg-secondary)', borderRadius:'8px', padding:'10px', textAlign:'center', border:'1px solid var(--border)'}}>
                        <div style={{fontSize:'1.1rem', fontWeight:700, color: s.color || 'var(--text-primary)'}}>{s.value ?? '-'}</div>
                        <div style={{fontSize:'0.72rem', color:'var(--text-muted)', marginTop:'2px'}}>{s.label}</div>
                      </div>
                    ))}
                  </div>
                )}

                {/* Progress bar */}
                {mfirmeCrawlStatus?.progress?.total_pages > 0 && (
                  <div style={{marginBottom:'12px'}}>
                    <div style={{display:'flex', justifyContent:'space-between', fontSize:'0.75rem', color:'var(--text-muted)', marginBottom:'4px'}}>
                      <span>{mfirmeCrawlStatus.progress.current_page?.toLocaleString()} / {mfirmeCrawlStatus.progress.total_pages?.toLocaleString()} pagini</span>
                      <span style={{color:'var(--primary)'}}>
                        {mfirmeCrawlStatus.progress.total_pages > 0
                          ? `${((mfirmeCrawlStatus.progress.current_page / mfirmeCrawlStatus.progress.total_pages) * 100).toFixed(1)}%`
                          : '0%'}
                      </span>
                    </div>
                    <div style={{height:'6px', background:'var(--border)', borderRadius:'3px', overflow:'hidden'}}>
                      <div style={{
                        height:'100%', background:'var(--primary)', transition:'width 0.5s',
                        width: `${mfirmeCrawlStatus.progress.total_pages > 0 ? (mfirmeCrawlStatus.progress.current_page / mfirmeCrawlStatus.progress.total_pages * 100) : 0}%`
                      }} />
                    </div>
                  </div>
                )}

                {/* Checkpoint info */}
                {mfirmeCrawlStatus?.checkpoint?.last_page > 0 && !mfirmeCrawling && (
                  <div style={{padding:'8px 12px', background:'rgba(99,102,241,0.08)', border:'1px solid rgba(99,102,241,0.25)', borderRadius:'8px', marginBottom:'10px', fontSize:'0.82rem', display:'flex', alignItems:'center', justifyContent:'space-between'}}>
                    <span>
                      Checkpoint salvat la pagina <strong>{mfirmeCrawlStatus.checkpoint.last_page?.toLocaleString()}</strong>
                      {' '}— continuă de unde a rămas
                    </span>
                    <Button size="sm" variant="ghost" onClick={clearMfirmeCheckpoint} style={{fontSize:'0.72rem', padding:'2px 8px'}}>
                      Resetează
                    </Button>
                  </div>
                )}

                {/* Controls */}
                <div style={{display:'flex', gap:'8px', marginBottom:'12px', flexWrap:'wrap'}}>
                  <Button
                    onClick={() => startMfirmeCrawl(true, 5)}
                    disabled={mfirmeCrawling}
                    data-testid="start-mfirme-crawl-btn"
                    style={{flex: 1}}
                  >
                    {mfirmeCrawling
                      ? <><Loader2 className="animate-spin" size={14} style={{marginRight:6}} />Crawl în progres...</>
                      : <><Download size={14} style={{marginRight:6}} />{mfirmeCrawlStatus?.checkpoint?.last_page > 0 ? 'Continuă Crawl' : 'Pornește Crawl'}</>
                    }
                  </Button>
                  {mfirmeCrawling && (
                    <Button variant="destructive" onClick={stopMfirmeCrawl} data-testid="stop-mfirme-crawl-btn">
                      <XCircle size={14} style={{marginRight:4}} /> Stop
                    </Button>
                  )}
                  <Button variant="outline" size="sm" onClick={() => { startMfirmeCrawl(false, 5); }} disabled={mfirmeCrawling} title="Start de la pagina 1 (ignoră checkpoint)">
                    De la început
                  </Button>
                </div>

                <p style={{fontSize:'0.75rem', color:'var(--text-muted)', marginBottom:'8px'}}>
                  ~17.000 pagini × 100 firme = ~1.7M firme | 5 pagini concurrent | Checkpoint automat la 500 pag.
                </p>

                {/* Live log */}
                {mfirmeCrawlLogs.length > 0 && (
                  <div className="download-log-panel">
                    <div className="download-log-header">
                      <Activity size={14} />
                      <span>Log crawler MFirme</span>
                      {mfirmeCrawling && <span className="download-log-stats">pagina {mfirmeCrawlStatus?.progress?.current_page?.toLocaleString()}</span>}
                    </div>
                    <ScrollArea className="download-log-scroll">
                      <div className="download-log-content">
                        {mfirmeCrawlLogs.map((line, i) => (
                          <div key={i} className="download-log-line"
                            style={{color: line.includes('❌') ? '#ef4444' : line.includes('✅') ? '#22c55e' : line.includes('⚠️') ? '#eab308' : undefined}}>
                            {line}
                          </div>
                        ))}
                      </div>
                    </ScrollArea>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
  );
}
