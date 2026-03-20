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

export default function AnafPage({ ctx }) {
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
    openCaptchaModal, testMfCui, fetchMfBilant,
    mfBilantYear, mfBilantResult, mfBilantLoading,
    mfOnlyActive, setMfOnlyActive, mfLogs,
    autoSolveCaptcha, autoSolving,
    loadDiagnostics, cleanupDuplicateDenumiri, cleanupDuplicateCui,
    cleanupOrphanedDosare, optimizeDatabase, migrateSchema, createIndexes,
    reconnectDatabase,
    formatDate, formatBytes,
    fixAnafTimestamps, resetAnafSyncStatus,
    // ANAF-specific (missing from base destructuring)
    clearAnafLogs, formatEta,
    // CAPTCHA
    captchaLoading,
    // MFinante-specific
    setMfSession, setMfSessionId, startMfSync, stopMfSync,
    // Sync Dosare per Firma
    syncDosareProgress, syncDosareLogs, syncDosareLoading,
    syncDosareLimit, setSyncDosareLimit,
    syncDosareCategorie, setSyncDosareCategorie,
    syncDosareDateStart, setSyncDosareDateStart,
    syncDosareDateEnd, setSyncDosareDateEnd,
    startSyncDosare, stopSyncDosare,
    categoriiCaz,
  } = ctx;

  return (
          /* ANAF Sync Tab */
          <div className="anaf-section" data-testid="anaf-section">
            {/* ANAF Stats Card */}
            <Card className="anaf-stats-card" data-testid="anaf-stats">
              <CardHeader>
                <div className="card-header-with-action">
                  <div>
                    <CardTitle className="card-title">
                      <Download size={20} />
                      Sincronizare ANAF
                    </CardTitle>
                    <CardDescription>
                      Descarcă date de la ANAF pentru firmele din baza de date (TVA, stare, e-Factura)
                    </CardDescription>
                  </div>
                  <div className="header-actions">
                    <Button variant="outline" onClick={() => { loadAnafStats(); loadAnafProgress(); }}>
                      <RefreshCw size={16} />
                      Reîncarcă
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {anafStats && (
                  <div className="anaf-stats-grid">
                    <div className="anaf-stat-card">
                      <span className="stat-value">{anafStats.total_firme_cu_cui?.toLocaleString() || 0}</span>
                      <span className="stat-label">Firme cu CUI</span>
                    </div>
                    <div className="anaf-stat-card synced">
                      <span className="stat-value">{anafStats.synced?.toLocaleString() || 0}</span>
                      <span className="stat-label">Sincronizate</span>
                    </div>
                    <div className="anaf-stat-card not-synced">
                      <span className="stat-value">{anafStats.not_synced?.toLocaleString() || 0}</span>
                      <span className="stat-label">Nesincronizate</span>
                    </div>
                    <div className="anaf-stat-card found">
                      <span className="stat-value">{anafStats.found?.toLocaleString() || 0}</span>
                      <span className="stat-label">Găsite în ANAF</span>
                    </div>
                    <div className="anaf-stat-card not-found">
                      <span className="stat-value">{anafStats.not_found?.toLocaleString() || 0}</span>
                      <span className="stat-label">Negăsite</span>
                    </div>
                    <div className="anaf-stat-card active">
                      <span className="stat-value">{anafStats.active?.toLocaleString() || 0}</span>
                      <span className="stat-label">Active</span>
                    </div>
                    <div className="anaf-stat-card radiate">
                      <span className="stat-value">{anafStats.radiate?.toLocaleString() || 0}</span>
                      <span className="stat-label">Radiate</span>
                    </div>
                    <div className="anaf-stat-card tva">
                      <span className="stat-value">{anafStats.platitori_tva?.toLocaleString() || 0}</span>
                      <span className="stat-label">Plătitori TVA</span>
                    </div>
                    <div className="anaf-stat-card efactura">
                      <span className="stat-value">{anafStats.e_factura?.toLocaleString() || 0}</span>
                      <span className="stat-label">e-Factura</span>
                    </div>
                  </div>
                )}

                {/* Fix timestamps warning */}
                {anafStats?.fara_timestamp > 0 && (
                  <div className="anaf-fix-banner" data-testid="anaf-fix-banner">
                    <AlertCircle size={16} />
                    <span>
                      <strong>{anafStats.fara_timestamp?.toLocaleString()}</strong> firme sincronizate anterior nu au timestamp
                      (sync vechi). Apasă "Repară Timestamps" pentru a fi recunoscute ca sincronizate.
                    </span>
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={fixAnafTimestamps}
                      data-testid="fix-timestamps-btn"
                    >
                      <Wrench size={14} />
                      Repară Timestamps
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Sync Progress Card */}
            <Card className="anaf-progress-card" data-testid="anaf-progress">
              <CardHeader>
                <CardTitle className="card-title">
                  {anafSyncRunning ? <Loader2 className="animate-spin" size={20} /> : <Zap size={20} />}
                  Progres Sincronizare
                </CardTitle>
              </CardHeader>
              <CardContent>
                {/* Live stats — always visible when data available */}
                {anafStats && (
                  <div style={{
                    display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(130px,1fr))',
                    gap:'8px', marginBottom:'14px'
                  }}>
                    {[
                      { label:'Total cu CUI', value: anafStats.total_firme_cu_cui, color:'var(--text-primary)' },
                      { label:'✅ Sincronizate', value: anafStats.synced, color:'#22c55e' },
                      { label:'⏳ Nesincronizate', value: anafStats.not_synced, color:'#eab308' },
                      { label:'Găsite ANAF', value: anafStats.found, color:'var(--primary)' },
                      { label:'Active', value: anafStats.active, color:'#22c55e' },
                      { label:'Radiate', value: anafStats.radiate, color:'#ef4444' },
                      { label:'Plătitori TVA', value: anafStats.platitori_tva, color:'var(--primary)' },
                      { label:'e-Factură', value: anafStats.e_factura, color:'#8b5cf6' },
                    ].map((s, i) => (
                      <div key={i} style={{
                        background:'var(--bg-secondary)', borderRadius:'8px', padding:'10px 12px',
                        textAlign:'center', border:'1px solid var(--border)'
                      }}>
                        <div style={{fontSize:'1.2rem', fontWeight:700, color: s.color}}>
                          {s.value?.toLocaleString() ?? '-'}
                        </div>
                        <div style={{fontSize:'0.72rem', color:'var(--text-muted)', marginTop:'2px'}}>
                          {s.label}
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                {/* Progress bar — shown during sync AND after last run */}
                {anafProgress && (anafProgress.active || anafProgress.processed > 0) && (
                  <div className="progress-info" style={{marginBottom:'16px'}}>
                    <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:'6px'}}>
                      <span style={{fontSize:'0.8rem', color:'var(--text-muted)'}}>
                        {anafProgress.active ? 'Sincronizare în curs...' : '✅ Ultima rulare finalizată'}
                      </span>
                      {anafProgress.processed > 0 && anafProgress.total_firms > 0 && (
                        <span style={{fontSize:'0.8rem', color:'var(--primary)', fontWeight:600}}>
                          {((anafProgress.processed / anafProgress.total_firms) * 100).toFixed(1)}%
                        </span>
                      )}
                    </div>
                    <div className="progress-bar-container">
                      <div className="progress-bar" style={{
                        width: `${anafProgress.total_firms > 0 ? Math.min((anafProgress.processed / anafProgress.total_firms * 100), 100) : 0}%`
                      }} />
                    </div>
                    <div className="progress-stats">
                      <span>Procesat: <strong>{anafProgress.processed?.toLocaleString()}</strong> / {anafProgress.total_firms?.toLocaleString()}</span>
                      <span>Batch: {anafProgress.current_batch?.toLocaleString()} / {anafProgress.total_batches?.toLocaleString()}</span>
                      <span style={{color:'#22c55e'}}>Găsite: <strong>{anafProgress.found?.toLocaleString()}</strong></span>
                      <span>Negăsite: {anafProgress.not_found?.toLocaleString()}</span>
                      <span style={{color: anafProgress.errors > 0 ? '#ef4444' : undefined}}>Erori: {anafProgress.errors?.toLocaleString()}</span>
                      {anafProgress.active && <span>ETA: {formatEta(anafProgress.eta_seconds)}</span>}
                    </div>
                    {anafProgress.active && (
                      <Button variant="destructive" onClick={stopAnafSync} className="stop-btn" style={{marginTop:'8px'}}>
                        <XCircle size={16} />
                        Oprește Sincronizarea
                      </Button>
                    )}
                  </div>
                )}

                {/* Batch buttons — always visible */}
                {!anafProgress?.active && (
                  <div className="sync-actions">
                    <div className="sync-batch-controls">
                      <h4>Sincronizare în Batch-uri</h4>
                      <div className="batch-buttons">
                        <Button 
                          onClick={() => startAnafSync({ only_unsynced: true, limit: 100 })}
                          disabled={anafLoading || anafSyncRunning}
                          variant="outline"
                          size="sm"
                        >
                          100 firme
                        </Button>
                        <Button 
                          onClick={() => startAnafSync({ only_unsynced: true, limit: 500 })}
                          disabled={anafLoading || anafSyncRunning}
                          variant="outline"
                          size="sm"
                        >
                          500 firme
                        </Button>
                        <Button 
                          onClick={() => startAnafSync({ only_unsynced: true, limit: 1000 })}
                          disabled={anafLoading || anafSyncRunning}
                          variant="outline"
                          size="sm"
                        >
                          1.000 firme
                        </Button>
                        <Button 
                          onClick={() => startAnafSync({ only_unsynced: true, limit: 5000 })}
                          disabled={anafLoading || anafSyncRunning}
                          variant="outline"
                          size="sm"
                        >
                          5.000 firme
                        </Button>
                        <Button
                          onClick={() => startAnafSync({ only_unsynced: true, limit: 1000000 })}
                          disabled={anafLoading || anafSyncRunning}
                          variant="outline"
                          size="sm"
                          style={{fontWeight: 700, borderColor: 'var(--primary)', color: 'var(--primary)'}}
                          data-testid="sync-1m-btn"
                        >
                          1.000.000 firme
                        </Button>
                      </div>
                    </div>
                    <Separator className="my-4" />
                    <div className="sync-options">
                      <Button 
                        onClick={() => startAnafSync({ only_unsynced: true })}
                        disabled={anafLoading || anafSyncRunning}
                        className="sync-btn"
                        data-testid="sync-all-btn"
                      >
                        <Download size={16} />
                        Sync Toate Nesincronizate ({anafStats ? anafStats.not_synced?.toLocaleString() : '...'})
                      </Button>
                      <Button 
                        variant="secondary"
                        onClick={() => startAnafSync({ only_unsynced: false })}
                        disabled={anafLoading || anafSyncRunning}
                      >
                        Re-sync toate firmele
                      </Button>
                      <Button
                        variant="outline"
                        onClick={() => resetAnafSyncStatus()}
                        disabled={anafLoading || anafSyncRunning}
                        style={{borderColor: 'var(--danger, #ef4444)', color: 'var(--danger, #ef4444)'}}
                        data-testid="reset-sync-btn"
                      >
                        Reset Status Sync
                      </Button>
                    </div>
                    <p className="sync-note">
                      ⚠️ ANAF permite max 100 CUI/request și 1 request/secundă. 
                      Pentru 1 milion de firme va dura ~3 ore.
                    </p>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Logs Card */}
            <Card className="anaf-logs-card" data-testid="anaf-logs">
              <CardHeader>
                <div className="card-header-with-action">
                  <CardTitle className="card-title">
                    <Activity size={20} />
                    Loguri Sincronizare
                  </CardTitle>
                  <Button variant="ghost" size="sm" onClick={clearAnafLogs}>
                    <Trash2 size={14} />
                    Șterge
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                <div className="logs-container">
                  {anafLogs.length === 0 ? (
                    <p className="no-logs">Niciun log încă. Pornește o sincronizare pentru a vedea logurile.</p>
                  ) : (
                    <pre className="logs-content">
                      {anafLogs.map((log, i) => (
                        <div key={i} className={`log-line ${log.includes('✗') ? 'error' : log.includes('✓') ? 'success' : ''}`}>
                          {log}
                        </div>
                      ))}
                    </pre>
                  )}
                </div>
              </CardContent>
            </Card>

            {/* Test ANAF API Card */}
            <Card className="anaf-test-card" data-testid="anaf-test">
              <CardHeader>
                <CardTitle className="card-title">
                  <Search size={20} />
                  Test API ANAF
                </CardTitle>
                <CardDescription>
                  Testează API-ul ANAF cu un singur CUI
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="test-form">
                  <input
                    type="text"
                    placeholder="Introdu CUI (ex: 14918042)"
                    value={anafTestCui}
                    onChange={(e) => setAnafTestCui(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && testAnafCuiFull()}
                    className="test-input"
                    data-testid="anaf-test-cui-input"
                  />
                  <Button onClick={testAnafCui} disabled={anafLoading} variant="outline" data-testid="anaf-verifica-rapid-btn">
                    {anafLoading ? <Loader2 className="animate-spin" size={16} /> : <Search size={16} />}
                    Verifică rapid
                  </Button>
                  <Button onClick={testAnafCuiFull} disabled={anafLoading} data-testid="anaf-date-complete-btn">
                    {anafLoading ? <Loader2 className="animate-spin" size={16} /> : <BarChart3 size={16} />}
                    Date complete
                  </Button>
                </div>
                {anafTestResult && (
                  <div className="test-result">
                    {anafTestResult.found !== undefined && (
                      <div style={{marginBottom:'8px',display:'flex',gap:'8px',alignItems:'center',flexWrap:'wrap'}}>
                        <Badge variant={anafTestResult.found ? 'success' : 'secondary'}>
                          {anafTestResult.found ? 'Găsit' : 'Negăsit'}
                        </Badge>
                        {anafTestResult.sections_analysis && Object.entries(anafTestResult.sections_analysis).map(([section, data]) => (
                          data.non_empty_fields > 0 && (
                            <Badge key={section} variant="outline" style={{fontSize:'0.7rem'}}>
                              {section}: {data.non_empty_fields}/{data.total_fields}
                            </Badge>
                          )
                        ))}
                      </div>
                    )}
                    {anafTestResult.raw_response?.found?.[0]?.date_generale && (
                      <div className="anaf-quick-summary">
                        {(() => {
                          const dg = anafTestResult.raw_response.found[0].date_generale;
                          return (
                            <div className="profile-grid" style={{marginBottom:'8px'}}>
                              <div className="profile-field"><label>Denumire:</label><span className="value">{dg.denumire}</span></div>
                              <div className="profile-field"><label>Stare:</label><span className="value">{dg.stare_inregistrare}</span></div>
                              <div className="profile-field"><label>Nr. Reg. Com.:</label><span className="value">{dg.nrRegCom}</span></div>
                              <div className="profile-field"><label>Cod CAEN:</label><span className="value">{dg.cod_CAEN}</span></div>
                              <div className="profile-field"><label>Adresă:</label><span className="value">{dg.adresa}</span></div>
                              <div className="profile-field"><label>Telefon:</label><span className="value">{dg.telefon || '-'}</span></div>
                            </div>
                          );
                        })()}
                      </div>
                    )}
                    <details>
                      <summary style={{cursor:'pointer',fontSize:'0.8rem',color:'var(--text-muted)',marginBottom:'4px'}}>JSON Raw complet</summary>
                      <ScrollArea style={{height:'220px',border:'1px solid var(--border)',borderRadius:'4px',padding:'8px'}}>
                        <pre style={{fontSize:'0.72rem',lineHeight:'1.4',whiteSpace:'pre-wrap',wordBreak:'break-all'}}>{JSON.stringify(anafTestResult, null, 2)}</pre>
                      </ScrollArea>
                    </details>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* MFinante Section */}
            <Card className="mfinante-card" data-testid="mfinante-section">
              <CardHeader>
                <div className="card-header-with-action">
                  <div>
                    <CardTitle className="card-title">
                      <FileSpreadsheet size={20} />
                      MFinante - Bilanțuri
                    </CardTitle>
                    <CardDescription>
                      Date financiare de pe mfinante.gov.ro (cifră afaceri, profit, nr. angajați)
                    </CardDescription>
                  </div>
                  <Button variant="outline" onClick={() => { loadMfStats(); loadMfProgress(); }}>
                    <RefreshCw size={16} />
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {/* Session Setup */}
                <div className="mf-session-section">
                  <h4>1. Rezolvă CAPTCHA și setează sesiunea</h4>

                  {/* Auto-Solve button — primary CTA */}
                  <div style={{marginBottom:'12px'}}>
                    <Button
                      onClick={autoSolveCaptcha}
                      disabled={autoSolving}
                      data-testid="auto-solve-captcha-btn"
                      style={{width:'100%', height:'44px', fontSize:'0.95rem', fontWeight:600}}
                    >
                      {autoSolving
                        ? <><Loader2 className="animate-spin" size={16} style={{marginRight:8}} />Se rezolvă automat... (AI citește CAPTCHA)</>
                        : <><Shield size={16} style={{marginRight:8}} />Auto-Solve CAPTCHA (AI)</>
                      }
                    </Button>
                    <p style={{fontSize:'0.78rem', color:'var(--text-muted)', marginTop:'4px', textAlign:'center'}}>
                      GPT-4o citește imaginea CAPTCHA și o trimite automat — până la 5 încercări
                    </p>
                  </div>

                  {/* Manual CAPTCHA Button */}
                  <div className="captcha-button-section">
                    <Button
                      onClick={openCaptchaModal}
                      className="captcha-main-btn"
                      disabled={captchaLoading}
                      variant="outline"
                    >
                      {captchaLoading ? (
                        <Loader2 className="animate-spin" size={16} />
                      ) : (
                        <Shield size={16} />
                      )}
                      Rezolvă CAPTCHA manual
                    </Button>
                    <span className="captcha-hint">
                      dacă auto-solve nu funcționează
                    </span>
                  </div>

                  {/* Manual method (collapsed by default) */}
                  <details className="manual-session-method">
                    <summary>Metodă alternativă (manuală)</summary>
                    <ol className="mf-instructions">
                      <li>Deschide <a href="https://mfinante.gov.ro/apps/infocodfiscal.html" target="_blank" rel="noopener noreferrer">mfinante.gov.ro/apps/infocodfiscal.html</a></li>
                      <li>Rezolvă CAPTCHA și trimite cu orice CUI (ex: 14918042)</li>
                      <li>Apasă F12 → Network → Găsește request-ul → Copiază <code>jsessionid</code> din URL</li>
                      <li>Lipește mai jos și apasă "Setează Sesiune"</li>
                    </ol>
                    <div className="mf-session-form">
                      <input
                        type="text"
                        placeholder="jsessionid (ex: Gtjp5kQJsBZ7Kd4ZU07zCai6IAzJ...)"
                        value={mfSession}
                        onChange={(e) => setMfSession(e.target.value)}
                        className="test-input"
                      />
                      <Button onClick={setMfSessionId} disabled={mfLoading}>
                        {mfLoading ? <Loader2 className="animate-spin" size={16} /> : <CheckCircle2 size={16} />}
                        Setează Sesiune
                      </Button>
                    </div>
                  </details>

                  {mfProgress && (
                    <div className={`session-status ${mfProgress.session_valid ? 'valid' : 'invalid'}`}>
                      {mfProgress.session_valid ? (
                        <><CheckCircle2 size={16} /> Sesiune validă</>
                      ) : (
                        <><XCircle size={16} /> Sesiune invalidă sau expirată</>
                      )}
                    </div>
                  )}
                </div>

                {/* Test CUI */}
                <div className="mf-test-section">
                  <h4>2. Testează cu un CUI</h4>
                  <div className="test-form">
                    <input
                      type="text"
                      placeholder="CUI (ex: 37044065)"
                      value={mfTestCui}
                      onChange={(e) => setMfTestCui(e.target.value)}
                      onKeyDown={(e) => e.key === 'Enter' && testMfCui()}
                      className="test-input"
                      data-testid="mf-test-cui-input"
                    />
                    <Button onClick={testMfCui} disabled={mfLoading || !mfProgress?.session_valid} data-testid="mf-test-btn">
                      {mfLoading ? <Loader2 className="animate-spin" size={16} /> : <Search size={16} />}
                      Test
                    </Button>
                  </div>

                  {mfTestResult && !mfTestResult.error && (
                    <div className="mf-test-result">
                      {/* Date identificare */}
                      {mfTestResult.found && mfTestResult.date_identificare && (
                        <div className="mf-id-section">
                          <div className="mf-id-header">
                            <CheckCircle2 size={16} style={{color:'#22c55e'}} />
                            <strong>{mfTestResult.date_identificare.denumire}</strong>
                          </div>
                          <div className="mf-id-grid">
                            <div className="mf-id-row"><span>Județ:</span><span>{mfTestResult.date_identificare.judet}</span></div>
                            <div className="mf-id-row"><span>Adresă:</span><span>{mfTestResult.date_identificare.adresa}</span></div>
                            <div className="mf-id-row"><span>Nr. Reg. Com.:</span><span>{mfTestResult.date_identificare.nr_reg_com}</span></div>
                            <div className="mf-id-row"><span>Stare:</span><span>{mfTestResult.date_identificare.stare}</span></div>
                            <div className="mf-id-row"><span>Telefon:</span><span>{mfTestResult.date_identificare.telefon || '-'}</span></div>
                            <div className="mf-id-row"><span>Cod Postal:</span><span>{mfTestResult.date_identificare.cod_postal || '-'}</span></div>
                          </div>
                          {mfTestResult.date_fiscale && (
                            <div className="mf-fiscal-row">
                              <Badge variant={mfTestResult.date_fiscale.platitor_tva ? 'success' : 'secondary'}>
                                {mfTestResult.date_fiscale.platitor_tva ? 'Plătitor TVA' : 'Ne-plătitor TVA'}
                              </Badge>
                              {mfTestResult.date_fiscale.tva_data && (
                                <span className="mf-fiscal-detail">TVA din: {mfTestResult.date_fiscale.tva_data}</span>
                              )}
                              {mfTestResult.date_fiscale.micro_data && (
                                <span className="mf-fiscal-detail">Micro din: {mfTestResult.date_fiscale.micro_data}</span>
                              )}
                            </div>
                          )}
                        </div>
                      )}

                      {!mfTestResult.found && (
                        <div style={{color:'var(--text-muted)',fontStyle:'italic',padding:'8px'}}>
                          Firma nu a fost găsită în MFinante.
                        </div>
                      )}

                      {/* Bilanturi disponibile */}
                      {mfTestResult.found && mfTestResult.bilanturi_disponibili?.length > 0 && (
                        <div className="mf-bilanturi-section">
                          <h5>Bilanțuri disponibile — click pentru a încărca datele:</h5>
                          <div className="mf-year-buttons">
                            {mfTestResult.bilanturi_disponibili.map((b) => (
                              <button
                                key={b.value}
                                className={`mf-year-btn ${mfBilantYear === b.an ? 'active' : ''}`}
                                onClick={() => fetchMfBilant(b.value, b.an)}
                                disabled={mfBilantLoading}
                                data-testid={`mf-year-btn-${b.an}`}
                              >
                                {mfBilantLoading && mfBilantYear === b.an
                                  ? <Loader2 className="animate-spin" size={12} />
                                  : b.an
                                }
                              </button>
                            ))}
                          </div>

                          {/* Bilant result */}
                          {mfBilantResult && !mfBilantResult.error && (
                            <div className="mf-bilant-result" data-testid="mf-bilant-result">
                              <h5>Bilanț {mfBilantYear} — Indicatori financiari:</h5>
                              {Object.keys(mfBilantResult.indicatori || {}).length === 0 ? (
                                <p style={{color:'var(--text-muted)',fontStyle:'italic',fontSize:'0.85rem'}}>
                                  Nu s-au putut extrage indicatori pentru acest an.
                                  Structura paginii MFinante poate fi diferită.
                                </p>
                              ) : (
                                <table className="mf-bilant-table">
                                  <thead>
                                    <tr><th>Indicator</th><th>Valoare (RON)</th></tr>
                                  </thead>
                                  <tbody>
                                    {Object.entries(mfBilantResult.indicatori).map(([key, val]) => (
                                      <tr key={key}>
                                        <td>{key.replace(/_/g, ' ')}</td>
                                        <td className="text-right">{val != null ? Number(val).toLocaleString('ro-RO') : '-'}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              )}
                            </div>
                          )}
                          {mfBilantResult?.error && (
                            <div className="mf-bilant-error">Eroare: {mfBilantResult.error}</div>
                          )}
                        </div>
                      )}
                    </div>
                  )}
                  {mfTestResult?.error && (
                    <div className="mf-bilant-error">{mfTestResult.error}</div>
                  )}
                </div>

                {/* Sync Actions */}
                <div className="mf-sync-section">
                  <h4>3. Sincronizează bilanțuri</h4>

                  {/* Stats — active firms eligible */}
                  {mfStats && (
                    <div style={{display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(140px,1fr))', gap:'8px', marginBottom:'12px'}}>
                      <div style={{background:'rgba(34,197,94,0.1)', border:'1px solid rgba(34,197,94,0.3)', borderRadius:'8px', padding:'10px', textAlign:'center'}}>
                        <div style={{fontSize:'1.2rem', fontWeight:700, color:'#22c55e'}}>{mfStats.active_anaf_eligible?.toLocaleString() ?? '-'}</div>
                        <div style={{fontSize:'0.72rem', color:'var(--text-muted)'}}>Firme ACTIVE ANAF</div>
                      </div>
                      <div style={{background:'rgba(234,179,8,0.1)', border:'1px solid rgba(234,179,8,0.3)', borderRadius:'8px', padding:'10px', textAlign:'center'}}>
                        <div style={{fontSize:'1.2rem', fontWeight:700, color:'#eab308'}}>{mfStats.active_fara_bilant?.toLocaleString() ?? '-'}</div>
                        <div style={{fontSize:'0.72rem', color:'var(--text-muted)'}}>Active fără bilanț</div>
                      </div>
                      <div style={{background:'var(--bg-secondary)', border:'1px solid var(--border)', borderRadius:'8px', padding:'10px', textAlign:'center'}}>
                        <div style={{fontSize:'1.2rem', fontWeight:700, color:'var(--primary)'}}>{mfStats.with_cifra_afaceri?.toLocaleString() ?? '-'}</div>
                        <div style={{fontSize:'0.72rem', color:'var(--text-muted)'}}>Cu cifra afaceri</div>
                      </div>
                      <div style={{background:'var(--bg-secondary)', border:'1px solid var(--border)', borderRadius:'8px', padding:'10px', textAlign:'center'}}>
                        <div style={{fontSize:'1.2rem', fontWeight:700}}>{mfStats.total_bilanturi_istorice?.toLocaleString() ?? '-'}</div>
                        <div style={{fontSize:'0.72rem', color:'var(--text-muted)'}}>Bilanțuri istorice</div>
                      </div>
                    </div>
                  )}

                  {/* Info box — always active-only */}
                  <div style={{display:'flex', alignItems:'center', gap:'8px', padding:'8px 12px', background:'rgba(34,197,94,0.08)', border:'1px solid rgba(34,197,94,0.25)', borderRadius:'8px', marginBottom:'10px', fontSize:'0.82rem'}}>
                    <CheckCircle2 size={15} style={{color:'#22c55e', flexShrink:0}} />
                    <span>Se procesează <strong>doar firmele confirmate ACTIVE</strong> de ANAF — firmele radiate/inactive sunt excluse automat.</span>
                  </div>

                  <div className="sync-options">
                    <Button
                      onClick={() => startMfSync(50)}
                      disabled={mfLoading || !mfProgress?.session_valid}
                      data-testid="mf-sync-50-btn"
                    >
                      {mfLoading ? <Loader2 className="animate-spin" size={14} style={{marginRight:4}} /> : null}
                      Test 50 firme
                    </Button>
                    <Button
                      variant="outline"
                      onClick={() => startMfSync(500)}
                      disabled={mfLoading || !mfProgress?.session_valid}
                      data-testid="mf-sync-500-btn"
                    >
                      Sync 500 firme
                    </Button>
                    <Button
                      variant="outline"
                      onClick={() => startMfSync(1000)}
                      disabled={mfLoading || !mfProgress?.session_valid}
                    >
                      Sync 1.000 firme
                    </Button>
                    <Button
                      variant="destructive"
                      onClick={stopMfSync}
                      disabled={!mfProgress?.progress?.active}
                      data-testid="mf-sync-stop-btn"
                    >
                      <XCircle size={16} />
                      Stop
                    </Button>
                  </div>

                  {/* Progress bar */}
                  {mfProgress?.progress?.active && (
                    <div className="mf-progress">
                      <div className="progress-bar-container">
                        <div
                          className="progress-bar"
                          style={{ width: `${mfProgress.progress.total_firms > 0 ? (mfProgress.progress.processed / mfProgress.progress.total_firms * 100) : 0}%` }}
                        />
                      </div>
                      <div className="progress-stats">
                        <span>Procesat: {mfProgress.progress.processed} / {mfProgress.progress.total_firms}</span>
                        <span style={{color:'#22c55e'}}>Găsite: {mfProgress.progress.found}</span>
                        <span style={{color:'var(--text-muted)'}}>Negăsite: {mfProgress.progress.not_found || 0}</span>
                        <span style={{color:'#ef4444'}}>Erori: {mfProgress.progress.errors}</span>
                      </div>
                    </div>
                  )}

                  {/* Live log panel */}
                  {(mfLogs.length > 0 || mfProgress?.progress?.active) && (
                    <div className="download-log-panel" style={{marginTop:'10px'}} data-testid="mf-sync-log-panel">
                      <div className="download-log-header">
                        <Activity size={14} />
                        <span>Log sincronizare MFinante</span>
                        {mfProgress?.progress?.active && (
                          <span className="download-log-stats">
                            {mfProgress.progress.processed}/{mfProgress.progress.total_firms} firme
                          </span>
                        )}
                      </div>
                      <ScrollArea className="download-log-scroll">
                        <div className="download-log-content">
                          {mfLogs.length === 0
                            ? <span className="download-log-empty">Se inițializează...</span>
                            : mfLogs.map((line, i) => (
                                <div key={i} className={`download-log-line ${line.includes('!!') || line.includes('Eroare') ? 'error' : line.includes('->') ? 'success' : ''}`}
                                     style={{color: line.includes('negasit') ? 'var(--text-muted)' : line.includes('->') ? '#22c55e' : line.includes('!!') || line.includes('Eroare') ? '#ef4444' : undefined}}>
                                  {line}
                                </div>
                              ))
                          }
                        </div>
                      </ScrollArea>
                    </div>
                  )}

                  <p className="sync-note">
                    MFinante este lent (~2 sec/firmă). Sesiunea expiră după ~15-30 min inactivitate.
                  </p>
                </div>
              </CardContent>
            </Card>

            {/* Sync Dosare per Firma Card */}
            <Card data-testid="sync-dosare-card">
              <CardHeader>
                <CardTitle className="card-title">
                  <FileJson size={20} />
                  Sync Dosare Portal JUST — per Firmă
                </CardTitle>
                <CardDescription>
                  Caută în Portal JUST dosarele fiecărei firme active ANAF, folosind <strong>denumirea din ANAF</strong> ca termen de căutare
                </CardDescription>
              </CardHeader>
              <CardContent>
                {/* How it works */}
                <div style={{padding:'10px 14px', background:'var(--bg-secondary)', borderRadius:'8px', marginBottom:'14px', fontSize:'0.83rem', lineHeight:'1.6'}}>
                  <strong>Flux:</strong> Firme cu CUI → Sync ANAF (active) → <strong>Căutare Portal JUST cu anaf_denumire</strong> → Dosare salvate direct la firma corectă
                </div>

                {/* Controls */}
                <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:'10px', marginBottom:'12px'}}>
                  <div>
                    <label style={{fontSize:'0.8rem', color:'var(--text-muted)', display:'block', marginBottom:'4px'}}>Categorie dosar</label>
                    <select
                      value={syncDosareCategorie}
                      onChange={(e) => setSyncDosareCategorie(e.target.value)}
                      style={{width:'100%', padding:'7px 10px', borderRadius:'6px', border:'1px solid var(--border)', background:'var(--bg-elevated)', color:'var(--text-primary)', fontSize:'0.84rem'}}
                      data-testid="sync-dosare-categorie"
                    >
                      <option value="">Toate categoriile</option>
                      {(categoriiCaz || []).map(cat => (
                        <option key={cat.value} value={cat.value}>{cat.label}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label style={{fontSize:'0.8rem', color:'var(--text-muted)', display:'block', marginBottom:'4px'}}>Nr. firme de procesat</label>
                    <input
                      type="number"
                      value={syncDosareLimit}
                      onChange={(e) => setSyncDosareLimit(parseInt(e.target.value) || 100)}
                      min="1" max="10000"
                      style={{width:'100%', padding:'7px 10px', borderRadius:'6px', border:'1px solid var(--border)', background:'var(--bg-elevated)', color:'var(--text-primary)', fontSize:'0.84rem'}}
                      data-testid="sync-dosare-limit"
                    />
                  </div>
                  <div>
                    <label style={{fontSize:'0.8rem', color:'var(--text-muted)', display:'block', marginBottom:'4px'}}>Data început dosar (opțional)</label>
                    <input
                      type="date"
                      value={syncDosareDateStart}
                      onChange={(e) => setSyncDosareDateStart(e.target.value)}
                      style={{width:'100%', padding:'7px 10px', borderRadius:'6px', border:'1px solid var(--border)', background:'var(--bg-elevated)', color:'var(--text-primary)', fontSize:'0.84rem'}}
                    />
                  </div>
                  <div>
                    <label style={{fontSize:'0.8rem', color:'var(--text-muted)', display:'block', marginBottom:'4px'}}>Data sfârșit dosar (opțional)</label>
                    <input
                      type="date"
                      value={syncDosareDateEnd}
                      onChange={(e) => setSyncDosareDateEnd(e.target.value)}
                      style={{width:'100%', padding:'7px 10px', borderRadius:'6px', border:'1px solid var(--border)', background:'var(--bg-elevated)', color:'var(--text-primary)', fontSize:'0.84rem'}}
                    />
                  </div>
                </div>

                <div style={{display:'flex', gap:'8px', marginBottom:'12px'}}>
                  <Button
                    onClick={startSyncDosare}
                    disabled={syncDosareLoading}
                    data-testid="start-sync-dosare-btn"
                  >
                    {syncDosareLoading
                      ? <><Loader2 className="animate-spin" size={14} style={{marginRight:6}} />Procesare...</>
                      : <><Play size={14} style={{marginRight:6}} />Pornește Sync Dosare</>
                    }
                  </Button>
                  {syncDosareLoading && (
                    <Button variant="destructive" onClick={stopSyncDosare} size="sm">
                      <XCircle size={14} style={{marginRight:4}} /> Stop
                    </Button>
                  )}
                </div>

                {/* Progress */}
                {syncDosareProgress && (
                  <div style={{display:'flex', gap:'16px', flexWrap:'wrap', fontSize:'0.8rem', marginBottom:'8px', color:'var(--text-muted)'}}>
                    <span>Procesate: <strong style={{color:'var(--text-primary)'}}>{syncDosareProgress.processed?.toLocaleString()}/{syncDosareProgress.total_firms?.toLocaleString()}</strong></span>
                    <span>Cu dosare: <strong style={{color:'#22c55e'}}>{syncDosareProgress.firms_with_dosare?.toLocaleString()}</strong></span>
                    <span>Dosare noi: <strong style={{color:'var(--primary)'}}>{syncDosareProgress.dosare_new?.toLocaleString()}</strong></span>
                    <span>Erori: <strong style={{color:'#ef4444'}}>{syncDosareProgress.errors}</strong></span>
                  </div>
                )}

                {/* Live log */}
                {syncDosareLogs.length > 0 && (
                  <div className="download-log-panel" data-testid="sync-dosare-log">
                    <div className="download-log-header">
                      <Activity size={14} />
                      <span>Log sync dosare</span>
                      {syncDosareProgress?.active && (
                        <span className="download-log-stats">
                          {syncDosareProgress.processed}/{syncDosareProgress.total_firms} firme
                        </span>
                      )}
                    </div>
                    <ScrollArea className="download-log-scroll">
                      <div className="download-log-content">
                        {syncDosareLogs.map((line, i) => (
                          <div key={i} className="download-log-line"
                               style={{color: line.includes('niciun dosar') ? 'var(--text-muted)' : line.includes('dosare noi') ? '#22c55e' : line.includes('Eroare') ? '#ef4444' : undefined}}>
                            {line}
                          </div>
                        ))}
                      </div>
                    </ScrollArea>
                  </div>
                )}

                <p className="sync-note" style={{marginTop:'8px'}}>
                  Caută simultan în toate cele 246 de instituții. ~0.3s/firmă × {syncDosareLimit} firme = ~{Math.round(syncDosareLimit * 246 * 0.3 / 60)} minute estimat.
                </p>
              </CardContent>
            </Card>
          </div>
  );
}
