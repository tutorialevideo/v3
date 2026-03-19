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

export default function DashboardPage({ ctx }) {
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
    reconnectDatabase, fetchData,
    formatDate, formatBytes,
  } = ctx;

  return (
          <>
        {/* Import CUI Section */}
        <Card className="import-card" data-testid="import-section">
          <CardHeader>
            <div className="card-header-with-action">
              <div>
                <CardTitle className="card-title">
                  <FileSpreadsheet size={20} />
                  Import CUI pentru Firme
                </CardTitle>
                <CardDescription>
                  Încarcă un fișier cu coloanele: DENUMIRE, CUI (orice format)
                </CardDescription>
              </div>
              <div className="import-stats">
                <Badge variant="outline" className="badge-info">
                  {dbStats?.firme_with_cui || 0} / {dbStats?.firme_total || 0} cu CUI
                </Badge>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="import-content">
              <div className="import-actions">
                <label className="import-btn">
                  <input
                    type="file"
                    accept="*/*"
                    multiple
                    onChange={handleCsvImport}
                    disabled={importLoading}
                    style={{ display: 'none' }}
                    data-testid="csv-file-input"
                  />
                  <Button variant="default" disabled={importLoading} asChild>
                    <span>
                      {importLoading ? (
                        <Loader2 className="animate-spin" size={16} />
                      ) : (
                        <Upload size={16} />
                      )}
                      {importLoading ? 'Se importă...' : 'Import CSV (multiple)'}
                    </span>
                  </Button>
                </label>
                <Button variant="outline" onClick={exportFirme} data-testid="export-firme-btn">
                  <Download size={16} />
                  Export Firme
                </Button>
                <Button variant="ghost" onClick={() => { setImportLog([]); setImportResult(null); setImportError(null); }}>
                  Șterge log
                </Button>
              </div>

              {/* Import Log Box */}
              {(importLog.length > 0 || importError) && (
                <div className="import-log-box" data-testid="import-log">
                  <div className="import-log-header">
                    <span>Log Import</span>
                    {importLoading && <Loader2 className="animate-spin" size={14} />}
                  </div>
                  <ScrollArea className="import-log-scroll">
                    {importLog.map((log, idx) => (
                      <div key={idx} className={`log-line ${log.includes('EROARE') ? 'error' : log.includes('✓') ? 'success' : ''}`}>
                        {log}
                      </div>
                    ))}
                  </ScrollArea>
                  {importError && (
                    <div className="import-error-box">
                      <AlertCircle size={16} />
                      <span>{importError}</span>
                    </div>
                  )}
                </div>
              )}
              
              {importResult && (
                <div className="import-result" data-testid="import-result">
                  <div className="import-stats-grid">
                    <div className="import-stat">
                      <span className="import-stat-value">{importResult.total_rows?.toLocaleString()}</span>
                      <span className="import-stat-label">Rânduri totale</span>
                    </div>
                    <div className="import-stat">
                      <span className="import-stat-value">{importResult.processed?.toLocaleString() || 0}</span>
                      <span className="import-stat-label">Firme procesate</span>
                    </div>
                    <div className="import-stat success">
                      <span className="import-stat-value">{(importResult.created_new || 0).toLocaleString()}</span>
                      <span className="import-stat-label">Firme noi create</span>
                    </div>
                    <div className="import-stat warning">
                      <span className="import-stat-value">{(importResult.skipped_not_company || 0).toLocaleString()}</span>
                      <span className="import-stat-label">PFA/II sărite</span>
                    </div>
                  </div>
                  
                  {(importResult.already_exists > 0 || importResult.updated_cui > 0) && (
                    <p className="import-info">
                      Existau deja: <strong>{importResult.already_exists?.toLocaleString() || 0}</strong> | 
                      CUI actualizat: <strong>{importResult.updated_cui?.toLocaleString() || 0}</strong> |
                      Fără CUI valid: <strong>{importResult.skipped_no_cui?.toLocaleString() || 0}</strong>
                    </p>
                  )}
                  
                  {importResult.sample_created?.length > 0 && (
                    <div className="import-not-found">
                      <p className="not-found-title" style={{color: 'var(--success)'}}>
                        <CheckCircle2 size={14} />
                        Exemple firme create:
                      </p>
                      <ScrollArea className="not-found-scroll">
                        {importResult.sample_created.map((firma, idx) => (
                          <span key={idx} className="not-found-item" style={{background: 'var(--success-bg)'}}>
                            {firma.denumire} ({firma.cui})
                          </span>
                        ))}
                      </ScrollArea>
                    </div>
                  )}
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Stats Cards */}
        <div className="stats-grid" data-testid="stats-section">
          <Card className="stat-card">
            <CardContent className="stat-content">
              <div className="stat-icon green">
                <CheckCircle2 size={24} />
              </div>
              <div className="stat-info">
                <span className="stat-value">{stats?.completed_runs || 0}</span>
                <span className="stat-label">Rulări complete</span>
              </div>
            </CardContent>
          </Card>
          
          <Card className="stat-card">
            <CardContent className="stat-content">
              <div className="stat-icon purple">
                <Building2 size={24} />
              </div>
              <div className="stat-info">
                <span className="stat-value">{stats?.db_firme || 0}</span>
                <span className="stat-label">Firme în DB</span>
              </div>
            </CardContent>
          </Card>
          
          <Card className="stat-card">
            <CardContent className="stat-content">
              <div className="stat-icon blue">
                <FileJson size={24} />
              </div>
              <div className="stat-info">
                <span className="stat-value">{stats?.db_dosare || 0}</span>
                <span className="stat-label">Dosare în DB</span>
              </div>
            </CardContent>
          </Card>
          
          <Card className="stat-card">
            <CardContent className="stat-content">
              <div className={`stat-icon ${cronEnabled ? 'green' : 'orange'}`}>
                <Timer size={24} />
              </div>
              <div className="stat-info">
                <span className="stat-value">{cronEnabled ? 'Activ' : 'Inactiv'}</span>
                <span className="stat-label">Cron zilnic</span>
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="content-grid">
          {/* Configuration Card */}
          <Card className="config-card" data-testid="config-section">
            <CardHeader>
              <CardTitle className="card-title">
                <Settings size={20} />
                Configurare
              </CardTitle>
              <CardDescription>Selectează perioada și pornește descărcarea</CardDescription>
            </CardHeader>
            <CardContent className="config-content">

              {/* PRIMARY ACTION: Date range + Start button */}
              <div className="form-group">
                <Label>Perioada dosarelor</Label>
                <div className="date-range-group">
                  <Popover>
                    <PopoverTrigger asChild>
                      <Button variant="outline" className="date-picker-btn" data-testid="date-start-btn">
                        <CalendarIcon size={16} />
                        {dateStart ? format(dateStart, 'dd.MM.yyyy') : 'Data început'}
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="calendar-popover" align="start">
                      <Calendar
                        mode="single"
                        selected={dateStart}
                        onSelect={setDateStart}
                        locale={ro}
                        initialFocus
                      />
                      {dateStart && (
                        <Button variant="ghost" size="sm" className="clear-date-btn" onClick={() => setDateStart(null)}>
                          Șterge data
                        </Button>
                      )}
                    </PopoverContent>
                  </Popover>

                  <span className="date-separator">→</span>

                  <Popover>
                    <PopoverTrigger asChild>
                      <Button variant="outline" className="date-picker-btn" data-testid="date-end-btn">
                        <CalendarIcon size={16} />
                        {dateEnd ? format(dateEnd, 'dd.MM.yyyy') : 'Data sfârșit'}
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="calendar-popover" align="start">
                      <Calendar
                        mode="single"
                        selected={dateEnd}
                        onSelect={setDateEnd}
                        locale={ro}
                        initialFocus
                      />
                      {dateEnd && (
                        <Button variant="ghost" size="sm" className="clear-date-btn" onClick={() => setDateEnd(null)}>
                          Șterge data
                        </Button>
                      )}
                    </PopoverContent>
                  </Popover>
                </div>
              </div>

              {/* Start / Stop buttons */}
              <div style={{display: 'flex', gap: '8px', marginTop: '8px', marginBottom: '4px'}}>
                <Button
                  className="run-primary-btn"
                  onClick={triggerRun}
                  disabled={!!currentRun}
                  data-testid="run-now-btn"
                  style={{flex: 1}}
                >
                  {currentRun
                    ? <><Loader2 className="animate-spin" size={16} style={{marginRight: 8}} />Descărcare în curs...</>
                    : <><Play size={16} style={{marginRight: 8}} />Start Descărcare</>
                  }
                </Button>
                {currentRun && (
                  <Button
                    variant="destructive"
                    onClick={stopDownloadJob}
                    data-testid="stop-run-btn"
                    style={{flexShrink: 0}}
                  >
                    <XCircle size={16} style={{marginRight: 6}} />
                    Oprește
                  </Button>
                )}
              </div>

              {/* Live log panel — shown only when running or logs available */}
              {(currentRun || downloadLogs.length > 0) && (
                <div className="download-log-panel" data-testid="download-log-panel">
                  <div className="download-log-header">
                    <Activity size={14} />
                    <span>Log descărcare</span>
                    {downloadProgress && (
                      <span className="download-log-stats">
                        {downloadProgress.processed}/{downloadProgress.total} instituții
                        {downloadProgress.dosare_found > 0 && ` • ${downloadProgress.dosare_found} dosare`}
                        {downloadProgress.firme_new > 0 && ` • ${downloadProgress.firme_new} firme noi`}
                      </span>
                    )}
                  </div>
                  {downloadProgress && downloadProgress.total > 0 && (
                    <Progress
                      value={(downloadProgress.processed / downloadProgress.total) * 100}
                      className="download-log-progress"
                    />
                  )}
                  <ScrollArea className="download-log-scroll">
                    <div className="download-log-content">
                      {downloadLogs.length === 0
                        ? <span className="download-log-empty">Se inițializează...</span>
                        : downloadLogs.map((line, i) => (
                            <div key={i} className="download-log-line">{line}</div>
                          ))
                      }
                    </div>
                  </ScrollArea>
                </div>
              )}

              {/* Advanced settings toggle */}
              <button
                className="advanced-toggle"
                onClick={() => setShowAdvancedConfig(v => !v)}
                data-testid="advanced-config-toggle"
                style={{
                  background: 'none', border: 'none', cursor: 'pointer',
                  color: 'var(--text-muted)', fontSize: '0.82rem',
                  display: 'flex', alignItems: 'center', gap: '4px',
                  marginTop: '8px', padding: '2px 0'
                }}
              >
                <ChevronDown size={14} style={{transform: showAdvancedConfig ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s'}} />
                {showAdvancedConfig ? 'Ascunde setări avansate' : 'Setări avansate (firmă, cron)'}
              </button>

              {showAdvancedConfig && (
                <div style={{marginTop: '12px'}}>
                  <div className="form-group">
                    <Label htmlFor="searchTerm">Nume Firmă <span style={{fontWeight: 'normal', opacity: 0.6, fontSize: '0.85em'}}>(opțional)</span></Label>
                    <div className="search-input-group">
                      <Input
                        id="searchTerm"
                        data-testid="search-term-input"
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        placeholder="Ex: SC EXEMPLU SRL — lasă gol pentru toate firmele"
                      />
                      <Button
                        variant="outline"
                        onClick={searchPreviewDosare}
                        disabled={searchLoading}
                        data-testid="preview-search-btn"
                      >
                        {searchLoading ? <Loader2 className="animate-spin" size={16} /> : <Search size={16} />}
                      </Button>
                    </div>
                  </div>

                  <Separator className="my-4" />

                  {/* Cron Configuration */}
                  <div className="cron-section">
                    <div className="form-group switch-group cron-toggle">
                      <div className="cron-label">
                        <Zap size={18} />
                        <div>
                          <Label>Cron Zilnic Activ</Label>
                          <p className="cron-description">Rulează automat în fiecare zi</p>
                        </div>
                      </div>
                      <Switch
                        id="cronEnabled"
                        data-testid="cron-enabled-switch"
                        checked={cronEnabled}
                        onCheckedChange={setCronEnabled}
                      />
                    </div>

                    <div className={`cron-time-config ${!cronEnabled ? 'disabled' : ''}`}>
                      <div className="form-row">
                        <div className="form-group">
                          <Label htmlFor="hour">Ora</Label>
                          <Input
                            id="hour"
                            data-testid="schedule-hour-input"
                            type="number" min="0" max="23"
                            value={scheduleHour}
                            onChange={(e) => setScheduleHour(parseInt(e.target.value) || 0)}
                            disabled={!cronEnabled}
                          />
                        </div>
                        <div className="form-group">
                          <Label htmlFor="minute">Minut</Label>
                          <Input
                            id="minute"
                            data-testid="schedule-minute-input"
                            type="number" min="0" max="59"
                            value={scheduleMinute}
                            onChange={(e) => setScheduleMinute(parseInt(e.target.value) || 0)}
                            disabled={!cronEnabled}
                          />
                        </div>
                      </div>
                      {cronEnabled && stats?.next_scheduled_run && (
                        <p className="next-run-info">
                          <Clock size={14} />
                          Următoarea rulare: {formatDate(stats.next_scheduled_run)}
                        </p>
                      )}
                    </div>
                  </div>

                  <div className="button-group" style={{marginTop: '12px'}}>
                    <Button onClick={saveConfig} data-testid="save-config-btn" variant="outline" size="sm">
                      Salvează Configurația
                    </Button>
                  </div>
                </div>
              )}

              {searchPreview && (
                <div className="preview-section" data-testid="search-preview">
                  <Separator className="my-4" />
                  <h4 className="preview-title">
                    <Building2 size={16} />
                    Rezultate căutare: {searchPreview.total} dosare
                  </h4>
                  <ScrollArea className="preview-scroll">
                    {searchPreview.dosare.slice(0, 5).map((dosar, idx) => (
                      <div key={idx} className="preview-item">
                        <span className="preview-number">{dosar.numar || '-'}</span>
                        <span className="preview-object">{dosar.obiect || '-'}</span>
                      </div>
                    ))}
                    {searchPreview.total > 5 && (
                      <p className="preview-more">...și încă {searchPreview.total - 5} dosare</p>
                    )}
                  </ScrollArea>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Current Run Status */}
          {currentRun && (
            <Card className="status-card running" data-testid="current-run-section">
              <CardHeader>
                <CardTitle className="card-title">
                  <Loader2 className="animate-spin" size={20} />
                  Job în Execuție
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="run-status">
                  <p>Descărcare în curs...</p>
                  <p className="run-count">{currentRun.records_downloaded} dosare descărcate</p>
                  <p className="run-progress">{currentRun.progress_message || ''}</p>
                  <Progress value={50} className="mt-2" />
                </div>
              </CardContent>
            </Card>
          )}

          {/* Files List */}
          <Card className="files-card" data-testid="files-section">
            <CardHeader>
              <div className="card-header-with-action">
                <div>
                  <CardTitle className="card-title">
                    <FileJson size={20} />
                    Fișiere Descărcate
                  </CardTitle>
                  <CardDescription>{files.length} fișiere salvate local</CardDescription>
                </div>
                <Button variant="ghost" size="icon" onClick={fetchData} data-testid="refresh-files-btn">
                  <RefreshCw size={16} />
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <ScrollArea className="files-scroll">
                {files.length === 0 ? (
                  <div className="empty-state">
                    <FileJson size={48} />
                    <p>Niciun fișier descărcat încă</p>
                  </div>
                ) : (
                  files.map((file, idx) => (
                    <div key={idx} className="file-item" data-testid={`file-item-${idx}`}>
                      <div className="file-info">
                        <span className="file-name">{file.name}</span>
                        <span className="file-meta">
                          {formatBytes(file.size)} • {formatDate(file.created)}
                        </span>
                      </div>
                      <div className="file-actions">
                        <Button 
                          variant="ghost" 
                          size="icon"
                          onClick={() => downloadFile(file.name)}
                          data-testid={`download-file-${idx}`}
                        >
                          <Download size={16} />
                        </Button>
                        <Button 
                          variant="ghost" 
                          size="icon"
                          onClick={() => deleteFile(file.name)}
                          data-testid={`delete-file-${idx}`}
                        >
                          <Trash2 size={16} />
                        </Button>
                      </div>
                    </div>
                  ))
                )}
              </ScrollArea>
            </CardContent>
          </Card>

          {/* Run History */}
          <Card className="history-card" data-testid="history-section">
            <CardHeader>
              <CardTitle className="card-title">
                <Clock size={20} />
                Istoric Rulări
              </CardTitle>
              <CardDescription>Ultimele 10 rulări</CardDescription>
            </CardHeader>
            <CardContent>
              <ScrollArea className="history-scroll">
                {runs.length === 0 ? (
                  <div className="empty-state">
                    <Clock size={48} />
                    <p>Nicio rulare înregistrată</p>
                  </div>
                ) : (
                  runs.slice(0, 10).map((run, idx) => (
                    <div key={idx} className="history-item" data-testid={`run-item-${idx}`}>
                      <div className="history-status">
                        {run.status === 'completed' ? (
                          <Badge variant="default" className="badge-success">
                            <CheckCircle2 size={12} />
                            Complet
                          </Badge>
                        ) : run.status === 'running' ? (
                          <Badge variant="secondary" className="badge-running">
                            <Loader2 className="animate-spin" size={12} />
                            În execuție
                          </Badge>
                        ) : (
                          <Badge variant="destructive" className="badge-failed">
                            <XCircle size={12} />
                            Eșuat
                          </Badge>
                        )}
                        {run.triggered_by === 'cron' && (
                          <Badge variant="outline" className="badge-cron">
                            <Timer size={10} />
                            Cron
                          </Badge>
                        )}
                      </div>
                      <div className="history-info">
                        <span className="history-date">{formatDate(run.started_at)}</span>
                        <span className="history-count">{run.total_records} dosare</span>
                      </div>
                    </div>
                  ))
                )}
              </ScrollArea>
            </CardContent>
          </Card>
        </div>
          </>
  );
}
