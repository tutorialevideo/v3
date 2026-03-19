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
          </div>
  );
}
