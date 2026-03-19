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

export default function FirmePage({ ctx }) {
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
          /* Firme Tab */
          <div className="firme-container" data-testid="firme-section">
            <Card className="firme-card">
              <CardHeader>
                <div className="card-header-with-action">
                  <div>
                    <CardTitle className="card-title">
                      <Building2 size={20} />
                      Firme în Baza de Date
                    </CardTitle>
                    <CardDescription>
                      {firmeTotal.toLocaleString()} firme totale • {dbStats?.firme_with_cui?.toLocaleString() || 0} cu CUI
                    </CardDescription>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {/* Search */}
                <div className="firme-search">
                  <Input
                    placeholder="Caută după denumire sau CUI..."
                    value={firmeSearch}
                    onChange={(e) => setFirmeSearch(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && handleFirmeSearch()}
                    data-testid="firme-search-input"
                  />
                  <Button onClick={handleFirmeSearch} disabled={firmeLoading}>
                    <Search size={16} />
                    Caută
                  </Button>
                  <Button variant="outline" onClick={() => { setFirmeSearch(""); setFirmePage(0); loadFirme(0, ""); }}>
                    Reset
                  </Button>
                </div>

                {/* Firme Table */}
                <div className="firme-table-container">
                  {firmeLoading ? (
                    <div className="firme-loading">
                      <Loader2 className="animate-spin" size={32} />
                      <p>Se încarcă...</p>
                    </div>
                  ) : (
                    <table className="firme-table">
                      <thead>
                        <tr>
                          <th>CUI</th>
                          <th>Denumire</th>
                          <th>Formă</th>
                          <th>Județ</th>
                          <th>Localitate</th>
                          <th>Adresă</th>
                          <th>Înreg.</th>
                          <th>Dosare</th>
                        </tr>
                      </thead>
                      <tbody>
                        {firmeList.map((firma) => (
                          <tr key={firma.id} data-testid={`firma-row-${firma.id}`} className="clickable-row" onClick={() => openFirmaProfile(firma.id)}>
                            <td className="col-cui" onClick={e => e.stopPropagation()}>
                              {inlineEditId === firma.id ? (
                                <div className="cui-edit-row" style={{display:'flex',gap:'4px',alignItems:'center'}}>
                                  <input
                                    className="cui-edit-input"
                                    value={inlineEditValue}
                                    onChange={e => setInlineEditValue(e.target.value)}
                                    placeholder="CUI"
                                    autoFocus
                                    style={{width:'90px',padding:'2px 6px',fontSize:'0.8rem'}}
                                    onKeyDown={e => { if (e.key === 'Enter') saveInlineCui(firma.id); if (e.key === 'Escape') setInlineEditId(null); }}
                                  />
                                  <button onClick={() => saveInlineCui(firma.id)} style={{background:'none',border:'none',cursor:'pointer',color:'var(--primary)',padding:'2px'}} title="Salvează"><CheckCircle2 size={14}/></button>
                                  <button onClick={() => setInlineEditId(null)} style={{background:'none',border:'none',cursor:'pointer',color:'var(--text-muted)',padding:'2px'}} title="Anulează"><XCircle size={14}/></button>
                                </div>
                              ) : (
                                <div style={{display:'flex',gap:'4px',alignItems:'center'}}>
                                  {firma.cui ? <Badge variant="outline" className="badge-cui">{firma.cui}</Badge> : <span className="no-cui">-</span>}
                                  <button
                                    onClick={e => { e.stopPropagation(); setInlineEditId(firma.id); setInlineEditValue(firma.cui || ""); }}
                                    style={{background:'none',border:'none',cursor:'pointer',color:'var(--text-muted)',padding:'2px',opacity:0.6}}
                                    title="Editează CUI"
                                    data-testid={`edit-cui-inline-${firma.id}`}
                                  ><Wrench size={11}/></button>
                                </div>
                              )}
                            </td>
                            <td className="col-denumire" title={firma.denumire}>{firma.denumire}</td>
                            <td className="col-forma">
                              {firma.forma_juridica && (
                                <Badge variant="secondary" className="badge-forma">{firma.forma_juridica}</Badge>
                              )}
                            </td>
                            <td className="col-judet">{firma.judet || '-'}</td>
                            <td className="col-localitate">{firma.localitate || '-'}</td>
                            <td className="col-adresa" title={`${firma.strada || ''} ${firma.numar || ''}`}>
                              {firma.strada ? `${firma.strada} ${firma.numar || ''}`.trim() : '-'}
                            </td>
                            <td className="col-inreg">
                              {firma.cod_inregistrare && (
                                <span className="cod-inreg" title={firma.data_inregistrare}>{firma.cod_inregistrare}</span>
                              )}
                            </td>
                            <td className="col-dosare">{firma.dosare_count || 0}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>

                {/* Pagination */}
                <div className="firme-pagination">
                  <Button 
                    variant="outline" 
                    disabled={firmePage === 0 || firmeLoading}
                    onClick={() => setFirmePage(p => Math.max(0, p - 1))}
                  >
                    ← Anterior
                  </Button>
                  <span className="pagination-info">
                    Pagina {firmePage + 1} din {Math.ceil(firmeTotal / 50) || 1}
                    <small>({(firmePage * 50) + 1} - {Math.min((firmePage + 1) * 50, firmeTotal)} din {firmeTotal.toLocaleString()})</small>
                  </span>
                  <Button 
                    variant="outline" 
                    disabled={(firmePage + 1) * 50 >= firmeTotal || firmeLoading}
                    onClick={() => setFirmePage(p => p + 1)}
                  >
                    Următor →
                  </Button>
                </div>
              </CardContent>
            </Card>
          </div>
  );
}
