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

export default function DbFinalPage({ ctx }) {
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
          /* DB Final Tab - Companies with CUI */
          <div className="dbfinal-section" data-testid="dbfinal-section">
            <Card className="dbfinal-stats-card">
              <CardHeader>
                <div className="card-header-with-action">
                  <div>
                    <CardTitle className="card-title">
                      <CheckCircle2 size={20} />
                      DB Final - Firme cu CUI
                    </CardTitle>
                    <CardDescription>
                      Firme validate cu CUI, pregătite pentru procesare avansată
                    </CardDescription>
                  </div>
                  <Button variant="outline" onClick={() => { loadDbFinalStats(); loadDbFinal(dbFinalPage, dbFinalSearch); }}>
                    <RefreshCw size={16} />
                    Reîncarcă
                  </Button>
                  <Button
                    variant="outline"
                    onClick={() => {
                      const params = new URLSearchParams();
                      if (dbFinalSearch) params.set('search', dbFinalSearch);
                      if (dbFinalFilters?.doarActive) params.set('doar_active', 'true');
                      if (dbFinalFilters?.doarCuBilant) params.set('doar_cu_bilant', 'true');
                      window.open(`${API}/dbfinal/export?${params.toString()}`, '_blank');
                    }}
                    data-testid="export-dbfinal-btn"
                  >
                    <Download size={16} />
                    Export CSV
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {dbFinalStats && (
                  <>
                    {/* Main stats row */}
                    <div className="stats-grid-4">
                      <div className="stat-card primary">
                        <span className="stat-value">{dbFinalStats.total_cu_cui?.toLocaleString() || 0}</span>
                        <span className="stat-label">Total cu CUI</span>
                      </div>
                      <div className="stat-card success">
                        <span className="stat-value">{dbFinalStats.sincronizate_anaf?.toLocaleString() || 0}</span>
                        <span className="stat-label">Sincronizate ANAF</span>
                      </div>
                      <div className="stat-card info">
                        <span className="stat-value">{dbFinalStats.cu_date_bilant?.toLocaleString() || 0}</span>
                        <span className="stat-label">Cu Bilant</span>
                      </div>
                      <div className="stat-card warning">
                        <span className="stat-value">{dbFinalStats.nesincronizate?.toLocaleString() || 0}</span>
                        <span className="stat-label">Nesincronizate ANAF</span>
                      </div>
                    </div>

                    {/* Detailed breakdown */}
                    {(dbFinalStats.sincronizate_anaf > 0 || dbFinalStats.active > 0) && (
                      <div data-testid="stare-breakdown" style={{
                        marginTop: '14px', padding: '14px', background: 'var(--bg-secondary)',
                        borderRadius: '10px', border: '1px solid var(--border)'
                      }}>
                        <div style={{ fontSize: '0.82rem', fontWeight: 600, marginBottom: '10px', color: 'var(--text-muted)' }}>
                          Stare firme (din cele sincronizate ANAF)
                        </div>
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: '8px' }}>
                          <div style={{ padding: '10px', borderRadius: '8px', textAlign: 'center', background: 'rgba(34,197,94,0.1)', border: '1px solid rgba(34,197,94,0.25)' }}>
                            <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#22c55e' }}>{dbFinalStats.active_fiscal?.toLocaleString() || 0}</div>
                            <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '2px' }}>ACTIVE (inregistrat + activ fiscal)</div>
                          </div>
                          <div style={{ padding: '10px', borderRadius: '8px', textAlign: 'center', background: 'rgba(234,179,8,0.1)', border: '1px solid rgba(234,179,8,0.25)' }}>
                            <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#eab308' }}>{dbFinalStats.active_dar_inactiv_fiscal?.toLocaleString() || 0}</div>
                            <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '2px' }}>INREGISTRAT dar INACTIV fiscal</div>
                          </div>
                          <div style={{ padding: '10px', borderRadius: '8px', textAlign: 'center', background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.25)' }}>
                            <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#ef4444' }}>{dbFinalStats.radiate?.toLocaleString() || 0}</div>
                            <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '2px' }}>RADIATE (Inchise)</div>
                          </div>
                          <div style={{ padding: '10px', borderRadius: '8px', textAlign: 'center', background: 'rgba(249,115,22,0.1)', border: '1px solid rgba(249,115,22,0.25)' }}>
                            <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#f97316' }}>{dbFinalStats.suspendate?.toLocaleString() || 0}</div>
                            <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '2px' }}>SUSPENDATE</div>
                          </div>
                          <div style={{ padding: '10px', borderRadius: '8px', textAlign: 'center', background: 'rgba(99,102,241,0.1)', border: '1px solid rgba(99,102,241,0.25)' }}>
                            <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#6366f1' }}>{dbFinalStats.transfer?.toLocaleString() || 0}</div>
                            <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '2px' }}>TRANSFER</div>
                          </div>
                          <div style={{ padding: '10px', borderRadius: '8px', textAlign: 'center', background: 'rgba(14,165,233,0.1)', border: '1px solid rgba(14,165,233,0.25)' }}>
                            <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#0ea5e9' }}>{(dbFinalStats.reluare + dbFinalStats.dizolvare)?.toLocaleString() || 0}</div>
                            <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '2px' }}>RELUARE / DIZOLVARE</div>
                          </div>
                        </div>

                        {/* Fiscal / TVA section */}
                        {(dbFinalStats.platitori_tva > 0 || dbFinalStats.e_factura > 0) && (
                          <>
                            <div style={{ fontSize: '0.82rem', fontWeight: 600, marginTop: '14px', marginBottom: '8px', color: 'var(--text-muted)' }}>
                              Date fiscale
                            </div>
                            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: '8px' }}>
                              <div style={{ padding: '10px', borderRadius: '8px', textAlign: 'center', background: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.25)' }}>
                                <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#10b981' }}>{dbFinalStats.platitori_tva?.toLocaleString() || 0}</div>
                                <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '2px' }}>Platitori TVA</div>
                              </div>
                              <div style={{ padding: '10px', borderRadius: '8px', textAlign: 'center', background: 'rgba(20,184,166,0.1)', border: '1px solid rgba(20,184,166,0.25)' }}>
                                <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#14b8a6' }}>{dbFinalStats.tva_incasare?.toLocaleString() || 0}</div>
                                <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '2px' }}>TVA la incasare</div>
                              </div>
                              <div style={{ padding: '10px', borderRadius: '8px', textAlign: 'center', background: 'rgba(59,130,246,0.1)', border: '1px solid rgba(59,130,246,0.25)' }}>
                                <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#3b82f6' }}>{dbFinalStats.e_factura?.toLocaleString() || 0}</div>
                                <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '2px' }}>e-Factura</div>
                              </div>
                              <div style={{ padding: '10px', borderRadius: '8px', textAlign: 'center', background: 'rgba(239,68,68,0.06)', border: '1px solid rgba(239,68,68,0.15)' }}>
                                <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#ef4444' }}>{dbFinalStats.inactiv_anaf?.toLocaleString() || 0}</div>
                                <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '2px' }}>Inactiv fiscal (total)</div>
                              </div>
                            </div>
                          </>
                        )}
                      </div>
                    )}
                  </>
                )}
              </CardContent>
            </Card>

            {/* Filters and Search */}
            <Card className="dbfinal-filters-card">
              <CardContent>
                <div className="dbfinal-controls">
                  <div className="search-box">
                    <input
                      type="text"
                      placeholder="Caută după denumire sau CUI..."
                      value={dbFinalSearch}
                      onChange={(e) => setDbFinalSearch(e.target.value)}
                      onKeyPress={(e) => e.key === 'Enter' && handleDbFinalSearch()}
                    />
                    <Button onClick={handleDbFinalSearch}>
                      <Search size={16} />
                      Caută
                    </Button>
                  </div>
                  <div className="filter-toggles">
                    <label className="filter-toggle">
                      <input
                        type="checkbox"
                        checked={dbFinalFilters.doarActive}
                        onChange={(e) => handleDbFinalFilterChange({ doarActive: e.target.checked })}
                      />
                      <span>Doar Active</span>
                    </label>
                    <label className="filter-toggle">
                      <input
                        type="checkbox"
                        checked={dbFinalFilters.doarCuBilant}
                        onChange={(e) => handleDbFinalFilterChange({ doarCuBilant: e.target.checked })}
                      />
                      <span>Doar cu Bilanț</span>
                    </label>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Companies List */}
            <Card className="dbfinal-list-card">
              <CardHeader>
                <CardTitle className="card-title">
                  Firme ({dbFinalTotal.toLocaleString()})
                </CardTitle>
              </CardHeader>
              <CardContent>
                {dbFinalLoading ? (
                  <div className="loading-spinner">
                    <Loader2 className="animate-spin" size={32} />
                    <span>Se încarcă...</span>
                  </div>
                ) : (
                  <div className="table-container">
                    <table className="dbfinal-table">
                      <thead>
                        <tr>
                          <th>CUI</th>
                          <th>Denumire</th>
                          <th>Județ</th>
                          <th>Stare</th>
                          <th>Cifră Afaceri</th>
                          <th>Profit</th>
                          <th>Angajați</th>
                          <th>An Bilanț</th>
                          <th>TVA</th>
                          <th>Sync</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dbFinalList.map((firma) => (
                          <tr key={firma.id} className="clickable-row" onClick={() => openFirmaProfile(firma.id)}>
                            <td onClick={e => e.stopPropagation()}>
                              {inlineEditId === firma.id ? (
                                <div style={{display:'flex',gap:'4px',alignItems:'center'}}>
                                  <input className="cui-edit-input" value={inlineEditValue} onChange={e => setInlineEditValue(e.target.value)} placeholder="CUI" autoFocus style={{width:'90px',padding:'2px 6px',fontSize:'0.8rem'}} onKeyDown={e => { if (e.key === 'Enter') saveInlineCui(firma.id); if (e.key === 'Escape') setInlineEditId(null); }} />
                                  <button onClick={() => saveInlineCui(firma.id)} style={{background:'none',border:'none',cursor:'pointer',color:'var(--primary)',padding:'2px'}}><CheckCircle2 size={14}/></button>
                                  <button onClick={() => setInlineEditId(null)} style={{background:'none',border:'none',cursor:'pointer',color:'var(--text-muted)',padding:'2px'}}><XCircle size={14}/></button>
                                </div>
                              ) : (
                                <div style={{display:'flex',gap:'4px',alignItems:'center'}}>
                                  <Badge variant="outline">{firma.cui}</Badge>
                                  <button onClick={e => { e.stopPropagation(); setInlineEditId(firma.id); setInlineEditValue(firma.cui || ""); }} style={{background:'none',border:'none',cursor:'pointer',color:'var(--text-muted)',padding:'2px',opacity:0.6}} title="Editează CUI"><Wrench size={11}/></button>
                                </div>
                              )}
                            </td>
                            <td className="col-denumire" title={firma.denumire}>{firma.denumire}</td>
                            <td>{firma.judet || '-'}</td>
                            <td>
                              {firma.stare && (
                                <Badge variant={firma.stare.includes('ACTIV') && !firma.stare.includes('INACTIV') ? 'success' : 'secondary'}>
                                  {firma.stare.substring(0, 20)}
                                </Badge>
                              )}
                            </td>
                            <td className="col-number">{firma.cifra_afaceri ? `${(firma.cifra_afaceri / 1000).toFixed(0)}k` : '-'}</td>
                            <td className="col-number">{firma.profit ? `${(firma.profit / 1000).toFixed(0)}k` : '-'}</td>
                            <td className="col-number">{firma.angajati || '-'}</td>
                            <td>{firma.an_bilant || '-'}</td>
                            <td>{firma.platitor_tva ? <CheckCircle2 size={16} className="text-success" /> : <XCircle size={16} className="text-muted" />}</td>
                            <td>
                              {firma.anaf_sync && <Badge variant="outline" className="sync-badge">ANAF</Badge>}
                              {firma.mf_sync && <Badge variant="outline" className="sync-badge">MF</Badge>}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {/* Pagination */}
                <div className="firme-pagination">
                  <Button 
                    variant="outline" 
                    disabled={dbFinalPage === 0 || dbFinalLoading}
                    onClick={() => setDbFinalPage(p => Math.max(0, p - 1))}
                  >
                    ← Anterior
                  </Button>
                  <span className="pagination-info">
                    Pagina {dbFinalPage + 1} din {Math.ceil(dbFinalTotal / 50) || 1}
                  </span>
                  <Button 
                    variant="outline" 
                    disabled={(dbFinalPage + 1) * 50 >= dbFinalTotal || dbFinalLoading}
                    onClick={() => setDbFinalPage(p => p + 1)}
                  >
                    Următor →
                  </Button>
                </div>
              </CardContent>
            </Card>
          </div>
  );
}
