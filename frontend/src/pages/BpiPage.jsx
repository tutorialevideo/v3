import { useState, useRef, useEffect } from "react";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { Badge } from "../components/ui/badge";
import { ScrollArea } from "../components/ui/scroll-area";
import { Loader2, FileJson, Upload, CheckCircle2, XCircle, Building2, Search, ChevronDown, FolderDown } from "lucide-react";
import axios from "axios";
import { toast } from "sonner";

const API = "/api";

export default function BpiPage({ ctx }) {
  const {
    bpiParsing, bpiResults, bpiHistory, bpiStats,
    parseBpiPdf, saveBpiRecord, loadBpiHistory,
    openFirmaProfile,
    bpiFolderInfo, bpiScanProgress, bpiScanLogs, bpiScanning,
    loadBpiFolderInfo, startBpiFolderScan, stopBpiFolderScan,
  } = ctx;

  const [dragOver, setDragOver] = useState(false);
  const [expandedIdx, setExpandedIdx] = useState(null);
  const [liteparseVersion, setLiteparseVersion] = useState(null);
  const [folderUploadProgress, setFolderUploadProgress] = useState(null);
  const [folderResults, setFolderResults] = useState([]);
  const [uploadMode, setUploadMode] = useState('file');
  const [exportFiles, setExportFiles] = useState([]);
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState(null);
  const fileInputRef = useRef(null);
  const folderInputRef = useRef(null);

  useEffect(() => {
    fetch(`${API}/bpi/liteparse-version`)
      .then(r => r.json())
      .then(d => setLiteparseVersion(d))
      .catch(() => {});
    fetch(`${API}/bpi/exports`)
      .then(r => r.json())
      .then(d => setExportFiles(Array.isArray(d) ? d : []))
      .catch(() => {});
  }, []);

  const importToDB = async (filename = null) => {
    setImporting(true);
    setImportResult(null);
    try {
      const url = filename
        ? `${API}/bpi/import-to-firme?filename=${encodeURIComponent(filename)}&only_new=true`
        : `${API}/bpi/import-to-firme?only_new=true`;
      const res = await axios.post(url);
      setImportResult(res.data);
      toast.success(res.data.message);
      // Refresh exports
      fetch(`${API}/bpi/exports`).then(r => r.json()).then(d => setExportFiles(Array.isArray(d) ? d : []));
    } catch (e) {
      toast.error(e.response?.data?.detail || 'Eroare import');
    } finally {
      setImporting(false);
    }
  };

  const handleFile = (files) => {
    const fileList = files instanceof FileList ? Array.from(files) : [files];
    const pdfs = fileList.filter(f => f.name.toLowerCase().endsWith('.pdf'));
    if (pdfs.length === 0) return;
    if (pdfs.length === 1) {
      parseBpiPdf(pdfs[0]);
    } else {
      handleFolderUpload(fileList);
    }
  };

  const handleFolderUpload = async (files) => {
    const pdfs = Array.from(files).filter(f => f.name.toLowerCase().endsWith('.pdf'));
    if (pdfs.length === 0) {
      toast.error('Niciun PDF găsit în folder');
      return;
    }

    const totalSize = pdfs.reduce((s, f) => s + f.size, 0);
    const totalSizeMB = (totalSize / (1024 * 1024)).toFixed(1);

    setFolderResults([]);
    setFolderUploadProgress({ total: pdfs.length, processed: 0, records: 0, errors: 0, sizeMB: totalSizeMB });
    toast.info(`Procesare ${pdfs.length} PDF-uri (${totalSizeMB} MB)...`);

    // Process in batches of 3 (parallel uploads)
    const BATCH = 3;
    for (let i = 0; i < pdfs.length; i += BATCH) {
      const batch = pdfs.slice(i, i + BATCH);
      await Promise.all(batch.map(async (file) => {
        try {
          const formData = new FormData();
          formData.append('file', file);
          const res = await axios.post(`${API}/bpi/parse`, formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
          });
          setFolderResults(prev => [...prev, { ...res.data, filename: file.name }]);
          setFolderUploadProgress(prev => ({
            ...prev,
            processed: prev.processed + 1,
            records: prev.records + (res.data.records_count || 0)
          }));
        } catch (err) {
          setFolderUploadProgress(prev => ({
            ...prev,
            processed: prev.processed + 1,
            errors: prev.errors + 1
          }));
        }
      }));
    }
    toast.success(`Folder procesat!`);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    const files = e.dataTransfer.files;
    if (files.length > 1) {
      handleFolderUpload(files);
    } else {
      handleFile(files);
    }
  };

  const tipBadgeStyle = (tip) => {
    if (!tip) return {};
    if (tip.includes('Faliment')) return { background: 'rgba(239,68,68,0.15)', color: '#ef4444' };
    if (tip.includes('Insolventa') || tip.includes('Insolventa')) return { background: 'rgba(234,179,8,0.15)', color: '#eab308' };
    if (tip.includes('Reorganizare')) return { background: 'rgba(99,102,241,0.15)', color: '#6366f1' };
    return {};
  };

  return (
    <div className="tab-content">
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>

        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>

          {bpiStats && bpiStats.total_records > 0 && (
            <div style={{ display: 'flex', gap: '10px' }}>
              <div className="anaf-stat-card" style={{ flex: 1 }}>
                <span className="stat-value">{bpiStats.total_records?.toLocaleString()}</span>
                <span className="stat-label">Total BPI salvate</span>
              </div>
            </div>
          )}

          <Card>
            <CardHeader>
              <CardTitle className="card-title">
                <FileJson size={20} />
                BPI Parser
              </CardTitle>
              <CardDescription>
                Parsare locala PDF-uri BPI. Extrage: firma, CUI, dosar, tribunal, tip procedura, termen, administrator.
                {liteparseVersion?.installed && (
                  <span style={{ marginLeft: 8, color: '#22c55e', fontSize: '0.75rem' }}>
                    LiteParse v{liteparseVersion.version} instalat
                  </span>
                )}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div
                data-testid="bpi-upload-zone"
                onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
                onDragLeave={() => setDragOver(false)}
                onDrop={handleDrop}
                onClick={() => fileInputRef.current?.click()}
                style={{
                  border: `2px dashed ${dragOver ? 'var(--primary)' : 'var(--border)'}`,
                  borderRadius: '10px', padding: '32px 20px', textAlign: 'center',
                  cursor: 'pointer', background: 'var(--bg-secondary)', marginBottom: '12px',
                  transition: 'all 0.2s'
                }}
              >
                {bpiParsing ? (
                  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px' }}>
                    <Loader2 className="animate-spin" size={32} style={{ color: 'var(--primary)' }} />
                    <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>Se parseaza PDF-ul...</p>
                  </div>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px' }}>
                    <Upload size={32} style={{ color: 'var(--text-muted)' }} />
                    <p style={{ fontWeight: 600, fontSize: '0.95rem' }}>Drag & Drop PDF-uri BPI</p>
                    <p style={{ color: 'var(--text-muted)', fontSize: '0.82rem' }}>unul sau mai multe fișiere simultan</p>
                    <p style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>sau folosește butoanele de mai jos</p>
                  </div>
                )}
              </div>

              <input ref={fileInputRef} type="file" accept=".pdf" multiple
                style={{ display: 'none' }}
                data-testid="bpi-file-input"
                onChange={(e) => handleFile(e.target.files)} />

              {/* Hidden folder input */}
              <input ref={folderInputRef} type="file" accept=".pdf"
                webkitdirectory="true" directory="true" multiple
                style={{ display: 'none' }}
                data-testid="bpi-folder-input"
                onChange={(e) => handleFolderUpload(e.target.files)} />

              <div style={{ display: 'flex', gap: '8px' }}>
                <Button onClick={() => fileInputRef.current?.click()} disabled={bpiParsing}
                  variant="outline" style={{ flex: 1 }} data-testid="bpi-upload-btn">
                  {bpiParsing
                    ? <><Loader2 className="animate-spin" size={16} style={{ marginRight: 6 }} />Procesare...</>
                    : <><Upload size={16} style={{ marginRight: 6 }} />PDF-uri multiple</>
                  }
                </Button>
                <Button onClick={() => folderInputRef.current?.click()}
                  disabled={bpiParsing || folderUploadProgress?.processed < folderUploadProgress?.total}
                  style={{ flex: 1 }} data-testid="bpi-folder-btn">
                  <FolderDown size={16} style={{ marginRight: 6 }} />
                  Folder (max 1GB)
                </Button>
              </div>

              {/* Folder upload progress */}
              {folderUploadProgress && folderUploadProgress.processed < folderUploadProgress.total && (
                <div style={{ marginTop: '12px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.78rem', color: 'var(--text-muted)', marginBottom: '4px' }}>
                    <span>Procesare {folderUploadProgress.processed}/{folderUploadProgress.total} PDF-uri ({folderUploadProgress.sizeMB} MB)</span>
                    <span style={{ color: 'var(--primary)' }}>{Math.round(folderUploadProgress.processed / folderUploadProgress.total * 100)}%</span>
                  </div>
                  <div style={{ height: '6px', background: 'var(--border)', borderRadius: '3px', overflow: 'hidden' }}>
                    <div style={{ height: '100%', background: 'var(--primary)', width: `${folderUploadProgress.processed / folderUploadProgress.total * 100}%`, transition: 'width 0.3s' }} />
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '4px' }}>
                    Înregistrări extrase: <strong style={{ color: 'var(--primary)' }}>{folderUploadProgress.records?.toLocaleString()}</strong>
                    {folderUploadProgress.errors > 0 && <span style={{ color: '#ef4444', marginLeft: 8 }}>{folderUploadProgress.errors} erori</span>}
                  </div>
                </div>
              )}

              {/* Folder results summary */}
              {folderUploadProgress?.processed === folderUploadProgress?.total && folderUploadProgress?.total > 1 && (
                <div style={{ marginTop: '10px', padding: '10px 12px', background: 'rgba(34,197,94,0.08)', border: '1px solid rgba(34,197,94,0.25)', borderRadius: '8px', fontSize: '0.82rem' }}>
                  <CheckCircle2 size={14} style={{ color: '#22c55e', marginRight: 6, display: 'inline' }} />
                  <strong>{folderUploadProgress.total} PDF-uri</strong> procesate — <strong style={{ color: 'var(--primary)' }}>{folderUploadProgress.records?.toLocaleString()} înregistrări</strong> extrase
                  {folderUploadProgress.errors > 0 && <span style={{ color: '#ef4444' }}> ({folderUploadProgress.errors} erori)</span>}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Batch results — all records from multi-file upload */}
          {folderResults.length > 0 && folderUploadProgress?.processed === folderUploadProgress?.total && (
            <Card>
              <CardHeader>
                <CardTitle className="card-title" style={{fontSize:'1rem'}}>
                  <CheckCircle2 size={18} style={{color:'#22c55e'}} />
                  Rezultate batch — {folderResults.reduce((s, r) => s + (r.records_count || 0), 0)} înregistrări din {folderResults.length} fișiere
                </CardTitle>
              </CardHeader>
              <CardContent>
                <ScrollArea style={{height: Math.min(folderResults.reduce((s, r) => s + (r.records_count || 0), 0) * 80 + 100, 500) + 'px'}}>
                  {folderResults.map((fileResult, fi) => (
                    <div key={fi} style={{marginBottom:'12px'}}>
                      <div style={{fontSize:'0.78rem', color:'var(--text-muted)', marginBottom:'4px', fontWeight:600}}>
                        {fileResult.filename} — {fileResult.records_count || 0} înregistrări
                      </div>
                      {(fileResult.records || []).map((rec, ri) => (
                        rec.cui || rec.dosar ? (
                          <div key={ri} style={{
                            padding:'8px 12px', marginBottom:'6px',
                            border:'1px solid var(--border)', borderRadius:'6px',
                            background:'var(--bg-secondary)', fontSize:'0.8rem'
                          }}>
                            <div style={{display:'flex', justifyContent:'space-between', alignItems:'center'}}>
                              <div>
                                <strong>{rec.denumire_firma || <em style={{opacity:0.5}}>Firma neidentificata</em>}</strong>
                                {rec.cui && <span style={{marginLeft:8, color:'var(--primary)', fontSize:'0.75rem'}}>CUI: {rec.cui}</span>}
                              </div>
                              <div style={{display:'flex', gap:'4px', alignItems:'center'}}>
                                {rec.tip_procedura && <Badge variant="outline" style={{fontSize:'0.65rem'}}>{rec.tip_procedura}</Badge>}
                                {rec.firma_match && <Badge variant="outline" style={{fontSize:'0.65rem', background:'rgba(34,197,94,0.15)', color:'#22c55e'}}>in DB</Badge>}
                              </div>
                            </div>
                            <div style={{color:'var(--text-muted)', marginTop:'2px', display:'flex', gap:'12px', flexWrap:'wrap'}}>
                              {rec.tribunal && <span>{rec.tribunal}</span>}
                              {rec.dosar && <span>Dosar: {rec.dosar}</span>}
                              {rec.termen && <span>Termen: {rec.termen}</span>}
                              {rec.administrator_judiciar && <span>Admin: {rec.administrator_judiciar}</span>}
                            </div>
                            {rec.firma_match && (
                              <div style={{marginTop:'4px', fontSize:'0.72rem', color:'#22c55e'}}>
                                Match DB: {rec.firma_match.denumire}
                              </div>
                            )}
                          </div>
                        ) : null
                      ))}
                    </div>
                  ))}
                </ScrollArea>
              </CardContent>
            </Card>
          )}

          {/* CUI Export + Import Card */}
          <Card>
            <CardHeader>
              <CardTitle className="card-title" style={{fontSize:'1rem'}}>
                <CheckCircle2 size={18} style={{color:'#22c55e'}} />
                CUI-uri extrase — Import în DB
              </CardTitle>
              <CardDescription>
                După parsare, CUI-urile sunt salvate automat în CSV. Importă-le în baza de firme pentru verificare ANAF.
              </CardDescription>
            </CardHeader>
            <CardContent>
              {/* Import from all bpi_records */}
              <div style={{marginBottom:'12px'}}>
                <Button
                  onClick={() => importToDB(null)}
                  disabled={importing}
                  style={{width:'100%', marginBottom:'6px'}}
                  data-testid="import-all-bpi-btn"
                >
                  {importing
                    ? <><Loader2 className="animate-spin" size={14} style={{marginRight:6}}/>Se importă...</>
                    : <><CheckCircle2 size={14} style={{marginRight:6}}/>Importă toate CUI-urile BPI în Firme</>
                  }
                </Button>
                <p style={{fontSize:'0.75rem', color:'var(--text-muted)'}}>
                  Adaugă firmele din BPI în tabelul Firme → le poți verifica la ANAF
                </p>
              </div>

              {importResult && (
                <div style={{padding:'10px 12px', background:'rgba(34,197,94,0.08)', border:'1px solid rgba(34,197,94,0.25)', borderRadius:'8px', marginBottom:'10px', fontSize:'0.82rem'}}>
                  <strong style={{color:'#22c55e'}}>{importResult.message}</strong>
                  <div style={{color:'var(--text-muted)', fontSize:'0.75rem', marginTop:'3px'}}>
                    Total CUI-uri unice: {importResult.total_cuis?.toLocaleString()}
                  </div>
                </div>
              )}

              {/* CSV Export files */}
              {exportFiles.length > 0 && (
                <div>
                  <div style={{fontSize:'0.78rem', color:'var(--text-muted)', marginBottom:'6px', fontWeight:600}}>
                    Fișiere CSV exportate automat:
                  </div>
                  {exportFiles.map((f, i) => (
                    <div key={i} style={{display:'flex', alignItems:'center', gap:'8px', padding:'6px 10px', background:'var(--bg-secondary)', borderRadius:'6px', marginBottom:'4px', fontSize:'0.78rem'}}>
                      <span style={{flex:1}}>{f.name}</span>
                      <span style={{color:'var(--text-muted)'}}>{f.size_kb} KB</span>
                      <a href={`${API}/bpi/exports/${encodeURIComponent(f.name)}`} download
                        style={{color:'var(--primary)', textDecoration:'none', fontSize:'0.75rem'}}>
                        ↓ Download
                      </a>
                      <button onClick={() => importToDB(f.name)} disabled={importing}
                        style={{background:'none', border:'1px solid var(--primary)', color:'var(--primary)', borderRadius:'4px', padding:'2px 8px', cursor:'pointer', fontSize:'0.72rem'}}>
                        Import în DB
                      </button>
                    </div>
                  ))}
                </div>
              )}

              {exportFiles.length === 0 && (
                <p style={{fontSize:'0.78rem', color:'var(--text-muted)', fontStyle:'italic'}}>
                  Niciun CSV generat încă. Parseaza un PDF BPI pentru a genera primul export.
                </p>
              )}
            </CardContent>
          </Card>

          {/* Folder Scan Card */}
          <Card>
            <CardHeader>
              <CardTitle className="card-title" style={{fontSize:'1rem'}}>
                <Upload size={18} />
                Scan Folder Server (20-30GB)
              </CardTitle>
              <CardDescription>
                Procesare colectii mari de PDF-uri BPI direct de pe server — fara upload browser.
              </CardDescription>
            </CardHeader>
            <CardContent>
              {/* Folder info */}
              {bpiFolderInfo ? (
                <div style={{marginBottom:'12px'}}>
                  <div style={{
                    padding:'10px 14px', borderRadius:'8px', fontSize:'0.82rem',
                    background: bpiFolderInfo.exists ? 'rgba(34,197,94,0.08)' : 'rgba(239,68,68,0.08)',
                    border: `1px solid ${bpiFolderInfo.exists ? 'rgba(34,197,94,0.3)' : 'rgba(239,68,68,0.3)'}`,
                    marginBottom:'8px'
                  }}>
                    <div style={{fontWeight:600, marginBottom:'4px'}}>
                      {bpiFolderInfo.exists ? '✅ Folder montat' : '❌ Folder negăsit'}
                    </div>
                    <div style={{color:'var(--text-muted)', fontFamily:'monospace', fontSize:'0.78rem'}}>{bpiFolderInfo.path}</div>
                    {bpiFolderInfo.exists && (
                      <div style={{marginTop:'6px', display:'flex', gap:'16px', flexWrap:'wrap'}}>
                        <span><strong>{bpiFolderInfo.total_pdfs?.toLocaleString()}</strong> PDF-uri</span>
                        <span><strong>{bpiFolderInfo.total_size_gb}</strong> GB</span>
                        <span style={{color:'#22c55e'}}><strong>{bpiFolderInfo.already_processed?.toLocaleString()}</strong> procesate</span>
                        <span style={{color:'#eab308'}}><strong>{bpiFolderInfo.remaining?.toLocaleString()}</strong> rămase</span>
                      </div>
                    )}
                  </div>

                  {!bpiFolderInfo.exists && (
                    <div style={{fontSize:'0.78rem', color:'var(--text-muted)', padding:'8px', background:'var(--bg-secondary)', borderRadius:'6px'}}>
                      <strong>Configurare docker-compose.yml:</strong><br/>
                      <code style={{display:'block', marginTop:'4px', fontFamily:'monospace'}}>
                        volumes:<br/>
                        &nbsp;&nbsp;- /calea/ta/bpi_pdfs:/app/bpi_input
                      </code>
                      <p style={{marginTop:'6px'}}>Pune toate PDF-urile în folderul <code>/calea/ta/bpi_pdfs</code> pe serverul tău, apoi rebuild Docker.</p>
                    </div>
                  )}
                </div>
              ) : (
                <div style={{display:'flex', justifyContent:'center', padding:'8px'}}>
                  <button onClick={loadBpiFolderInfo} style={{background:'none', border:'none', cursor:'pointer', color:'var(--primary)', fontSize:'0.85rem'}}>
                    Verifică folder →
                  </button>
                </div>
              )}

              {/* Scan controls */}
              {bpiFolderInfo?.exists && bpiFolderInfo?.remaining > 0 && (
                <div style={{display:'flex', gap:'8px', marginBottom:'10px'}}>
                  <Button onClick={() => startBpiFolderScan(true)} disabled={bpiScanning} style={{flex:1}} data-testid="start-folder-scan-btn">
                    {bpiScanning
                      ? <><Loader2 className="animate-spin" size={14} style={{marginRight:6}} />Procesare {bpiScanProgress?.processed?.toLocaleString()}/{bpiScanProgress?.total_files?.toLocaleString()}...</>
                      : <><Upload size={14} style={{marginRight:6}} />Scanează {bpiFolderInfo.remaining?.toLocaleString()} PDF-uri</>
                    }
                  </Button>
                  {bpiScanning && (
                    <Button variant="destructive" onClick={stopBpiFolderScan} size="sm">
                      <XCircle size={14} />
                    </Button>
                  )}
                </div>
              )}

              {bpiFolderInfo?.exists && bpiFolderInfo?.remaining === 0 && !bpiScanning && (
                <p style={{color:'#22c55e', fontSize:'0.82rem', textAlign:'center', padding:'8px'}}>
                  ✅ Toate PDF-urile din folder au fost procesate!
                </p>
              )}

              {/* Progress bar */}
              {bpiScanProgress && bpiScanProgress.total_files > 0 && (
                <div style={{marginBottom:'8px'}}>
                  <div style={{
                    height:'6px', borderRadius:'3px', background:'var(--border)', overflow:'hidden', marginBottom:'6px'
                  }}>
                    <div style={{
                      height:'100%', background:'var(--primary)', transition:'width 0.3s',
                      width: `${(bpiScanProgress.processed / bpiScanProgress.total_files * 100).toFixed(1)}%`
                    }} />
                  </div>
                  <div style={{display:'flex', gap:'12px', fontSize:'0.75rem', color:'var(--text-muted)', flexWrap:'wrap'}}>
                    <span>Procesate: <strong>{bpiScanProgress.processed?.toLocaleString()}</strong></span>
                    <span>Înregistrări: <strong style={{color:'var(--primary)'}}>{bpiScanProgress.records_found?.toLocaleString()}</strong></span>
                    <span>Erori: <strong style={{color:'#ef4444'}}>{bpiScanProgress.errors}</strong></span>
                  </div>
                </div>
              )}

              {/* Live log */}
              {bpiScanLogs.length > 0 && (
                <div className="download-log-panel" style={{marginTop:'8px'}}>
                  <div className="download-log-header">
                    <Upload size={13} />
                    <span>Log scan folder BPI</span>
                    {bpiScanning && <span className="download-log-stats">{bpiScanProgress?.current_file}</span>}
                  </div>
                  <div className="download-log-scroll" style={{height:'160px', overflow:'auto', padding:'8px 12px'}}>
                    {bpiScanLogs.map((line, i) => (
                      <div key={i} className="download-log-line"
                        style={{color: line.includes('Eroare') ? '#ef4444' : line.includes('finalizat') ? '#22c55e' : undefined}}>
                        {line}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          {bpiResults && (
            <Card data-testid="bpi-results-card">
              <CardHeader>
                <CardTitle className="card-title" style={{ fontSize: '1rem' }}>
                  {bpiResults.success
                    ? <><CheckCircle2 size={18} style={{ color: '#22c55e' }} /> {bpiResults.records_count} inregistrari extrase</>
                    : <><XCircle size={18} style={{ color: '#ef4444' }} /> Eroare parsare</>
                  }
                </CardTitle>
                <CardDescription>
                  {bpiResults.filename} &middot; {bpiResults.pages} pagini &middot; {bpiResults.text_length?.toLocaleString()} caractere
                </CardDescription>
              </CardHeader>
              <CardContent>
                {!bpiResults.success && (
                  <p style={{ color: '#ef4444', fontSize: '0.85rem' }}>{bpiResults.error}</p>
                )}

                {bpiResults.records?.length > 0 && (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                    {bpiResults.records.map((rec, i) => (
                      <div key={i} data-testid={`bpi-record-${i}`} style={{
                        border: '1px solid var(--border)', borderRadius: '8px',
                        overflow: 'hidden', background: 'var(--bg-secondary)'
                      }}>
                        <div style={{ padding: '10px 14px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px' }}
                          onClick={() => setExpandedIdx(expandedIdx === i ? null : i)}>
                          <ChevronDown size={14} style={{ transform: expandedIdx === i ? 'rotate(180deg)' : 'none', transition: '0.2s', flexShrink: 0 }} />
                          <div style={{ flex: 1 }}>
                            <div style={{ fontWeight: 600, fontSize: '0.88rem' }}>
                              {rec.denumire_firma || <em style={{ opacity: 0.5 }}>Firma neidentificata</em>}
                              {rec.cui && <span style={{ marginLeft: 8, fontSize: '0.75rem', color: 'var(--primary)' }}>CUI: {rec.cui}</span>}
                            </div>
                            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '2px' }}>
                              {rec.dosar && <span>Dosar: {rec.dosar} &middot; </span>}
                              {rec.tribunal && <span>{rec.tribunal}</span>}
                            </div>
                          </div>
                          {rec.tip_procedura && (
                            <Badge variant="outline" style={{ fontSize: '0.68rem', flexShrink: 0, ...tipBadgeStyle(rec.tip_procedura) }}>
                              {rec.tip_procedura}
                            </Badge>
                          )}
                          {rec.firma_match && (
                            <Badge variant="outline" style={{ fontSize: '0.68rem', background: 'rgba(34,197,94,0.15)', color: '#22c55e', flexShrink: 0 }}>
                              in DB
                            </Badge>
                          )}
                        </div>

                        {expandedIdx === i && (
                          <div style={{ padding: '0 14px 12px', borderTop: '1px solid var(--border)' }}>
                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px 16px', marginTop: '10px', fontSize: '0.8rem' }}>
                              {[
                                ['Firma', rec.denumire_firma],
                                ['CUI', rec.cui],
                                ['Nr. Reg. Com.', rec.nr_reg_com],
                                ['Tribunal', rec.tribunal],
                                ['Dosar', rec.dosar],
                                ['Tip Procedura', rec.tip_procedura],
                                ['Judecator Sindic', rec.judecator_sindic],
                                ['Administrator Judiciar', rec.administrator_judiciar],
                                ['Lichidator', rec.lichidator],
                                ['Data Publicare', rec.data_publicare],
                                ['Termen', rec.termen],
                                ['Adresa', rec.adresa],
                              ].filter(([, v]) => v).map(([label, value]) => (
                                <div key={label}>
                                  <span style={{ color: 'var(--text-muted)' }}>{label}: </span>
                                  <strong>{value}</strong>
                                </div>
                              ))}
                            </div>

                            {rec.firma_match && (
                              <div style={{ marginTop: '8px', padding: '8px 10px', background: 'rgba(34,197,94,0.08)', borderRadius: '6px', fontSize: '0.78rem' }}>
                                <span style={{ color: '#22c55e' }}>Firma gasita in DB: </span>
                                <button onClick={() => openFirmaProfile(rec.firma_match.id)}
                                  style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--primary)', textDecoration: 'underline', fontSize: '0.78rem' }}>
                                  {rec.firma_match.denumire} (CUI: {rec.firma_match.cui})
                                </button>
                              </div>
                            )}

                            <div style={{ display: 'flex', gap: '6px', marginTop: '10px' }}>
                              <Button size="sm" variant="outline" onClick={() => saveBpiRecord(rec)} data-testid={`save-bpi-${i}`}>
                                Salveaza in istoric
                              </Button>
                              {rec.firma_match && (
                                <Button size="sm" variant="outline" onClick={() => openFirmaProfile(rec.firma_match.id)}>
                                  <Building2 size={12} style={{ marginRight: 4 }} />
                                  Profil firma
                                </Button>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}

                {bpiResults.success && bpiResults.records_count === 0 && (
                  <div style={{ textAlign: 'center', padding: '20px', color: 'var(--text-muted)' }}>
                    <p>Nu s-au putut extrage date structurate.</p>
                    <details style={{ marginTop: '8px', fontSize: '0.78rem' }}>
                      <summary style={{ cursor: 'pointer' }}>Preview text extras</summary>
                      <pre style={{ marginTop: '8px', whiteSpace: 'pre-wrap', textAlign: 'left', fontSize: '0.72rem', maxHeight: '200px', overflow: 'auto' }}>
                        {bpiResults.raw_text_preview}
                      </pre>
                    </details>
                  </div>
                )}
              </CardContent>
            </Card>
          )}
        </div>

        <div>
          <Card>
            <CardHeader>
              <CardTitle className="card-title" style={{ fontSize: '1rem' }}>
                <Search size={18} />
                Istoric BPI Salvate
                {bpiStats && <span style={{ marginLeft: 8, fontSize: '0.78rem', color: 'var(--text-muted)', fontWeight: 400 }}>({bpiStats.total_records} total)</span>}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {bpiHistory.length === 0 ? (
                <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem', textAlign: 'center', padding: '20px' }}>
                  Niciun record salvat inca.<br />
                  Parseaza un PDF si salveaza inregistrarile.
                </p>
              ) : (
                <ScrollArea style={{ height: '600px' }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    {bpiHistory.map((rec, i) => (
                      <div key={i} style={{
                        padding: '10px 12px', border: '1px solid var(--border)',
                        borderRadius: '8px', background: 'var(--bg-secondary)', fontSize: '0.8rem'
                      }}>
                        <div style={{ fontWeight: 600, marginBottom: '3px' }}>
                          {rec.denumire_firma || <em style={{ opacity: 0.5 }}>Firma neidentificata</em>}
                        </div>
                        <div style={{ color: 'var(--text-muted)', display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
                          {rec.cui && <span>CUI: {rec.cui}</span>}
                          {rec.dosar && <span>Dosar: {rec.dosar}</span>}
                          {rec.tip_procedura && <Badge variant="outline" style={{ fontSize: '0.65rem' }}>{rec.tip_procedura}</Badge>}
                        </div>
                        {rec.saved_at && (
                          <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: '3px' }}>
                            {new Date(rec.saved_at).toLocaleString('ro-RO')}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </ScrollArea>
              )}
            </CardContent>
          </Card>
        </div>

      </div>
    </div>
  );
}
