import { Button } from "../components/ui/button";
import { Loader2, Shield, XCircle, CheckCircle2, RefreshCw } from "lucide-react";

export default function CaptchaModal({ ctx }) {
  const {
    captchaModalOpen, captchaLoading, captchaImageUrl,
    captchaCode, setCaptchaCode, captchaError,
    closeCaptchaModal, submitCaptcha, openCaptchaModal,
  } = ctx;

  if (!captchaModalOpen) return null;

  return (
    <div className="modal-overlay" onClick={closeCaptchaModal}>
      <div className="captcha-modal" onClick={e => e.stopPropagation()}>
        <div className="captcha-modal-header">
          <h3>
            <Shield size={20} />
            Rezolvă CAPTCHA - MFinante
          </h3>
          <button className="close-btn" onClick={closeCaptchaModal}>
            <XCircle size={20} />
          </button>
        </div>
        
        <div className="captcha-modal-body">
          {captchaLoading ? (
            <div className="captcha-loading">
              <Loader2 className="animate-spin" size={32} />
              <p>Se încarcă CAPTCHA...</p>
            </div>
          ) : captchaImageUrl ? (
            <>
              <div className="captcha-image-container">
                <img 
                  src={captchaImageUrl} 
                  alt="CAPTCHA" 
                  className="captcha-image"
                  onError={() => setCaptchaError("Nu s-a putut încărca imaginea CAPTCHA")}
                />
                <button 
                  className="refresh-captcha-btn" 
                  onClick={() => {
                    // Reinitialize to get a completely new session and image
                    openCaptchaModal();
                  }}
                  title="Generează un nou CAPTCHA"
                >
                  <RefreshCw size={16} />
                </button>
              </div>
              
              <div className="captcha-input-section">
                <label htmlFor="captcha-input">Introdu codul EXACT din imagine (atenție la majuscule/minuscule):</label>
                <input
                  id="captcha-input"
                  type="text"
                  value={captchaCode}
                  onChange={(e) => setCaptchaCode(e.target.value)}
                  placeholder="Ex: aB12cD"
                  className="captcha-input"
                  autoFocus
                  onKeyPress={(e) => e.key === 'Enter' && submitCaptcha()}
                />
                <span className="captcha-hint-text">Codul este case-sensitive (diferență între litere mari/mici)</span>
              </div>
              
              {captchaError && (
                <div className="captcha-error">
                  <XCircle size={16} />
                  {captchaError}
                </div>
              )}
            </>
          ) : (
            <div className="captcha-error">
              <XCircle size={16} />
              {captchaError || "Eroare la încărcarea CAPTCHA"}
            </div>
          )}
        </div>
        
        <div className="captcha-modal-footer">
          <Button variant="outline" onClick={closeCaptchaModal}>
            Anulează
          </Button>
          <Button 
            onClick={submitCaptcha} 
            disabled={captchaLoading || !captchaCode}
          >
            {captchaLoading ? (
              <Loader2 className="animate-spin" size={16} />
            ) : (
              <CheckCircle2 size={16} />
            )}
            Verifică CAPTCHA
          </Button>
        </div>
      </div>
    </div>
  );
}
