"""
未入力・検証エラーのフィールド検出ユーティリティ（Playwright用・軽量）

目的:
- 送信後に画面上で HTML5/ARIA ベースの検証エラーまたは未入力必須を検出し、
  セレクタ・入力型・簡易ヒントを返す。

注意:
- ログや返却値に機微データ（入力値/URL/企業名等）は含めない。
- 呼び出し側で必要に応じてサニタイズ/短縮して利用すること。
"""

from typing import List, Dict, Any

from playwright.async_api import Page


async def detect_invalid_required_fields(page: Page) -> List[Dict[str, Any]]:
    """
    ページ上の未入力/検証エラーのフォーム要素を検出する。

    Returns:
        List[Dict]:
            - selector: 推奨セレクタ（id>name>型つきname>タグの優先）
            - input_type: 正規化した入力タイプ(text/email/tel/url/textarea/select/checkbox/radio)
            - hint: ラベル/placeholder等の短いテキスト（先頭30文字）
            - reason: validationMessage もしくは簡易理由(required/aria-invalid)
            - meta: name/id/class の一部（値は含めない）
            - select_first_option: select時の先頭有効オプション名（あれば）
    """
    try:
        results = await page.evaluate(
            """
            () => {
              const OUT = [];
              const ctrls = Array.from(document.querySelectorAll('form input, form textarea, form select'));
              const isVisible = (el) => !!(el.offsetParent) && getComputedStyle(el).visibility !== 'hidden';
              const buildSelector = (el) => {
                const id = el.getAttribute('id');
                if (id) return `#${CSS.escape(id)}`;
                const name = el.getAttribute('name');
                const tag = (el.tagName||'').toLowerCase() || 'input';
                const typ = (el.getAttribute('type')||'').toLowerCase();
                if (name && typ) return `${tag}[name="${name}"][type="${typ}"]`;
                if (name) return `${tag}[name="${name}"]`;
                // 最低限: タグのみ（衝突の可能性あり）
                return tag;
              };
              const labelTextFor = (el) => {
                try{
                  const id = el.getAttribute('id');
                  if (id) {
                    const l = document.querySelector(`label[for="${CSS.escape(id)}"]`);
                    if (l && l.innerText) return l.innerText.trim();
                  }
                }catch(e){}
                try{
                  const parentLabel = el.closest('label');
                  if (parentLabel && parentLabel.innerText) return parentLabel.innerText.trim();
                }catch(e){}
                const ph = el.getAttribute('placeholder') || '';
                const aria = el.getAttribute('aria-label') || '';
                const nm = el.getAttribute('name') || '';
                const id2 = el.getAttribute('id') || '';
                return (ph || aria || nm || id2 || (el.tagName||'')).toString().trim();
              };
              const normalizeType = (el) => {
                const tag = (el.tagName||'').toLowerCase();
                if (tag === 'textarea') return 'textarea';
                if (tag === 'select') return 'select';
                const typ = (el.getAttribute('type')||'text').toLowerCase();
                if (['email','tel','url','radio','checkbox','password','number'].includes(typ)) return typ;
                return 'text';
              };
              const isInvalid = (el) => {
                try{ if (typeof el.checkValidity === 'function' && !el.checkValidity()) return true; }catch(e){}
                const ariaInv = (el.getAttribute('aria-invalid')||'').toLowerCase() === 'true';
                if (ariaInv) return true;
                const req = el.hasAttribute('required') || (el.getAttribute('aria-required')||'').toLowerCase() === 'true';
                if (req){
                  const tag = (el.tagName||'').toLowerCase();
                  if (tag === 'input' && (el.type==='checkbox' || el.type==='radio')) {
                    if (!el.checked) return true;
                  } else if (tag === 'select') {
                    const v = (el.value||'').trim();
                    if (!v) return true;
                  } else {
                    const v = (el.value||'').trim();
                    if (!v) return true;
                  }
                }
                return false;
              };
              const firstValidOption = (el) => {
                try{
                  const toks = ['選択してください','選択して下さい','お選びください','please select','--','-','none','なし','未選択'];
                  const opts = Array.from(el.options||[]);
                  for (const o of opts){
                    const txt = (o.textContent||'').trim();
                    const val = (o.value||'').trim();
                    if (!val && !txt) continue;
                    const low = (txt||'').toLowerCase();
                    if (toks.some(t => low.includes(t))) continue;
                    return {value: val, text: txt};
                  }
                }catch(e){}
                return null;
              };

              for (const el of ctrls){
                try{
                  if (!isVisible(el) || el.disabled) continue;
                  if (!isInvalid(el)) continue;
                  const typ = normalizeType(el);
                  const reason = (el.validationMessage||'').trim() || (el.getAttribute('aria-invalid')==='true' ? 'aria-invalid' : 'required');
                  const hint = (labelTextFor(el)||'').slice(0,30);
                  const selector = buildSelector(el);
                  const meta = {
                    name: el.getAttribute('name')||'',
                    id: el.getAttribute('id')||'',
                    class: el.getAttribute('class')||''
                  };
                  const out = { selector, input_type: typ, hint, reason, meta };
                  if (typ === 'select'){
                    const fo = firstValidOption(el);
                    if (fo) out['select_first_option'] = fo;
                  }
                  OUT.push(out);
                }catch(e){/* ignore */}
              }
              return OUT;
            }
            """
        )
        return results or []
    except Exception:
        return []

