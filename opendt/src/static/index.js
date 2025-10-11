const $ = s => document.querySelector(s);
const modeKey = 'opendt-view-mode';

// ---- helpers -------------------------------------------------
function fmt(n, d=1){ return (n===null||n===undefined||isNaN(n)) ? '—' : Number(n).toFixed(d); }
function pct(n, d=1){ return (n===null||n===undefined||isNaN(n)) ? '—' : (Number(n)*100).toFixed(d) + '%'; }

// Parse "Window 1: 121 tasks, 1155 fragments"
function parseWindowCounts(info){
  const m = /:\s*([\d,]+)\s*tasks?\s*,\s*([\d,]+)\s*fragments?/i.exec(info || "");
  if (!m) return { tasks: null, frags: null };
  const toInt = s => parseInt(String(s).replace(/,/g,''), 10);
  return { tasks: toInt(m[1]), frags: toInt(m[2]) };
}

// ---- UI mode / button ripple --------------------------------
document.addEventListener('click', e => {
  const b = e.target.closest('.btn');
  if (!b) return;
  const r = b.getBoundingClientRect();
  b.style.setProperty('--press-x', (e.clientX - r.left) + 'px');
  b.style.setProperty('--press-y', (e.clientY - r.top) + 'px');
});

function setMode(mode){
  document.body.classList.toggle('mode-json', mode === 'json');
  $('#btnModeUI')?.classList.toggle('active', mode === 'ui');
  $('#btnModeJSON')?.classList.toggle('active', mode === 'json');
  localStorage.setItem(modeKey, mode);
}

function applyStatus(status){
  const up = (status||'').toLowerCase() === 'running';
  const btn = $('#toggleBtn');
  if (btn){
    btn.className = 'btn ' + (up ? 'btn-danger' : 'btn-primary');
    btn.innerHTML = up
      ? `<span class="icon" style="--icon-url:var(--sym-stop)"></span>&nbsp;Stop`
      : `<span class="icon" style="--icon-url:var(--sym-play)"></span>&nbsp;Start`;
  }
  const pill = $('#statusPill');
  pill?.classList.toggle('running', up);
  pill?.classList.toggle('stopped', !up);
  $('#statusText') && ($('#statusText').textContent = (status || '—').toUpperCase());
}

async function toggleSystem(){
  const status = ($('#statusText')?.textContent || '').toUpperCase();
  const endpoint = (status === 'RUNNING' || status === 'STARTING') ? '/api/stop' : '/api/start';
  try{ await fetch(endpoint, {method:'POST'}); }catch(e){}
}

// ---- Renders -------------------------------------------------
function renderMetrics(s){
  const { tasks, frags } = parseWindowCounts(s.current_window);
  $('#mCycles')      && ($('#mCycles').textContent = s.cycle_count ?? 0);
  $('#mTasks')       && ($('#mTasks').textContent  = (tasks ?? s.total_tasks ?? 0));
  $('#mFragments')   && ($('#mFragments').textContent = (frags ?? s.total_fragments ?? 0));

  const sim = s.last_simulation || {};
  $('#mEnergy')      && ($('#mEnergy').textContent  = fmt(sim.energy_kwh, 2));
  $('#mCPU')         && ($('#mCPU').textContent     = pct(sim.cpu_utilization ?? 0, 1));
  $('#mRuntime')     && ($('#mRuntime').textContent = fmt(sim.runtime_hours, 1) + 'h');
  $('#mTopoUpdates') && ($('#mTopoUpdates').textContent = s.topology_updates ?? 0);

  const opt = s.last_optimization || {};
  const action = opt.action_taken || (Array.isArray(opt.action_type) ? opt.action_type[0] : null) || '—';
  $('#mLLMAction') && ($('#mLLMAction').textContent = action);
  const typ = (opt.type || '—').replace('_',' ');
  const llmTypeEl = $('#mLLMType');
  if (llmTypeEl) llmTypeEl.innerHTML = typ !== '—' ? `<span class="badge">${typ}</span>` : '—';
}

function renderOpenDC(sim,s){
  $('#kvEnergy')   && ($('#kvEnergy').textContent   = fmt(sim.energy_kwh, 2));
  $('#kvCPU')      && ($('#kvCPU').textContent      = pct(sim.cpu_utilization, 1));
  $('#kvRuntime')  && ($('#kvRuntime').textContent  = fmt(sim.runtime_hours, 1));
  $('#kvMaxPower') && ($('#kvMaxPower').textContent = fmt(sim.max_power_draw, 0));
  $('#kvSimType')  && ($('#kvSimType').textContent  = fmt(s.cycle_count_opt) ?? '—');

  const pre = $('#simJSON');
  const txt = JSON.stringify(sim || null, null, 2);
  if (pre && pre.textContent !== txt) pre.textContent = txt;
}

function renderLLM(opt){
  $('#kvLLMAction')   && ($('#kvLLMAction').textContent = opt.action_taken || '—');
  $('#kvLLMReason')   && ($('#kvLLMReason').textContent = opt.reason || '—');
  $('#kvLLMPriority') && ($('#kvLLMPriority').textContent = opt.priority || '—');
  $('#kvLLMType')     && ($('#kvLLMType').textContent     = (opt.type || '—').replace('_',' '));

  const list = $('#kvLLMRecs');
  const recs = opt.recommendations || opt.llm_recommendations || [];
  if (list) list.innerHTML = recs.length ? recs.map(r=>`<li>${r}</li>`).join('') : '<li>—</li>';

  const pre = $('#llmJSON');
  const txt = JSON.stringify(opt || null, null, 2);
  if (pre && pre.textContent !== txt) pre.textContent = txt;
}

function renderTopoTable(topo){
  const body = $('#topoTable tbody');
  if (!body) return;
  const rows = [];
  try{
    const clusters = (topo && topo.clusters) || [];
    clusters.forEach(c=>{
      (c.hosts || []).forEach(h=>{
        rows.push(`
          <tr>
            <td>${c.name || '—'}</td>
            <td>${h.name || '—'}</td>
            <td>${h.count ?? '—'}</td>
            <td>${h.cpu?.coreCount ?? '—'}</td>
            <td>${h.cpu?.coreSpeed ?? '—'}</td>
            <td>${Math.round((h.memory?.memorySize || 0)/1073741824) || '—'}</td>
          </tr>
        `);
      });
    });
  }catch(e){}
  body.innerHTML = rows.length ? rows.join('') : '<tr><td colspan="6">No topology</td></tr>';

  const pre = $('#topologyJSON');
  const txt = JSON.stringify(topo || null, null, 2);
  if (pre && pre.textContent !== txt) pre.textContent = txt;
}

function renderBest(best){
  const badge = $('#bestScore');
  if (badge){
    if (best && best.score !== undefined && best.score !== null){
      badge.textContent = `Score: ${Number(best.score).toFixed(2)}`;
    } else {
      badge.textContent = 'Score: —';
    }
  }
  const pre = $('#bestJSON');
  const payload = best ? (best.config ? best : {config: best}) : null;
  const txt = JSON.stringify(payload, null, 2);
  if (pre && pre.textContent !== txt) pre.textContent = txt;
}

// ---- Recommendation Action -------------------------------------------------
async function acceptRecommendation() {
  try {
    const btn = $('#btnAcceptRec');
    if (btn) btn.disabled = true;
    
    const response = await fetch('/api/accept_recommendation', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    // Refresh the UI
    await poll();
  } catch(e) {
    console.error('Failed to accept recommendation:', e);
  } finally {
    const btn = $('#btnAcceptRec');
    if (btn) btn.disabled = false;
  }
}

// ---- polling -------------------------------------------------
async function poll(){
  try{
    const r = await fetch('/api/status', {cache:'no-store'});
    const s = await r.json();

    applyStatus(s.status);

    // NEW: right-side window pill text
    const wp = document.querySelector('#windowPill');
    if (wp) {
      const n = s.cycle_count ?? null;                 // window number
      const info = s.current_window || '';             // "Window 1: …"
      wp.textContent = n ? `Window ${n}` : (info || '—');
    }

    renderMetrics(s);
    renderOpenDC(s.last_optimization || {},s);
    renderLLM(s.last_optimization || {});
    renderTopoTable(s.current_topology || null);
    renderBest(s.best_config || null);
  }catch(e){}
}

setMode(localStorage.getItem(modeKey) || 'ui');
poll();
setInterval(poll, 2000);

// ---- SLO submit handler -------------------------------------------------
async function submitSLO() {
  const btn = document.getElementById('submit_slo');
  if (!btn) return;

  try {
    const energy = parseFloat(document.getElementById('energy_input').value);
    const runtime = parseFloat(document.getElementById('runtime_input').value);
    
    // Validate inputs
    if (isNaN(energy) || isNaN(runtime)) {
      console.error('Invalid input values');
      btn.classList.add('btn-danger');
      setTimeout(() => {
        btn.classList.remove('btn-danger');
        btn.classList.add('btn-primary');
      }, 1000);
      return;
    }

    // Disable button and store class
    btn.disabled = true;
    btn.classList.remove('btn-primary');
    btn.classList.add('btn-ghost');

    const response = await fetch('/api/set_slo', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        energy_target: energy,
        runtime_target: runtime
      })
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    // Update pill sigils
    const energySigil = document.querySelector('#energy_input').closest('.pill-input').querySelector('.pill-sigil');
    const runtimeSigil = document.querySelector('#runtime_input').closest('.pill-input').querySelector('.pill-sigil');
    if (energySigil) energySigil.textContent = fmt(energy, 2);
    if (runtimeSigil) runtimeSigil.textContent = fmt(runtime, 1);

    // Success feedback
    btn.classList.remove('btn-ghost');
    btn.classList.add('btn-success');
    setTimeout(() => {
      btn.classList.remove('btn-success');
      btn.classList.add('btn-primary');
    }, 1000);

  } catch (error) {
    console.error('Failed to submit SLO:', error);
    btn.classList.remove('btn-ghost');
    btn.classList.add('btn-danger');
    setTimeout(() => {
      btn.classList.remove('btn-danger');
      btn.classList.add('btn-primary');
    }, 1000);
  } finally {
    btn.disabled = false;
  }
}
