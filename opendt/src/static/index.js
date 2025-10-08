const $ = s => document.querySelector(s);
const modeKey = 'opendt-view-mode';



function setMode(mode){
  document.body.classList.toggle('mode-json', mode === 'json');
  $('#btnModeUI').classList.toggle('active', mode === 'ui');
  $('#btnModeJSON').classList.toggle('active', mode === 'json');
  localStorage.setItem(modeKey, mode);
}

function fmt(n, d=1){ return (n===null||n===undefined||isNaN(n)) ? '—' : Number(n).toFixed(d); }
function pct(n, d=1){ return (n===null||n===undefined||isNaN(n)) ? '—' : (Number(n)*100).toFixed(d) + '%'; }

function applyStatus(status){
  const up = (status||'').toLowerCase() === 'running';
  const btn = $('#toggleBtn');
  btn.textContent = up ? '⏹ Stop' : '▶ Start';
  btn.className = 'btn ' + (up ? 'btn-danger' : 'btn-primary');

  const pill = $('#statusPill');
  pill.classList.toggle('running', up);
  pill.classList.toggle('stopped', !up);
  $('#statusText').textContent = (status || '—').toUpperCase();
}

async function toggleSystem(){
  const status = ($('#statusText').textContent || '').toUpperCase();
  const endpoint = (status === 'RUNNING' || status === 'STARTING') ? '/api/stop' : '/api/start';
  try{
    await fetch(endpoint, {method:'POST'});
    // polling will refresh UI
  }catch(e){}
}

function renderMetrics(s){
  $('#mCycles').textContent  = s.cycle_count ?? 0;
  $('#mTasks').textContent   = s.total_tasks ?? 0;

  const sim = s.last_simulation || {};
  $('#mEnergy').textContent  = fmt(sim.energy_kwh, 2);
  $('#mCPU').textContent     = pct(sim.cpu_utilization ?? 0, 1);
  $('#mRuntime').textContent = fmt(sim.runtime_hours, 1) + 'h';
  $('#mTopoUpdates').textContent = s.topology_updates ?? 0;

  const opt = s.last_optimization || {};
  const action = opt.action_taken || (Array.isArray(opt.action_type) ? opt.action_type[0] : null) || '—';
  $('#mLLMAction').textContent = action;
  const typ = (opt.type || '—').replace('_',' ');
  $('#mLLMType').innerHTML = typ !== '—' ? `<span class="badge">${typ}</span>` : '—';
}

function renderOpenDC(sim){
  $('#kvEnergy').textContent   = fmt(sim.energy_kwh, 2);
  $('#kvCPU').textContent      = pct(sim.cpu_utilization, 1);
  $('#kvRuntime').textContent  = fmt(sim.runtime_hours, 1);
  $('#kvMaxPower').textContent = fmt(sim.max_power_draw, 0);
  $('#kvSimType').textContent  = sim.status || '—';

  const pre = $('#simJSON');
  const txt = JSON.stringify(sim || null, null, 2);
  if (pre.textContent !== txt) pre.textContent = txt;
}

function renderLLM(opt){
  $('#kvLLMAction').textContent   = opt.action_taken || '—';
  $('#kvLLMReason').textContent   = opt.reason || '—';
  $('#kvLLMPriority').textContent = opt.priority || '—';
  $('#kvLLMType').textContent     = (opt.type || '—').replace('_',' ');

  // Recommendations list
  const list = $('#kvLLMRecs');
  const recs = opt.recommendations || opt.llm_recommendations || [];
  list.innerHTML = recs.length ? recs.map(r=>`<li>${r}</li>`).join('') : '<li>—</li>';

  const pre = $('#llmJSON');
  const txt = JSON.stringify(opt || null, null, 2);
  if (pre.textContent !== txt) pre.textContent = txt;
}

function renderTopoTable(topo){
  const body = $('#topoTable tbody');
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
  if (pre.textContent !== txt) pre.textContent = txt;
}

function renderBest(best){
  const scoreBadge = $('#bestScore');
  if (best && best.score !== undefined && best.score !== null){
    scoreBadge.textContent = `Score: ${Number(best.score).toFixed(2)}`;
  } else {
    scoreBadge.textContent = 'Score: —';
  }
  const pre = $('#bestJSON');
  const payload = best ? (best.config ? best : {config: best}) : null;
  const txt = JSON.stringify(payload, null, 2);
  if (pre.textContent !== txt) pre.textContent = txt;
}

async function poll(){
  try{
    const r = await fetch('/api/status', {cache:'no-store'});
    const s = await r.json();

    applyStatus(s.status);
    $('#windowInfo').textContent = s.current_window || '';
    renderMetrics(s);
    renderOpenDC(s.last_simulation || {});
    renderLLM(s.last_optimization || {});
    renderTopoTable(s.current_topology || null);
    renderBest(s.best_config || null);
  }catch(e){}
}

// init view mode
setMode(localStorage.getItem(modeKey) || 'ui');

// first paint & live refresh
poll();
setInterval(poll, 2000);


