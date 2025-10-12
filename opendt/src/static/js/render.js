// render.js

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

  // round the score to no decimal, but just full number, say 30, 21
  if (badge){
    if (best && best.score !== undefined && best.score !== null){
      badge.textContent = `Score: ${Number(best.score).toFixed(0)}`;

    } else {
      badge.textContent = 'Score: —';
    }
  }
  const pre = $('#bestJSON');
  const payload = best ? (best.config ? best : {config: best}) : null;
  const txt = JSON.stringify(payload, null, 2);
  if (pre && pre.textContent !== txt) pre.textContent = txt;
}

window.renderMetrics   = renderMetrics;
window.renderOpenDC    = renderOpenDC;
window.renderLLM       = renderLLM;
window.renderTopoTable = renderTopoTable;
window.renderBest      = renderBest;
