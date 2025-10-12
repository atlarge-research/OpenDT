// polling.js

async function poll(){
  try{
    const r = await fetch('/api/status', {cache:'no-store'});
    const s = await r.json();

    applyStatus(s.status);

    // right-side window pill text
    const wp = document.querySelector('#windowPill');
    if (wp) {
      const n = s.cycle_count ?? null;
      const info = s.current_window || '';
      wp.textContent = n ? `Window ${n}` : (info || '—');
    }

    renderMetrics(s);
    renderOpenDC(s.last_optimization || {}, s);
    renderLLM(s.last_optimization || {});
    renderTopoTable(s.current_topology || null);
    renderBest(s.best_config || null);
  }catch(e){}
}
window.poll = poll;
