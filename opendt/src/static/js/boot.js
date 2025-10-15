// boot.js

setMode(localStorage.getItem(modeKey) || 'ui');
poll();

// boot charts + sse and periodic refresh
document.addEventListener('DOMContentLoaded', ()=>{
  drawCharts();
  startSse();
  // safety poll every 5s
  setInterval(()=>{ drawCharts().catch(()=>{}); }, 5000);
});

document.addEventListener('DOMContentLoaded', () => {
  const energy  = document.getElementById('energy_input');
  const runtime = document.getElementById('runtime_input');
  if (!energy || !runtime || typeof submitSLO !== 'function') return;

  const debounce = (fn, ms=300) => {
    let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
  };
  const trigger = debounce(() => submitSLO());

  ['change','input'].forEach(evt => {
    energy.addEventListener(evt, trigger);
    runtime.addEventListener(evt, trigger);
  });
});


// also keep status/metrics polling every 2s
setInterval(poll, 2000);
