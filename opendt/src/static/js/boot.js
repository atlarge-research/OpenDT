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

// also keep status/metrics polling every 2s
setInterval(poll, 2000);
