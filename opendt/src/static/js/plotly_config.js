// plotly_config.js
const PLOTLY_CONFIG = {
  responsive: true,
  displaylogo: false,
  displayModeBar: false,
  showTips: false,
  toImageButtonOptions: { format:'png', filename:'opendt-chart', height:720, width:1280, scale:2 }
};

// Theme (auto: body has .mode-json for dark)
function getPlotTheme() {
  const dark = document.body.classList.contains('mode-json') ||
               window.matchMedia('(prefers-color-scheme: dark)').matches;
  return dark
    ? { primary:'#ff7f0e', grid:'rgba(255,255,255,0.15)', axis:'#9ca3af', font:'#e5e7eb' }
    : { primary:'#ff7f0e', grid:'rgba(0,0,0,0.08)',  axis:'#94a3b8', font:'#334155' };
}

// Keep zoom on re-render
let UIREVISION = 'persist-zoom';

function layoutFor(title, opts = {}) {
  const { yTickformat = null, yAxisTitle = null, yRange = null } = opts;
  const t = getPlotTheme();
  return {
    title,
    uirevision: UIREVISION,
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor:  'rgba(0,0,0,0)',
    margin: { l: 70, r: 40, t: 60, b: 70 },
    font: { color: t.font, family: 'Arial, sans-serif' },
    showlegend: false,
    xaxis: {
      title: { text: 'Time', standoff: 8 },
      type: 'date',
      showgrid: true,  gridcolor: t.grid,
      showline: true,  linecolor: t.axis, linewidth: 1.2,
      ticks: 'outside', tickcolor: t.axis, ticklen: 6,
      rangeslider: { visible: false }
    },
    yaxis: {
      title: { text: yAxisTitle || '', standoff: 8 },
      showgrid: true,  gridcolor: t.grid,
      showline: true,  linecolor: t.axis, linewidth: 1.2,
      ticks: 'outside', tickcolor: t.axis, ticklen: 6,
      tickformat: yTickformat || undefined,
      rangemode: 'tozero',
      range: yRange || undefined
    }
  };
}
