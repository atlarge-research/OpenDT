// CPU
Plotly.react('chart-cpu', [{
  x: timePoints,
  y: data,                      // assumed 0..1; if you send 0..100 use data.map(v=>v/100)
  mode: 'lines',
  showlegend: false,
  line: { color: getPlotTheme().primary, width: 2.5 }
}], layoutFor('CPU — Real vs Sim', {
  yTickformat: ',.0%',
  yAxisTitle: 'Utilization [%]',
  yRange: [0, 1]                // lock to 0..100%
}), PLOTLY_CONFIG);

// Power
Plotly.react('chart-power', [{
  x: timePoints,
  y: data,
  mode: 'lines',
  showlegend: false,
  line: { color: getPlotTheme().primary, width: 2.5 }
}], layoutFor('Power — Real vs Sim', { yAxisTitle: 'Power [W]' }), PLOTLY_CONFIG);

// Energy
Plotly.react('chart-energy', [{
  x: timePoints,
  y: data,
  mode: 'lines',
  showlegend: false,
  line: { color: getPlotTheme().primary, width: 2.5 }
}], layoutFor('Cumulative Energy', { yAxisTitle: 'Energy [kWh]' }), PLOTLY_CONFIG);
