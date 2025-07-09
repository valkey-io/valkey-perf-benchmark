// app.js - Plain JS dashboard using Chart.js

const COMPLETED_URL = "../completed_commits.json";
const RESULT_URL = sha => `../results/${sha}/metrics.json`;
const COMMIT_URL = sha => `https://github.com/valkey-io/valkey/commit/${sha}`;

async function fetchJSON(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error(`${url}: ${r.status}`);
  return r.json();
}

const state = {
  commits: [],       // ordered list of shas
  commitTimes: {},   // sha -> timestamp
  metrics: [],       // raw metric rows
  cluster: 'all',
  tls: 'all',
  metricKey: 'rps',
  pipeline: 'all',
  dataSize: 'all',
  selectedCommands: new Set(),
  fromDate: '',
  toDate: ''
};

// DOM helpers --------------------------------------------------------------
function el(tag, attrs={}, children=[]){
  const e = document.createElement(tag);
  Object.entries(attrs).forEach(([k,v])=>{
    if(k.startsWith('on') && typeof v === 'function'){
      e.addEventListener(k.slice(2).toLowerCase(), v);
    } else if(k==='class') {
      e.className = v;
    } else {
      e.setAttribute(k,v);
    }
  });
  children.forEach(c=>{ if(typeof c==='string') e.appendChild(document.createTextNode(c));
                       else if(c) e.appendChild(c); });
  return e;
}

function buildControls(){
  const controls = el('div', {class:'flex flex-wrap gap-4 justify-center text-center mb-4'});
  const selectCluster = el('select', {class:'border rounded p-1 ml-2',
    onchange:e=>{state.cluster=e.target.value; updateChart();}},
    ['all','true','false'].map(o=>el('option',{value:o},[o])));
  const selectTLS = el('select', {class:'border rounded p-1 ml-2',
    onchange:e=>{state.tls=e.target.value; updateChart();}},
    ['all','true','false'].map(o=>el('option',{value:o},[o])));
  const selectPipeline = el('select', {class:'border rounded p-1 ml-2'});
  selectPipeline.addEventListener('change', e=>{state.pipeline=e.target.value; updateChart();});
  const selectSize = el('select', {class:'border rounded p-1 ml-2'});
  selectSize.addEventListener('change', e=>{state.dataSize=e.target.value; updateChart();});
  const selectMetric = el('select', {class:'border rounded p-1 ml-2',
    onchange:e=>{state.metricKey=e.target.value; updateChart();}},
    ['rps','avg_latency_ms','p95_latency_ms','p99_latency_ms'].map(o=>el('option',{value:o},[o])));
  const inputFrom = el('input', {type:'date', class:'border rounded p-1 ml-2',
    onchange:e=>{state.fromDate=e.target.value; updateChart();}});
  const inputTo = el('input', {type:'date', class:'border rounded p-1 ml-2',
    onchange:e=>{state.toDate=e.target.value; updateChart();}});

  controls.appendChild(el('label',{class:'font-medium inline-flex items-center'},['Cluster:',selectCluster]));
  controls.appendChild(el('label',{class:'font-medium inline-flex items-center'},['TLS:',selectTLS]));
  controls.appendChild(el('label',{class:'font-medium inline-flex items-center'},['Pipeline:',selectPipeline]));
  controls.appendChild(el('label',{class:'font-medium inline-flex items-center'},['Data Size:',selectSize]));
  controls.appendChild(el('label',{class:'font-medium inline-flex items-center'},['Metric:',selectMetric]));
  controls.appendChild(el('label',{class:'font-medium inline-flex items-center'},['From:',inputFrom]));
  controls.appendChild(el('label',{class:'font-medium inline-flex items-center'},['To:',inputTo]));

  return {controls, selectPipeline, selectSize, inputFrom, inputTo};
}

function buildCommandChecks(commands){
  const wrap = el('div', {class:'flex flex-wrap gap-2 justify-center text-center mb-4'});
  commands.forEach(cmd=>{
    const cb = el('input',{type:'checkbox',class:'mr-1',checked:true});
    cb.addEventListener('change',()=>{
      if(cb.checked) state.selectedCommands.add(cmd);
      else state.selectedCommands.delete(cmd);
      updateChart();
    });
    state.selectedCommands.add(cmd);
    wrap.appendChild(el('label',{class:'flex items-center'},[cb,cmd]));
  });
  return wrap;
}

let chart;

function updateChart(){
  if(!state.metrics.length) return;
  // filter commits by date range
  const allowedShas = state.commits.filter(sha=>{
    const ts = state.commitTimes[sha];
    if(!ts) return false;
    if(state.fromDate && new Date(ts) < new Date(state.fromDate)) return false;
    if(state.toDate && new Date(ts) > new Date(state.toDate)) return false;
    return true;
  });

  const labels = allowedShas.map(s=>s.slice(0,8));
  const datasets = [];
  const commands = Array.from(state.selectedCommands);
  commands.forEach((cmd,i)=>{
    const rows = state.metrics.filter(r=>
      r.command===cmd &&
      (state.cluster==='all' || r.cluster_mode === (state.cluster==='true')) &&
      (state.tls==='all'     || r.tls          === (state.tls==='true')) &&
      (state.pipeline==='all'|| r.pipeline     === Number(state.pipeline)) &&
      (state.dataSize==='all'|| r.data_size    === Number(state.dataSize))
    );
    const data = allowedShas.map(sha=>{
      const row = rows.find(r=>r.sha===sha);
      return row ? row[state.metricKey] : null;
    });
    const color = `hsl(${(i*50)%360},70%,50%)`;
    datasets.push({label:cmd,data,borderColor:color,backgroundColor:color,fill:false});
  });

  const ctx = document.getElementById('chartCanvas').getContext('2d');
  if(chart) chart.destroy();
  chart = new Chart(ctx, {
    type:'line',
    data:{labels,datasets},
    options:{responsive:true,interaction:{mode:'index',intersect:false},scales:{x:{display:true},y:{display:true}}}
  });
}

async function init(){
  const root = document.getElementById('chartRoot');
  const {controls,selectPipeline,selectSize,inputFrom,inputTo} = buildControls();
  root.appendChild(controls);
  const canvas = el('canvas',{id:'chartCanvas',class:'bg-white rounded shadow p-2 w-full max-w-4xl'});
  root.appendChild(canvas);

  try {
    const raw = await fetchJSON(COMPLETED_URL);
    const shas=[]; const times={};
    raw.forEach(c=>{
      if(typeof c==='object' && c.status==='in_progress') return;
      const sha = typeof c==='string'? c : (c.sha||c.commit||c.full);
      if(!sha) return;
      shas.push(sha);
      if(c.timestamp) times[sha]=c.timestamp;
    });
    state.commits=shas; state.commitTimes=times;
  } catch(err){ console.error('Failed to load commit list',err); return; }

  try {
    const all=[]; const times={...state.commitTimes};
    await Promise.all(state.commits.map(async sha=>{
      try {
        const rows=await fetchJSON(RESULT_URL(sha));
        rows.forEach(r=>all.push({...r,sha}));
        if(rows[0]&&rows[0].timestamp&&!times[sha]) times[sha]=rows[0].timestamp;
      } catch(err){ console.error('Failed to load metrics for',sha,err); }
    }));
    state.metrics=all; state.commitTimes=times;
    // update pipelines and data sizes
    const pipelines=[...new Set(all.map(r=>r.pipeline))].sort((a,b)=>a-b);
    pipelines.forEach(p=>selectPipeline.appendChild(el('option',{value:String(p)},[String(p)])));
    const sizes=[...new Set(all.map(r=>r.data_size))].sort((a,b)=>a-b);
    sizes.forEach(s=>selectSize.appendChild(el('option',{value:String(s)},[String(s)])));
    const cmds=[...new Set(all.map(r=>r.command))].sort();
    const cmdChecks=buildCommandChecks(cmds);
    root.insertBefore(cmdChecks, canvas);
    if(Object.values(times).length){
      const min=new Date(Math.min(...Object.values(times)));
      const max=new Date(Math.max(...Object.values(times)));
      inputFrom.value=min.toISOString().slice(0,10);
      inputTo.value=max.toISOString().slice(0,10);
      state.fromDate=inputFrom.value;
      state.toDate=inputTo.value;
    }
    updateChart();
  } catch(err){ console.error('Failed to load metrics',err); }
}

document.addEventListener('DOMContentLoaded', init);
