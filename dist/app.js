const state = { data: null, filter: "all", selected: null, seconds: 300 };
const labels = {
  long: "顺势做多", short: "顺势做空", watch_long: "观察偏多",
  watch_short: "观察偏空", neutral: "多空分歧", missing: "数据缺失"
};
const timeframeLabels = {
  strong_long: "强势多头", long: "偏多", strong_short: "强势空头",
  short: "偏空", watch_long: "偏多", watch_short: "偏空", neutral: "中性"
};
const colors = { long:"#2ee68a", short:"#ff5c72", watch_long:"#f6bd46", watch_short:"#f09454", neutral:"#718097", missing:"#8390a2" };
const factorNames = { momentum:"5日动量", breakout:"20日突破", trend:"MA20趋势", rsi:"RSI14区间" };
const fmt = (value, digits=1) => value == null ? "—" : Number(value).toLocaleString("zh-CN", {minimumFractionDigits:digits, maximumFractionDigits:digits});
const pct = value => value == null ? "—" : `${value >= 0 ? "+" : ""}${fmt(value, 2)}%`;
const escapeHtml = value => String(value ?? "").replace(/[&<>'"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"}[c]));

function signalSide(signal){
  return signal === "long" || signal === "watch_long" ? "long" : signal === "short" || signal === "watch_short" ? "short" : signal;
}
function filteredAssets(){
  if(!state.data) return [];
  return state.data.assets.filter(asset => state.filter === "all" ||
    (state.filter === "watch" ? asset.signal.startsWith("watch") : signalSide(asset.signal) === state.filter));
}
function timeframeClass(signal){
  if(signal.includes("long")) return "tf-long";
  if(signal.includes("short")) return "tf-short";
  return "tf-neutral";
}
function timeframePill(prefix, item){
  return `<span class="${timeframeClass(item.signal)}">${prefix}·${timeframeLabels[item.signal] || labels[item.signal]}</span>`;
}
function cardTemplate(asset){
  const color = colors[asset.signal] || colors.neutral;
  if(asset.status === "missing") return `<article class="card missing ${state.selected===asset.id?'selected':''}" data-id="${asset.id}">
    <div class="card-top"><div><span class="contract">${escapeHtml(asset.name)}</span><span class="symbol">${asset.symbol}</span><small class="exchange">${asset.exchange}</small></div><span class="signal-pill">数据缺失</span></div>
    <div class="error-text">${escapeHtml(asset.error || "等待行情接入")}</div><div class="card-foot"><span>${escapeHtml(asset.source)}</span><span>—</span></div></article>`;
  const factors = asset.daily_score >= 0 ? asset.long_factors : asset.short_factors;
  const statusLabel = asset.status === "closed" ? "休市 · " : asset.status === "stale" ? "陈旧 · " : "";
  return `<article class="card ${state.selected===asset.id?'selected':''}" data-id="${asset.id}" style="--signal-color:${color}">
    <div class="card-top"><div><span class="contract">${asset.name}</span><span class="symbol">${asset.symbol}</span><small class="exchange">${asset.exchange}</small></div><span class="signal-pill">${labels[asset.signal]}</span></div>
    <div class="price-row"><span class="price">${fmt(asset.price, asset.decimals)}</span><span class="change">${pct(asset.change_pct)}</span></div>
    <div class="timeframe-pills">${timeframePill("周",asset.timeframes.week)}${timeframePill("日",asset.timeframes.day)}${timeframePill("时",asset.timeframes.hour)}</div>
    <div class="factor-pills">${Object.entries(factors).map(([key,on])=>`<span class="${on?'on':''}">${factorNames[key]} ${on?'✓':'×'}</span>`).join('')}</div>
    <div class="card-foot"><span>${statusLabel}${asset.bar_time.slice(5)}</span><span>日线 ${asset.long_count}/${asset.short_count}</span></div>
  </article>`;
}
function renderCards(){
  const assets = filteredAssets();
  document.querySelector("#cards").innerHTML = assets.length ? assets.map(cardTemplate).join("") : `<div class="empty-detail">当前筛选没有品种</div>`;
  document.querySelectorAll(".card").forEach(card => card.addEventListener("click", () => selectAsset(card.dataset.id)));
}
function sparkPoints(points){
  if(!points || points.length < 2) return "";
  const values=points.map(point=>point.value), min=Math.min(...values), max=Math.max(...values), range=max-min || 1;
  return values.map((value,index)=>`${(index/(values.length-1)*100).toFixed(2)},${(92-(value-min)/range*82).toFixed(2)}`).join(" ");
}
function bindSpark(asset){
  const wrap=document.querySelector("#detailContent .spark-wrap"), svg=wrap?.querySelector("svg");
  if(!wrap || !svg || !asset.sparkline?.length) return;
  const line=svg.querySelector(".crosshair"), dot=svg.querySelector(".hover-dot"), tip=wrap.querySelector(".spark-tooltip");
  const values=asset.sparkline.map(point=>point.value), min=Math.min(...values), max=Math.max(...values), range=max-min||1;
  const inspect=event=>{
    const rect=svg.getBoundingClientRect(), ratio=Math.max(0,Math.min(1,(event.clientX-rect.left)/rect.width));
    const index=Math.round(ratio*(asset.sparkline.length-1)), point=asset.sparkline[index];
    const x=index/(asset.sparkline.length-1)*100, y=92-(point.value-min)/range*82;
    line.setAttribute("x1",x); line.setAttribute("x2",x); line.style.opacity=1;
    dot.setAttribute("cx",x); dot.setAttribute("cy",y); dot.style.opacity=1;
    tip.textContent=`${point.time} · ${fmt(point.value,asset.decimals)} ${asset.unit}`;
    tip.style.left=`${Math.max(8,Math.min(92,x))}%`; tip.classList.add("show");
  };
  svg.addEventListener("pointermove",inspect); svg.addEventListener("pointerdown",inspect);
  svg.addEventListener("pointerleave",()=>{line.style.opacity=0;dot.style.opacity=0;tip.classList.remove("show")});
}
function factorDetail(asset, key, passed){
  const side=asset.daily_score>=0 ? "多" : "空";
  const values={
    momentum:`${pct(asset.momentum_pct)}；${side}头阈值 ${side==='多'?'>':'< -'}${fmt(asset.threshold_pct,1)}%`,
    breakout:`收盘 ${fmt(asset.sparkline.at(-1).value,asset.decimals)}；20日高/低 ${fmt(asset.prior_high,asset.decimals)} / ${fmt(asset.prior_low,asset.decimals)}`,
    trend:`收盘相对 MA20 ${fmt(asset.ma20,asset.decimals)}`,
    rsi:`RSI14 = ${fmt(asset.rsi,1)}`
  };
  return `<div class="factor-row ${passed?'pass':''}"><i></i><div><b>${factorNames[key]}</b><small>${values[key]}</small></div><small>${passed?'通过':'未通过'}</small></div>`;
}
function selectAsset(id, shouldScroll=true){
  state.selected=id; renderCards();
  const asset=state.data.assets.find(item=>item.id===id);
  const title=document.querySelector("#detailTitle"), badge=document.querySelector("#detailBadge"), body=document.querySelector("#detailContent");
  title.textContent=`${asset.name} ${asset.symbol}`; badge.textContent=labels[asset.signal]; badge.style.color=colors[asset.signal]; badge.style.borderColor=colors[asset.signal];
  if(asset.status==="missing") { body.className="empty-detail"; body.textContent=asset.error; return; }
  const factors=asset.daily_score>=0?asset.long_factors:asset.short_factors, color=colors[asset.signal], bt=asset.backtest||{};
  body.className="detail-body"; body.style.setProperty("--detail-color",color);
  body.innerHTML=`<div class="spark-wrap"><svg viewBox="0 0 100 100" preserveAspectRatio="none" aria-label="最近60个交易日日线收盘价，可移动指针查看真实观测"><polyline points="${sparkPoints(asset.sparkline)}"></polyline><line class="crosshair" y1="7" y2="95"></line><circle class="hover-dot" r="2.3"></circle></svg><div class="spark-tooltip"></div><div class="spark-meta"><span>${asset.sparkline.length}个交易日 · ${asset.unit}</span><span>日线截止 ${asset.daily_date}</span></div><div class="spark-source">${asset.source} · ${asset.bar_timezone}</div></div>
    <div class="factor-list">${Object.entries(factors).map(([key,on])=>factorDetail(asset,key,on)).join('')}</div>
    <div class="timeframe-detail"><b>多周期确认</b>${timeframePill("周",asset.timeframes.week)}${timeframePill("日",asset.timeframes.day)}${timeframePill("时",asset.timeframes.hour)}${timeframePill("5分",asset.timeframes.five)}</div>
    <div class="metrics"><div class="metric"><small>1日 / 5日 / 20日</small><b>${pct(asset.returns.day)} · ${pct(asset.returns.week)} · ${pct(asset.returns.month)}</b></div><div class="metric"><small>持仓变化 1日 / 5日</small><b>${pct(asset.position_changes.day)} · ${pct(asset.position_changes.week)}</b></div><div class="metric"><small>日线四因子</small><b>多 ${asset.long_count}/4 · 空 ${asset.short_count}/4</b></div><div class="metric"><small>样本内快速回测</small><b>${bt.trades||0}笔 · 胜率${fmt(bt.win_rate_pct,1)}%</b></div><div class="metric"><small>收益 / 最大回撤</small><b>${pct(bt.net_return_pct)} · ${pct(bt.max_drawdown_pct)}</b></div></div>`;
  bindSpark(asset);
  if(shouldScroll) document.querySelector("#detail").scrollIntoView({behavior:"smooth",block:"center"});
}
function renderRanking(){
  const ranked=state.data.assets.filter(asset=>asset.score!=null).sort((a,b)=>b.score-a.score);
  document.querySelector("#ranking").innerHTML=ranked.map((asset,index)=>{const color=asset.score>=0?colors.long:colors.short;const left=asset.score>=0?50:50-Math.abs(asset.score)/2;return `<div class="rank-row" style="--rank-color:${color}"><span class="num">${String(index+1).padStart(2,'0')}</span><b>${asset.short_name}</b><span class="rank-track"><i style="left:${left}%;width:${Math.abs(asset.score)/2}%"></i></span><em>${asset.score>0?'+':''}${asset.score}</em></div>`}).join("");
}
function renderSummary(){
  const summary=state.data.summary, valid=state.data.assets.filter(asset=>asset.status!=="missing").length;
  document.querySelector("#longCount").textContent=summary.long; document.querySelector("#shortCount").textContent=summary.short;
  document.querySelector("#watchCount").textContent=summary.watch_long+summary.watch_short; document.querySelector("#healthCount").textContent=`${valid}/10`;
  const scores=state.data.assets.filter(asset=>asset.score!=null).map(asset=>asset.score), average=scores.length?scores.reduce((a,b)=>a+b,0)/scores.length:0;
  document.querySelector("#spectrumNeedle").style.left=`${Math.max(3,Math.min(97,50+average/2))}%`;
  document.querySelector("#scanTime").textContent=`扫描 ${new Date(state.data.generated_at).toLocaleString('zh-CN',{hour12:false})}`;
}
function renderMethod(){
  const names={bar:"频率结构",factors:"日线四因子",decision:"综合判断",execution:"刷新与回测"};
  document.querySelector("#methodList").innerHTML=Object.entries(state.data.methodology).map(([key,value])=>`<div class="method-item"><b>${names[key]}</b><span>${value}</span></div>`).join("");
  document.querySelector("#warnings").innerHTML=state.data.warnings.map(warning=>`<span class="warning">${warning}</span>`).join("");
}
function render(){renderSummary();renderCards();renderRanking();renderMethod();if(!state.selected){const first=state.data.assets.find(asset=>asset.status!=="missing");if(first)selectAsset(first.id,false)}}
function toast(message){const element=document.querySelector("#toast");element.textContent=message;element.classList.add("show");setTimeout(()=>element.classList.remove("show"),2600)}
function applyFilter(filter){if(!["all","long","short","watch","missing"].includes(filter))throw new Error("筛选值无效");state.filter=filter;document.querySelectorAll(".filter").forEach(button=>button.classList.toggle("active",button.dataset.filter===filter));renderCards();return filteredAssets().length}
function registerWebMcp(){const context=document.modelContext;if(!context?.registerTool)return;try{Promise.resolve(context.registerTool({name:"filter_metal_momentum_signals",title:"筛选金属多周期信号",description:"按偏多、偏空、观察或数据异常筛选金属多周期动量卡片。",inputSchema:{type:"object",properties:{filter:{type:"string",enum:["all","long","short","watch","missing"]}},required:["filter"],additionalProperties:false},annotations:{readOnlyHint:false,untrustedContentHint:false},execute(input){return{filter:input.filter,visible_assets:applyFilter(input.filter)}}})).catch(()=>{})}catch(_){}}
async function loadData(force=false){
  const button=document.querySelector("#refreshButton");button.disabled=true;button.firstChild.textContent=force?"扫描中 ":"载入中 ";
  try{let response;if(force){response=await fetch("/api/scan",{method:"POST"});if(!response.ok)throw new Error("本地扫描服务未启动")}else{response=await fetch("/api/status",{cache:"no-store"});if(!response.ok)response=await fetch(`latest.json?t=${Date.now()}`,{cache:"no-store"})}state.data=await response.json();if(state.data.error)throw new Error(state.data.error);state.seconds=state.data.interval_seconds||300;render();if(force)toast("多周期行情扫描完成")}
  catch(error){if(!force){try{const response=await fetch(`latest.json?t=${Date.now()}`);state.data=await response.json();render()}catch(_){}}toast(force?"云端静态页不能主动抓数，请等待定时更新":"数据载入失败")}
  finally{button.disabled=false;button.firstChild.textContent="立即扫描 "}
}
document.querySelectorAll(".filter").forEach(button=>button.addEventListener("click",()=>applyFilter(button.dataset.filter)));
document.querySelectorAll(".nav-item").forEach(button=>button.addEventListener("click",()=>{document.querySelectorAll(".nav-item").forEach(item=>item.classList.remove("active"));button.classList.add("active");document.querySelector(`#${button.dataset.scroll}`).scrollIntoView({behavior:"smooth"})}));
document.querySelector("#refreshButton").addEventListener("click",()=>loadData(true));
setInterval(()=>{const now=new Date();document.querySelector("#nowClock").textContent=now.toLocaleTimeString('zh-CN',{hour12:false});const h=now.getHours()+now.getMinutes()/60,day=now.getDay(),open=day>0&&day<6&&((h>=8.916&&h<=11.583)||(h>=13.416&&h<=15.083)||(h>=20.916)||(h<=2.583));document.querySelector("#marketState").classList.toggle("closed",!open);state.seconds=Math.max(0,state.seconds-1);document.querySelector("#countdown").textContent=`${String(Math.floor(state.seconds/60)).padStart(2,'0')}:${String(state.seconds%60).padStart(2,'0')}`;if(state.seconds===0)loadData(false)},1000);
loadData(false);registerWebMcp();
