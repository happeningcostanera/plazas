import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
  import {
    getFirestore, collection, doc, onSnapshot,
    setDoc, deleteDoc, addDoc, query, orderBy, limit, writeBatch, getDocs, getDoc, where
  } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
  import { getAuth, signInWithEmailAndPassword, onAuthStateChanged } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
  import { firebaseConfig } from "./config.js";

  const app = initializeApp(firebaseConfig);
  const db  = getFirestore(app);

  const LONG_PRESS_MS = 600;
  const MAX_SS_PER_SECTOR = 12;
  const MAX_SS_PER_BAR_SECTOR = 10;

  // ===================== TURNO =====================
  const TURNOS_VALIDOS = ["manana", "noche"];
  const TURNO_NOMBRES = { manana: "Mañana", noche: "Noche" };
  const TURNO_COLORES = { manana: { accent: "#e8b866", bg: "rgba(201,147,58,.15)" }, noche: { accent: "#90cfe0", bg: "rgba(60,120,180,.15)" } };
  const turno = new URLSearchParams(location.search).get("turno");
  const turnoValido = TURNOS_VALIDOS.includes(turno);
  const dangerMode = new URLSearchParams(location.search).get("danger") === "1";

  let popupSlotId=null, restMozoId=null, fijaMozoId=null, editCtx=null;
  let mozos=[], mozosBar=[], peones=[], sectores=[], sectoresBar=[], sectoresPeon=[], asignaciones={}, historial=[], ultimaRotacionTs=null;
  let pendingHistorial=null, mozoRotIdx=0, formacionBloqueada=false;
  let editableHastaLocal=null, feedbackGuardado="";
  let feedbackPorRotacion={};
  let notas={pesca:"",dolar:"",sugerencia:"",faltantes:""};
  let notasActivas=true;

  // Campo de disponibilidad por turno (fallback a "disponible" para datos existentes)
  const dispKey = turnoValido ? "disponible_" + turno : "disponible";
  function isDisp(item) { return dispKey in item ? !!item[dispKey] : !!item.disponible; }

  // Colecciones compartidas (personal y sectores)
  const mozosCol    = collection(db,"mozos");
  const barraCol      = collection(db,"mozosBar");
  const sectoresBarCol = collection(db,"sectoresBar");
  const peonesCol      = collection(db,"peones");
  const sectoresPeonCol = collection(db,"sectoresPeon");
  const sectoresCol = collection(db,"sectores");
  // Colecciones por turno (asignaciones e historial)
  const asigCol     = collection(db, turnoValido ? "asignaciones_" + turno : "asignaciones");
  const histCol     = collection(db, turnoValido ? "historial_" + turno : "historial");
  const feedbackCol = collection(db, turnoValido ? "feedback_" + turno : "feedback");

  let renderAllScheduled=false;
  function scheduleRenderAll() {
    if(renderAllScheduled) return;
    renderAllScheduled=true;
    Promise.resolve().then(()=>{
      renderAll();
      renderAllScheduled=false;
    });
  }

  onSnapshot(mozosCol, snap => { mozos=snap.docs.map(d=>({id:d.id,...d.data()})); scheduleRenderAll(); });
  onSnapshot(barraCol,       snap => { mozosBar=snap.docs.map(d=>({id:d.id,...d.data()})); scheduleRenderAll(); });
  onSnapshot(query(sectoresBarCol,orderBy("orden","asc")), snap => { sectoresBar=snap.docs.map(d=>({id:d.id,...d.data()})); scheduleRenderAll(); });
  onSnapshot(peonesCol,       snap => { peones=snap.docs.map(d=>({id:d.id,...d.data()})); scheduleRenderAll(); });
  onSnapshot(query(sectoresPeonCol,orderBy("orden","asc")), snap => { sectoresPeon=snap.docs.map(d=>({id:d.id,...d.data()})); scheduleRenderAll(); });
  onSnapshot(query(sectoresCol,orderBy("orden","asc")), snap => { sectores=snap.docs.map(d=>({id:d.id,...d.data()})); scheduleRenderAll(); });
  onSnapshot(asigCol, snap => { asignaciones={}; snap.docs.forEach(d=>{asignaciones[d.id]=d.data();}); scheduleRenderAll(); });
  onSnapshot(query(histCol,orderBy("ts","desc")), snap => { historial=snap.docs.map(d=>({id:d.id,...d.data()})); renderHistorial(); renderRotaciones(); });
  onSnapshot(feedbackCol, snap => { feedbackPorRotacion={}; snap.docs.forEach(d=>{ feedbackPorRotacion[d.id]=d.data().feedback||""; }); renderRotaciones(); });
  const metaSuffix = turnoValido ? "_" + turno : "";
  let ultimaRotacionNotas={};
  onSnapshot(doc(db,"meta","ultimaRotacion"+metaSuffix), snap => {
    if(snap.exists()){
      ultimaRotacionTs=snap.data().ts;
      ultimaRotacionNotas=snap.data().notas||{};
      // Restaurar estado editable si hay rotacionId y no expiró
      const d=snap.data();
      if(d.rotacionId && d.editableHasta && Date.now()<d.editableHasta && !ultimaRotacionId){
        ultimaRotacionId=d.rotacionId;
        formacionBloqueada=true;
        const af=document.getElementById("acciones-formacion"); if(af) af.style.display="flex";
        scheduleRenderAll();
      }
      editableHastaLocal=d.editableHasta||null;
      feedbackGuardado=d.feedback||"";
      renderFeedbackStrip();
    }else{ultimaRotacionTs=null;ultimaRotacionNotas={};editableHastaLocal=null;feedbackGuardado="";renderFeedbackStrip();}
    renderUltimaRotacion();
  });
  onSnapshot(doc(db,"meta","rotacion"+metaSuffix), snap => { if(snap.exists()) mozoRotIdx=snap.data().idx||0; });
  onSnapshot(doc(db,"meta","config"), snap => {
    if(snap.exists()){
      notasActivas=snap.data().notasActivas!==false;
    }
    const sw=document.getElementById("switch-notas"); if(sw) sw.checked=notasActivas;
    const ns=document.getElementById("notas-section"); if(ns) ns.style.display=notasActivas?"":"none";
  });
  onSnapshot(doc(db,"meta","notas"+metaSuffix), snap => {
    if(snap.exists()){
      notas=snap.data();
      const p=document.getElementById("nota-pesca");
      const d=document.getElementById("nota-dolar");
      const s=document.getElementById("nota-sugerencia");
      const f=document.getElementById("nota-faltantes");
      if(p&&p!==document.activeElement) p.value=notas.pesca||"";
      if(d&&d!==document.activeElement) d.value=notas.dolar||"";
      if(s&&s!==document.activeElement) s.value=notas.sugerencia||"";
      if(f&&f!==document.activeElement) f.value=notas.faltantes||"";
    }
  });

  window.addEventListener("online",  ()=>setConn(true));
  window.addEventListener("offline", ()=>setConn(false));
  setConn(navigator.onLine);
  function setConn(ok) {
    const el=document.getElementById("conn-status");
    el.textContent=ok?"● en línea":"● sin conexión";
    el.className=ok?"online":"offline";
  }

  // Si no hay turno válido en la URL, mostrar pantalla de selección
  if(!turnoValido){
    document.getElementById("loader").style.display="none";
    document.body.innerHTML=`
      <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;gap:24px;padding:20px">
        <img src="img/Logo.png" alt="SistemAlf Plaza" style="height:48px;opacity:.8"/>
        <h1 style="color:var(--gold2);font-size:22px;text-align:center">SistemAlf Plaza</h1>
        <p style="color:var(--text3);font-size:13px">Seleccioná el turno</p>
        <div style="display:flex;gap:16px;flex-wrap:wrap;justify-content:center">
          <a href="?turno=manana" style="text-decoration:none;background:linear-gradient(135deg,rgba(201,147,58,.15),rgba(232,184,102,.08));border:1px solid #e8b866;border-radius:14px;padding:24px 40px;display:flex;flex-direction:column;align-items:center;gap:8px;transition:transform .2s">
            <span style="font-size:36px">☀️</span>
            <span style="color:#e8b866;font-size:18px;font-weight:700">Mañana</span>
          </a>
          <a href="?turno=noche" style="text-decoration:none;background:linear-gradient(135deg,rgba(60,120,180,.15),rgba(144,207,224,.08));border:1px solid #90cfe0;border-radius:14px;padding:24px 40px;display:flex;flex-direction:column;align-items:center;gap:8px;transition:transform .2s">
            <span style="font-size:36px">🌙</span>
            <span style="color:#90cfe0;font-size:18px;font-weight:700">Noche</span>
          </a>
        </div>
      </div>`;
  }

  const auth = getAuth(app);

  function iniciarApp() {
    document.getElementById("loader").style.display="none";
    document.getElementById("login").style.display="none";
    document.getElementById("app").style.display="block";
    const headerTurno=document.getElementById("header-turno");
    const tc=TURNO_COLORES[turno];
    if(headerTurno) headerTurno.innerHTML=`<span style="color:${tc.accent}">${turno==="noche"?"🌙":"☀️"} Turno ${TURNO_NOMBRES[turno]}</span>`;
    if(turno==="noche") document.body.classList.add("turno-noche");
    seedIfEmpty();
  }

  function mostrarLogin() {
    document.getElementById("loader").style.display="none";
    document.getElementById("login").style.display="block";
  }

  window.doLogin = async function() {
    const email=document.getElementById("login-email").value.trim();
    const pass=document.getElementById("login-pass").value;
    const errEl=document.getElementById("login-error");
    errEl.style.display="none";
    if(!email||!pass){ errEl.textContent="Completar ambos campos."; errEl.style.display="block"; return; }
    try {
      await signInWithEmailAndPassword(auth, email, pass);
    } catch(e) {
      errEl.textContent="Usuario o contraseña incorrectos.";
      errEl.style.display="block";
    }
  };

  if(!turnoValido) { /* no iniciar app sin turno */ }
  else onAuthStateChanged(auth, user => {
    if(user && user.email) iniciarApp();
    else mostrarLogin();
  });

  async function seedIfEmpty() {
    const snap=await getDocs(mozosCol);
    if(!snap.empty) return;
    const batch=writeBatch(db);
    [["Carlos","👨‍🍳"],["Laura","👩‍🍳"],["Martín","👨‍🍳"],["Sofía","👩‍🍳"],["Diego","👨‍🍳"],["Ana","👩‍🍳"]]
      .forEach(([nombre,emoji])=>batch.set(doc(mozosCol),{nombre,emoji,[dispKey]:true,restricciones:[]}));
    [
      {nombre:"Parque",subsectores:[]},
      {nombre:"Deck",subsectores:[{id:"ss1",nombre:"Deck 1",[dispKey]:true},{id:"ss2",nombre:"Deck 2",[dispKey]:true}]},
      {nombre:"Salón",subsectores:[{id:"ss1",nombre:"Salón 1",[dispKey]:true},{id:"ss2",nombre:"Salón 2",[dispKey]:true},{id:"ss3",nombre:"Salón 3",[dispKey]:true},{id:"ss4",nombre:"Salón 4",[dispKey]:true}]},
      {nombre:"Pasillo",subsectores:[]},
      {nombre:"Cava",subsectores:[]},
      {nombre:"Cafetería",subsectores:[]},
      {nombre:"Barra",subsectores:[]}
    ].forEach((s,i)=>batch.set(doc(sectoresCol),{nombre:s.nombre,[dispKey]:true,subsectores:s.subsectores,orden:i}));
    await batch.commit();
  }

  function getSlots(soloActivos=true) {
    const slots=[];
    (soloActivos?sectores.filter(s=>isDisp(s)):sectores).forEach(s=>{
      const subs=soloActivos?(s.subsectores||[]).filter(ss=>isDisp(ss)):(s.subsectores||[]);
      subs.forEach(ss=>slots.push({slotId:s.id+"___"+ss.id,sectorId:s.id,ssId:ss.id,sectorNombre:s.nombre,ssNombre:ss["nombre_"+turno]||ss.nombre}));
    });
    return slots;
  }

  function getSlotsBar() {
    // Subsectores activos de sectores de barra (colección separada)
    const slots=[];
    sectoresBar.filter(s=>isDisp(s)).forEach(s=>{
      const subs=(s.subsectores||[]).filter(ss=>isDisp(ss));
      subs.forEach(ss=>slots.push({slotId:"bar_"+s.id+"___"+ss.id,sectorId:s.id,ssId:ss.id,sectorNombre:s.nombre,ssNombre:ss.nombre}));
    });
    return slots;
  }

  const FEEDBACK_TOOLTIP_KEY="feedbackTooltipVisto_"+(turno||"");
  const FEEDBACK_TOOLTIP_MAX=3;

  function renderFeedbackStrip() {
    const strip=document.getElementById("feedback-strip");
    if(!strip) return;
    const visible=formacionBloqueada && editableHastaLocal && Date.now()<editableHastaLocal;
    strip.style.display=visible?"block":"none";
    if(!visible) return;
    const label=document.getElementById("feedback-label");
    const hint=document.getElementById("feedback-toggle-hint");
    const input=document.getElementById("feedback-input");
    if(feedbackGuardado){
      if(label) label.textContent="✓ Comentario guardado";
      if(hint) hint.textContent="Editar";
    } else {
      if(label) label.textContent="¿Algo para mejorar en esta rotación?";
      if(hint) hint.textContent="Comentar";
    }
    if(input && document.getElementById("feedback-expanded").style.display==="none"){
      input.value=feedbackGuardado;
    }
    // Tooltip "nueva función" — hasta 3 veces
    const visto=parseInt(localStorage.getItem(FEEDBACK_TOOLTIP_KEY)||"0");
    const tooltip=document.getElementById("feedback-tooltip");
    if(tooltip) tooltip.remove();
    if(visto<FEEDBACK_TOOLTIP_MAX){
      const t=document.createElement("div");
      t.id="feedback-tooltip";
      t.style.cssText="margin-top:6px;padding:12px 14px;background:rgba(232,184,102,.1);border:1px solid rgba(232,184,102,.35);border-radius:10px;position:relative";
      t.innerHTML=`<div style="font-size:11px;font-weight:600;color:var(--gold2);margin-bottom:4px">✨ SistemAlf Nueva función</div>
        <div style="font-size:12px;color:var(--text2);line-height:1.5">Si la plaza se generó de una manera que tuviste que corregir, explicá lo mas preciso posible para mejorar la rotación automática.</div>
        <div style="text-align:right;margin-top:8px">
          <button onclick="cerrarFeedbackTooltip()" style="font-size:11px;padding:4px 12px;border-radius:6px;border:1px solid var(--gold);background:rgba(201,147,58,.15);color:var(--gold2);font-family:'DM Sans',sans-serif;cursor:pointer">Entendido</button>
        </div>`;
      strip.after(t);
    }
  }

  window.cerrarFeedbackTooltip = function() {
    const visto=parseInt(localStorage.getItem(FEEDBACK_TOOLTIP_KEY)||"0");
    localStorage.setItem(FEEDBACK_TOOLTIP_KEY, visto+1);
    const t=document.getElementById("feedback-tooltip");
    if(t) t.remove();
  };

  window.toggleFeedbackStrip = function() {
    const exp=document.getElementById("feedback-expanded");
    const input=document.getElementById("feedback-input");
    if(!exp) return;
    const abriendo=exp.style.display==="none";
    exp.style.display=abriendo?"block":"none";
    if(abriendo && input) { input.value=feedbackGuardado; input.focus(); }
  };

  window.verFeedbackRotacion = function(rotId) {
    const texto=feedbackPorRotacion[rotId];
    if(!texto) return;
    let tip=document.getElementById("feedback-tip");
    if(!tip){
      tip=document.createElement("div");
      tip.id="feedback-tip";
      tip.style.cssText="position:fixed;z-index:9999;background:#1a1a2a;border:1px solid var(--gold);color:var(--text);font-size:12px;padding:10px 14px;border-radius:8px;max-width:260px;box-shadow:0 4px 20px rgba(0,0,0,.6);white-space:pre-wrap;line-height:1.5;cursor:pointer";
      tip.title="Click para cerrar";
      tip.onclick=()=>tip.remove();
      document.body.appendChild(tip);
    }
    tip.textContent=texto;
    const x=Math.min(window.innerWidth/2-130, window.innerWidth-280);
    tip.style.left=Math.max(10,x)+"px";
    tip.style.top="80px";
  };
  window.guardarFeedback = async function() {
    const input=document.getElementById("feedback-input");
    if(!input||!ultimaRotacionId) return;
    const texto=input.value.trim();
    const feedbackCol=collection(db, turnoValido ? "feedback_"+turno : "feedback");
    const batch=writeBatch(db);
    batch.set(doc(feedbackCol,ultimaRotacionId),{rotacionId:ultimaRotacionId,feedback:texto,ts:Date.now()});
    // también en ultimaRotacion para restaurar si recargan la página durante el turno
    batch.set(doc(db,"meta","ultimaRotacion"+metaSuffix),{feedback:texto},{merge:true});
    await batch.commit();
    feedbackGuardado=texto;
    document.getElementById("feedback-expanded").style.display="none";
    renderFeedbackStrip();
  };

  function renderUltimaRotacion() {
    const el=document.getElementById("ultima-rotacion");
    if(!el) return;
    if(!ultimaRotacionTs){el.style.display="none";return;}
    const d=new Date(ultimaRotacionTs);
    const fecha=d.toLocaleDateString("es-AR",{weekday:"long",day:"2-digit",month:"long"});
    const hora=d.toLocaleTimeString("es-AR",{hour:"2-digit",minute:"2-digit",hour12:false});
    el.style.display="block";
    const turnoIcon = turno==="noche" ? "🌙" : "☀️";
    const turnoLabel = TURNO_NOMBRES[turno] || "Mañana";
    const turnoColor = TURNO_COLORES[turno] || TURNO_COLORES.manana;
    el.innerHTML=`<div style="padding:10px 12px;border-radius:8px 8px 0 0;background:linear-gradient(135deg,${turnoColor.bg},rgba(232,184,102,.08));border:1px solid ${turnoColor.accent};border-bottom:none;display:flex;align-items:baseline;justify-content:space-between;flex-wrap:wrap;gap:6px">
      <span style="font-size:15px;font-weight:700;color:${turnoColor.accent}">${turnoIcon} TURNO ${turnoLabel.toUpperCase()} — ${fecha}</span>
      <span style="font-size:11px;color:var(--text3)">confirmada ${hora}</span>
    </div>`;
  }

  function renderAll() {
    renderStats(); renderAvisoGlobal(); renderAvisoRotacion(); renderUltimaRotacion();
    const btnLib=document.getElementById("btn-liberar-todo");
    if(btnLib) btnLib.style.display=Object.keys(asignaciones).length>0?"inline-block":"none";
    renderSectoresGrid(); renderLibres(); renderPersonal(); renderSectoresConfig(); renderBarraGrid(); renderPeonesGrid();
    ["nota-pesca","nota-dolar","nota-sugerencia","nota-faltantes"].forEach(id=>{const el=document.getElementById(id);if(el) el.disabled=formacionBloqueada;});
    const ns=document.getElementById("notas-section"); if(ns) ns.style.display=notasActivas?"":"none";
  }

  function renderStats() {
    const mDisp=mozos.filter(m=>isDisp(m));
    const slots=getSlots();
    const libres=mDisp.filter(m=>!Object.values(asignaciones).some(a=>a.mozoId===m.id));
    const asigNormales=Object.keys(asignaciones).filter(id=>!id.startsWith("bar_")&&!id.startsWith("peon_")).length;
    const barraDisp=mozosBar.filter(m=>isDisp(m));
    document.getElementById("st-mozos").textContent=mDisp.length;
    document.getElementById("st-slots").textContent=slots.length;
    document.getElementById("st-asig").textContent=asigNormales;
    document.getElementById("st-libres").textContent=libres.length;
    document.getElementById("st-barra").textContent=barraDisp.length;
    // Stats en Personal y Salón
    const pa=document.getElementById("st-personal-activos"); if(pa) pa.textContent=mDisp.length;
    const pt=document.getElementById("st-personal-total"); if(pt) pt.textContent=mozos.length;
    const pp=document.getElementById("st-personal-plazas"); if(pp) pp.textContent=slots.length;
    const sp=document.getElementById("st-salon-plazas"); if(sp) sp.textContent=slots.length;
    const sm=document.getElementById("st-salon-mozos"); if(sm) sm.textContent=mDisp.length;
  }

  function renderAvisoGlobal() {
    const el=document.getElementById("aviso-global");
    const slots=getSlots();
    const mozosLibres=mozos.filter(m=>isDisp(m)&&!Object.values(asignaciones).some(a=>a.mozoId===m.id));
    const conflictos=slots.filter(sl=>!asignaciones[sl.slotId]&&mozosLibres.length>0&&mozosLibres.every(m=>(m.restricciones||[]).includes(sl.slotId)));
    if(conflictos.length>0) {
      const nombres=conflictos.map(sl=>sl.ssNombre||sl.sectorNombre).join(", ");
      el.innerHTML=`<div class="aviso error"><strong>⛔ Conflicto de restricciones</strong>Ningún mozo puede ir a: <strong>${nombres}</strong>.</div>`;
    } else el.innerHTML="";
  }

  function fmtHora(ts) {
    const d=new Date(ts);
    return d.getHours().toString().padStart(2,"0")+":"+d.getMinutes().toString().padStart(2,"0");
  }

  function toYMD(ts) {
    const d=new Date(ts);
    const y=d.getFullYear();
    const m=(d.getMonth()+1).toString().padStart(2,"0");
    const day=d.getDate().toString().padStart(2,"0");
    return `${y}-${m}-${day}`;
  }

  function getAyerSlotPorMozo() {
    const hoy = new Date(); hoy.setHours(0,0,0,0);
    const ayerFin = hoy.getTime() - 1;
    const ayerInicio = ayerFin - 86399999;
    const ayerRecs = historial.filter(h => h.ts >= ayerInicio && h.ts <= ayerFin && h.tipo !== "notas");
    if (!ayerRecs.length) return {};
    const rotId = ayerRecs[0]?.rotacionId;
    const recs = rotId ? ayerRecs.filter(h => h.rotacionId === rotId) : ayerRecs;
    const map = {};
    recs.forEach(h => { if (h.mozoId && h.slotId) map[h.mozoId] = h.slotId; });
    return map;
  }

  function renderSectoresGrid() {
    const grid=document.getElementById("sectores-grid");
    if(sectores.length===0){grid.innerHTML=`<div class="empty">No hay sectores. Creá uno en Sectores.</div>`;return;}

    const ayerMap = getAyerSlotPorMozo();
    let html=`<div class="slots-grid">`;

    let firstSector=true;
    sectores.forEach(s=>{
      const subs=s.subsectores||[];
      const subsActivos=subs.filter(ss=>isDisp(ss));
      const tieneSubsActivos=subsActivos.length>0;

      if(!isDisp(s)) return; // skip inactive sectors entirely
      if(!tieneSubsActivos) return; // skip sectors with no active subsectors

      // Sector label separator
      html+=`<div class="sector-label${firstSector?" first":""}">${s.nombre}</div>`;
      firstSector=false;

      html+=`<div class="sector-chips-row">`;
      if(tieneSubsActivos){
        subsActivos.forEach(ss=>{
          const slotId=s.id+"___"+ss.id;
          const asig=asignaciones[slotId];
          const mozo=asig?mozos.find(m=>m.id===asig.mozoId):null;
          html+=`<div class="ss-chip ${mozo?"ocupada":"libre"}"
            onclick="chipClick('${slotId}')"
            onpointerdown="startLongPress(event,'${slotId}','ss','${s.id}',${subsActivos.indexOf(ss)})"
            onpointerup="cancelLongPress()" onpointerleave="cancelLongPress()">`;
          html+=`<span class="ss-nombre">${ss["nombre_"+turno]||ss.nombre}</span>`;
          if(mozo){
            const repite = ayerMap[mozo.id] === slotId;
            html+=`<span class="ss-mozo">${mozo.nombre}${mozo.largo?' <b style="background:#c03020;color:#fff;font-size:11px;padding:1px 4px;border-radius:3px;vertical-align:middle">L</b>':''}${repite?' <b style="background:#c97c20;color:#fff;font-size:11px;padding:1px 4px;border-radius:3px;vertical-align:middle" title="Repite sector de ayer">↩</b>':''}</span>`;
            if(asig.comentario) html+=`<span class="ss-desc" style="color:#f0c060;font-style:italic">💬 ${asig.comentario}</span>`;
            else if(ss.descripcion) html+=`<span class="ss-desc">${ss.descripcion}</span>`;
            if(!formacionBloqueada) html+=`<button class="ss-liberar" onclick="event.stopPropagation();liberarSlot('${slotId}')">Liberar</button>`;
          } else {
            if(ss.descripcion) html+=`<span class="ss-desc">${ss.descripcion}</span>`;
            html+=`<span class="ss-libre-txt">libre</span>`;
          }
          html+=`</div>`;
        });
      } else {
        // Sector itself is the slot
        const slotId=s.id;
        const asig=asignaciones[slotId];
        const mozo=asig?mozos.find(m=>m.id===asig.mozoId):null;
        html+=`<div class="ss-chip ${mozo?"ocupada":"libre"}"
          onclick="chipClick('${slotId}')"
          onpointerdown="startLongPress(event,'${slotId}','sector','${s.id}',-1)"
          onpointerup="cancelLongPress()" onpointerleave="cancelLongPress()">`;
        html+=`<span class="ss-nombre">${s.nombre}</span>`;
        if(mozo){
          const repite = ayerMap[mozo.id] === slotId;
          html+=`<span class="ss-mozo">${mozo.nombre}${mozo.largo?' <b style="background:#c03020;color:#fff;font-size:11px;padding:1px 4px;border-radius:3px;vertical-align:middle">L</b>':''}${repite?' <b style="background:#c97c20;color:#fff;font-size:11px;padding:1px 4px;border-radius:3px;vertical-align:middle" title="Repite sector de ayer">↩</b>':''}</span>`;
          if(asig.comentario) html+=`<span class="ss-desc" style="color:#f0c060;font-style:italic">💬 ${asig.comentario}</span>`;
          else if(s.descripcion) html+=`<span class="ss-desc">${s.descripcion}</span>`;
          if(!formacionBloqueada) html+=`<button class="ss-liberar" onclick="event.stopPropagation();liberarSlot('${slotId}')">Liberar</button>`;
        } else {
          if(s.descripcion) html+=`<span class="ss-desc">${s.descripcion}</span>`;
          html+=`<span class="ss-libre-txt">libre</span>`;
        }
        html+=`</div>`;
      }
      html+=`</div>`;
    });

    html+=`</div>`;
    grid.innerHTML=html;
  }

  function renderLibres() {
    const libres=mozos.filter(m=>isDisp(m)&&!Object.values(asignaciones).some(a=>a.mozoId===m.id));
    document.getElementById("libres-section").style.display=libres.length?"block":"none";
    document.getElementById("libres-list").innerHTML=libres.map(m=>`<span class="chip on">${m.emoji} ${m.nombre}</span>`).join("");
    renderBarraGrid();
  }

  function renderBarraGrid() {
    // Operación barra
    // Construir HTML de grilla de barra (reutilizado en dos lugares)
    const buildBarraHtml = () => {
      let html="";
      sectoresBar.filter(s=>isDisp(s)).forEach(s=>{
        const subs=(s.subsectores||[]).filter(ss=>isDisp(ss));
        html+=`<div class="sector-row"><div class="sector-label">${s.nombre}</div><div class="sector-chips">`;
        if(subs.length===0){
          html+=`<span style="font-size:11px;color:var(--text3);font-style:italic">Sin sub sectores activos</span>`;
        } else {
          subs.forEach(ss=>{
            const slotId="bar_"+s.id+"___"+ss.id;
            const asig=asignaciones[slotId];
            const mozo=asig?mozosBar.find(m=>m.id===asig.mozoId):null;
            html+=`<div class="ss-chip ${mozo?"ocupada":"libre"}" onclick="${formacionBloqueada?"":"chipBarClick('"+slotId+"')"}" style="border-color:#5a8fa0;${mozo?"background:linear-gradient(135deg,#14293a,#1a3040)":"background:linear-gradient(135deg,#0f1d2a,#142530)"}">`;
            html+=`<span class="ss-nombre">${ss.nombre}</span>`;
            if(mozo){
              html+=`<span class="ss-mozo" style="color:#90cfe0">${mozo.nombre}</span>`;
                if(!formacionBloqueada) html+=`<button class="ss-liberar" onclick="event.stopPropagation();liberarSlot('${slotId}')">Liberar</button>`;
            } else {
              if(!formacionBloqueada) html+=`<span class="ss-libre-txt" style="color:#90cfe0">Libre — tap para asignar</span>`;
            }
            html+=`</div>`;
          });
        }
        html+=`</div></div>`;
      });
      return html;
    };

    const hasBarSectors = sectoresBar.filter(s=>isDisp(s)).length>0;

    // Grilla en pestaña Barra
    const opGrid=document.getElementById("barra-op-grid");
    if(opGrid){
      opGrid.innerHTML = hasBarSectors
        ? buildBarraHtml()
        : `<div class="empty" style="font-size:12px">Agregá sectores de barra abajo para empezar.</div>`;
    }

    // Grilla en pestaña Operación
    const opSec=document.getElementById("barra-op-section");
    const opGridOp=document.getElementById("barra-op-grid-op");
    if(opSec&&opGridOp){
      opSec.style.display = hasBarSectors ? "block" : "none";
      if(hasBarSectors) opGridOp.innerHTML = buildBarraHtml();
    }

    // Mozos de barra sin asignar
    const slotsBarActivos=getSlotsBar();
    const mozoBarAsignadosIds=new Set(
      slotsBarActivos.map(sl=>asignaciones[sl.slotId]?.mozoId).filter(Boolean)
    );
    const barLibres=mozosBar.filter(m=>isDisp(m)&&!mozoBarAsignadosIds.has(m.id));
    const barLibresSec=document.getElementById("barra-libres-section");
    const barLibresList=document.getElementById("barra-libres-list");
    if(barLibresSec&&barLibresList){
      barLibresSec.style.display = (hasBarSectors && barLibres.length) ? "block" : "none";
      barLibresList.innerHTML = barLibres.map(m=>`<span class="chip on" style="border-color:#5a8fa0;color:#90cfe0">🍸 ${m.nombre}</span>`).join("");
    }

    // Config sectores de barra
    const cfgCont=document.getElementById("sectores-bar-config");
    if(!cfgCont) return;
    if(sectoresBar.length===0){
      cfgCont.innerHTML=`<div class="empty">No hay sectores de barra aún.</div>`;
      return;
    }
  cfgCont.innerHTML=sectoresBar.map(s=>{
      const subs=s.subsectores||[];
      const canAdd=subs.length<MAX_SS_PER_BAR_SECTOR;
      const subsHtml=subs.map((ss,i)=>`
        <div class="ss-cfg-row" style="flex-wrap:wrap">
          <span class="ss-cfg-name ${isDisp(ss)?"":"off"}">${ss.nombre}</span>
          <button class="btn btn-ghost" onclick="editarNombreSubsectorBar('${s.id}',${i})">✏️</button>
          <button class="btn ${isDisp(ss)?"btn-gold":"btn-green"}" onclick="toggleSubsectorBar('${s.id}',${i},${!isDisp(ss)})">${isDisp(ss)?"Desact.":"Activ."}</button>
          ${dangerMode?`<button class="btn btn-red" onclick="eliminarSubsectorBar('${s.id}',${i})">✕</button>`:""}
        </div>`).join("");
      return `<div class="sector-cfg-block">
        <div class="sector-cfg-header" style="flex-wrap:wrap">
          <span class="sector-cfg-name ${isDisp(s)?"":"off"}">${s.nombre}</span>
          <button class="btn btn-ghost" onclick="editarNombreSectorBar('${s.id}')">✏️</button>
          <button class="btn ${isDisp(s)?"btn-gold":"btn-green"}" onclick="toggleSectorBar('${s.id}',${!isDisp(s)})">${isDisp(s)?"Desact.":"Activ."}</button>
          ${dangerMode?`<button class="btn btn-red" onclick="eliminarSectorBar('${s.id}')">✕</button>`:""}
        </div>
        ${subsHtml}
        ${canAdd?`<div class="input-row" style="margin-top:8px">
          <input type="text" id="nuevo-ss-bar-${s.id}" placeholder="Nuevo sub sector"/>
          <button class="btn btn-ghost" onclick="agregarSubsectorBar('${s.id}')">+ Sub sector</button>
        </div>`:`<div style="font-size:11px;color:var(--text3)">Máximo 10 sub sectores</div>`}
      </div>`;
    }).join("");
  }

  function renderPeonesGrid() {
    // Obtener peones asignados (keys con prefijo peon_)
    const peonAsig = Object.entries(asignaciones).filter(([k])=>k.startsWith("peon_"));

    const buildPeonesHtml = () => {
      let html="";
      sectoresPeon.filter(s=>isDisp(s)).forEach(s=>{
        const asigEnSector = peonAsig.filter(([k])=>k.startsWith("peon_"+s.id+"___"));
        html+=`<div class="sector-row"><div class="sector-label">🧹 ${s.nombre}</div><div class="sector-chips">`;
        if(asigEnSector.length>0){
          asigEnSector.forEach(([slotId,a])=>{
            const p=peones.find(p=>p.id===a.mozoId);
            if(!p) return;
            html+=`<div class="ss-chip ocupada" style="border-color:#8050a0;background:linear-gradient(135deg,#2a1a3a,#302040)">`;
            html+=`<span class="ss-mozo" style="color:#d0a0f0">${p.nombre}</span>`;
            if(!formacionBloqueada) html+=`<button class="ss-liberar" onclick="event.stopPropagation();liberarSlot('${slotId}')">Liberar</button>`;
            html+=`</div>`;
          });
        }
        // Botón para agregar peón al sector (solo si no está bloqueado)
        if(!formacionBloqueada){
          html+=`<div class="ss-chip libre" onclick="chipPeonClick('${s.id}')" style="cursor:pointer;border-color:#8050a0;background:linear-gradient(135deg,#1a1028,#201530)">`;
          html+=`<span class="ss-libre-txt" style="color:#b080d0">+ asignar peón</span>`;
          html+=`</div>`;
        }
        html+=`</div></div>`;
      });
      return html;
    };

    const hasPeonSectors = sectoresPeon.filter(s=>isDisp(s)).length>0;

    // Grilla en pestaña Peones
    const opGrid=document.getElementById("peones-op-grid");
    if(opGrid){
      opGrid.innerHTML = hasPeonSectors
        ? buildPeonesHtml()
        : `<div class="empty" style="font-size:12px">Agregá sectores de peones abajo para empezar.</div>`;
    }

    // Grilla en pestaña Operación
    const opSec=document.getElementById("peones-op-section");
    const opGridOp=document.getElementById("peones-op-grid-op");
    if(opSec&&opGridOp){
      opSec.style.display = hasPeonSectors ? "block" : "none";
      if(hasPeonSectors) opGridOp.innerHTML = buildPeonesHtml();
    }

    // Peones sin asignar
    const peonAsigIds=new Set(peonAsig.map(([,v])=>v.mozoId));
    const peonLibres=peones.filter(p=>isDisp(p)&&!peonAsigIds.has(p.id));
    const peonLibresSec=document.getElementById("peones-libres-section");
    const peonLibresList=document.getElementById("peones-libres-list");
    if(peonLibresSec&&peonLibresList){
      peonLibresSec.style.display = (hasPeonSectors && peonLibres.length) ? "block" : "none";
      peonLibresList.innerHTML = peonLibres.map(p=>`<span class="chip on" style="border-color:#8050a0;color:#d0a0f0">🧹 ${p.nombre}</span>`).join("");
    }

    // Config sectores de peones
    const cfgCont=document.getElementById("sectores-peon-config");
    if(!cfgCont) return;
    if(sectoresPeon.length===0){
      cfgCont.innerHTML=`<div class="empty">No hay sectores de peones aún.</div>`;
      return;
    }
    cfgCont.innerHTML=sectoresPeon.map(s=>{
      return `<div class="sector-cfg-block">
        <div class="sector-cfg-header" style="flex-wrap:wrap">
          <span class="sector-cfg-name ${isDisp(s)?"":"off"}">${s.nombre}</span>
          <button class="btn btn-ghost" onclick="editarNombreSectorPeon('${s.id}')">✏️</button>
          <button class="btn ${isDisp(s)?"btn-gold":"btn-green"}" onclick="toggleSectorPeon('${s.id}',${!isDisp(s)})">${isDisp(s)?"Desact.":"Activ."}</button>
          ${dangerMode?`<button class="btn btn-red" onclick="eliminarSectorPeon('${s.id}')">✕</button>`:""}
        </div>
      </div>`;
    }).join("");
  }

  function renderPersonal() {
    document.getElementById("mozos-list").innerHTML=[...mozos].sort((a,b)=>a.nombre.localeCompare(b.nombre)).map(m=>{
      const allSlots=getSlots(false);
      const huerfanas=(m.restricciones||[]).filter(slotId=>!allSlots.find(s=>s.slotId===slotId));
      if(huerfanas.length>0) setDoc(doc(mozosCol,m.id),{restricciones:(m.restricciones||[]).filter(r=>!huerfanas.includes(r))},{merge:true});
      if(m.plazaFija&&!allSlots.find(s=>s.slotId===m.plazaFija)) setDoc(doc(mozosCol,m.id),{plazaFija:null},{merge:true});
      const fijaTag=m.plazaFija?(()=>{
        const sl=allSlots.find(s=>s.slotId===m.plazaFija);
        const label=sl?(sl.ssNombre?`${sl.sectorNombre} › ${sl.ssNombre}`:sl.sectorNombre):m.plazaFija;
        return `<span class="rest-tag" style="border-color:var(--gold);color:var(--gold2)">📌 ${label} <button onclick="setPlazaFija('${m.id}',null)">×</button></span>`;
      })():"";
      const largoTag=m.largo?`<span class="rest-tag" style="border-color:#e06050;color:#f08070"><b>L</b> Largo <button onclick="toggleLargo('${m.id}',false)">×</button></span>`:"";
      const restTags=(m.restricciones||[]).filter(slotId=>!huerfanas.includes(slotId)).map(slotId=>{
        const sl=allSlots.find(s=>s.slotId===slotId);
        const label=sl?(sl.ssNombre?`${sl.sectorNombre} › ${sl.ssNombre}`:sl.sectorNombre):slotId;
        return `<span class="rest-tag">🚫 ${label} <button onclick="quitarRestriccion('${m.id}','${slotId}')">×</button></span>`;
      }).join("");
      return `<div class="person-row">
        <span style="font-size:18px">${m.emoji}</span>
        <div class="person-info">
          <div class="person-name ${isDisp(m)?"":"off"}">${m.nombre}</div>
          <div class="restricciones-tags">${largoTag}${fijaTag}${restTags}</div>
        </div>
        <div class="person-actions">
          <button class="btn ${m.largo?"btn-red":"btn-ghost"}" onclick="toggleLargo('${m.id}',${!m.largo})" title="Largo" style="font-weight:bold">L</button>
          <button class="btn btn-ghost" onclick="abrirPlazaFija('${m.id}')" title="Plaza fija">📌</button>
          <button class="btn btn-orange" onclick="abrirRestricciones('${m.id}')">🚫</button>
          <button class="btn btn-ghost"  onclick="abrirEdicion('mozo','${m.id}')">✏️</button>
          <button class="btn ${isDisp(m)?"btn-gold":"btn-green"}" onclick="toggleMozo('${m.id}',${!isDisp(m)})">${isDisp(m)?"Desactivar":"Activar"}</button>
          ${dangerMode?`<button class="btn btn-red" onclick="eliminarMozo('${m.id}')">✕</button>`:""}
        </div>
      </div>`;
    }).join("");

    // Mozos de barra
    const barList=document.getElementById("mozos-bar-list");
    if(barList) barList.innerHTML=mozosBar.length===0
      ? `<div class="empty">No hay mozos de barra aún.</div>`
      : [...mozosBar].sort((a,b)=>a.nombre.localeCompare(b.nombre)).map(m=>`<div class="person-row">
        <span style="font-size:18px">🍸</span>
        <div class="person-info">
          <div class="person-name ${isDisp(m)?"":"off"}">${m.nombre}</div>
        </div>
        <div class="person-actions">
          <button class="btn btn-ghost" onclick="abrirEdicionBar('${m.id}')">✏️</button>
          <button class="btn ${isDisp(m)?"btn-gold":"btn-green"}" onclick="toggleMozoBar('${m.id}',${!isDisp(m)})">${isDisp(m)?"Desactivar":"Activar"}</button>
          ${dangerMode?`<button class="btn btn-red" onclick="eliminarMozoBar('${m.id}')">✕</button>`:""}
        </div>
      </div>`).join("");

    // Peones
    const peonList=document.getElementById("peones-list");
    if(peonList) peonList.innerHTML=peones.length===0
      ? `<div class="empty">No hay peones aún.</div>`
      : [...peones].sort((a,b)=>a.nombre.localeCompare(b.nombre)).map(p=>`<div class="person-row">
        <span style="font-size:18px">🧹</span>
        <div class="person-info">
          <div class="person-name ${isDisp(p)?"":"off"}">${p.nombre}</div>
        </div>
        <div class="person-actions">
          <button class="btn btn-ghost" onclick="abrirEdicionPeon('${p.id}')">✏️</button>
          <button class="btn ${isDisp(p)?"btn-gold":"btn-green"}" onclick="togglePeon('${p.id}',${!isDisp(p)})">${isDisp(p)?"Desactivar":"Activar"}</button>
          ${dangerMode?`<button class="btn btn-red" onclick="eliminarPeon('${p.id}')">✕</button>`:""}
        </div>
      </div>`).join("");
  }

  function renderSectoresConfig() {
    const cont=document.getElementById("sectores-config");
    if(sectores.length===0){cont.innerHTML=`<div class="empty">No hay sectores aún.</div>`;return;}
    cont.innerHTML=sectores.map(s=>{
      const subs=s.subsectores||[];
      const canAdd=subs.length<MAX_SS_PER_SECTOR;
      const subsHtml=subs.map((ss,i)=>`
        <div class="ss-cfg-row" style="flex-wrap:wrap">
          <span class="ss-cfg-name ${isDisp(ss)?"":"off"}" style="min-width:80px">${ss["nombre_"+turno]||ss.nombre}</span>
          ${ss.descripcion?`<span style="font-size:10px;color:var(--text3);flex:1;font-style:italic">${ss.descripcion}</span>`:""}
          <button class="btn btn-ghost" onclick="abrirEdicion('subsector','${s.id}',${i})">✏️</button>
          <button class="btn ${isDisp(ss)?"btn-gold":"btn-green"}" onclick="toggleSubsector('${s.id}',${i},${!isDisp(ss)})">${isDisp(ss)?"Desact.":"Activ."}</button>
          ${dangerMode?`<button class="btn btn-red" onclick="eliminarSubsector('${s.id}',${i})">✕</button>`:""}
        </div>`).join("");
      return `<div class="sector-cfg-row" data-id="${s.id}"
        draggable="true"
        ondragstart="onDragStart(event,'${s.id}')"
        ondragover="onDragOver(event)"
        ondragleave="onDragLeave(event)"
        ondrop="onDrop(event,'${s.id}')"
        ondragend="onDragEnd(event)">
        <div class="sector-cfg-header" style="flex-wrap:wrap">
          <span class="drag-handle">⠿</span>
          <span class="sector-cfg-name ${isDisp(s)?"":"off"}">${s.nombre}</span>
          ${s.descripcion?`<span style="font-size:10px;color:var(--text3);flex:1;font-style:italic">${s.descripcion}</span>`:""}
          <button class="btn btn-ghost" onclick="abrirEdicion('sector','${s.id}')">✏️</button>
          <button class="btn ${s.evitarRepetirSector?"btn-orange":"btn-ghost"}" onclick="toggleEvitarRepetir('${s.id}',${!s.evitarRepetirSector})" title="Evitar repetir sector completo en ciclo siguiente">${s.evitarRepetirSector?"🔒 No repetir sector":"🔓 Permitir repetir"}</button>
          <button class="btn ${isDisp(s)?"btn-gold":"btn-green"}" onclick="toggleSector('${s.id}',${!isDisp(s)})">${isDisp(s)?"Desact.":"Activ."}</button>
          ${dangerMode?`<button class="btn btn-red" onclick="eliminarSector('${s.id}')">✕</button>`:""}
        </div>
        <div style="padding:4px 12px 6px;display:flex;align-items:center;gap:6px;flex-wrap:wrap">
          <span style="font-size:11px;color:var(--text3)">Grupo:</span>
          <select onchange="setGrupo('${s.id}',this.value)" style="background:var(--bg);border:1px solid var(--border2);border-radius:5px;color:var(--text);padding:3px 8px;font-family:'DM Sans',sans-serif;font-size:11px;outline:none">
            <option value="" ${!s.grupo?"selected":""}>— sin grupo —</option>
            ${[...new Set(sectores.map(x=>x.grupo).filter(Boolean))].map(g=>
              `<option value="${g}" ${s.grupo===g?"selected":""}>${g}</option>`
            ).join("")}
          </select>
          <input type="text" placeholder="Nuevo grupo" id="nuevo-grupo-${s.id}" style="width:90px;background:var(--bg);border:1px solid var(--border2);border-radius:5px;color:var(--text);padding:3px 8px;font-family:'DM Sans',sans-serif;font-size:11px;outline:none"/>
          <button class="btn btn-ghost" style="font-size:10px;padding:2px 6px" onclick="crearGrupo('${s.id}')">+ Crear</button>
        </div>
        <div class="ss-cfg-list">${subsHtml}</div>
        ${canAdd?`<div class="add-ss-row">
          <input type="text" id="new-ss-${s.id}" placeholder="Nuevo sub sector" style="font-size:12px;padding:6px 10px"/>
          <button class="btn btn-green" onclick="agregarSubsector('${s.id}')">+ Sub</button>
        </div>`:`<div style="font-size:10px;color:var(--text3);padding-left:12px;margin-top:6px">Máximo 12 sub sectores</div>`}
      </div>`;
    }).join("");
  }

  // Grupo efectivo de un sector (para rotación): si tiene grupo, lo usa; si no, usa su propio id
  function grupoDeId(sectorId) {
    const s=sectores.find(x=>x.id===sectorId);
    return s&&s.grupo ? s.grupo : sectorId;
  }

  // Resolver nombre actual de un mozo desde un registro del historial (por ID o fallback nombre)
  function resolverNombreMozo(h) {
    const m=mozos.find(m=>m.id===h.mozoId);
    return m?m.nombre:(h.mozoNombre||"");
  }

  function renderHistorial() {
    // Leer filtros ANTES de tocar el DOM del selector
    const filtroMozo  = document.getElementById("filtro-mozo")?.value || "";
    const filtroFecha = document.getElementById("filtro-fecha")?.value || "";

    // Actualizar opciones del selector sin destruir la selección actual
    const sel = document.getElementById("filtro-mozo");
    if (sel) {
      const nombres = [...new Set(historial.map(h=>resolverNombreMozo(h)).filter(Boolean))].sort();
      // Solo reconstruir si cambió la lista de mozos
      const optsActuales = [...sel.options].map(o=>o.value).filter(Boolean).join(",");
      const optsNuevas = nombres.join(",");
      if (optsActuales !== optsNuevas) {
        sel.innerHTML = `<option value="">Todos los mozos</option>` + nombres.map(n=>`<option value="${n}">${n}</option>`).join("");
        if (filtroMozo) sel.value = filtroMozo;
      }
    }

    let filtrado = historial.filter(h=>h.tipo!=="notas");
    if (filtroMozo)  filtrado = filtrado.filter(h => resolverNombreMozo(h) === filtroMozo);
    if (filtroFecha) filtrado = filtrado.filter(h => {
      if (!h.ts) return false;
      return toYMD(h.ts) === filtroFecha;
    });

    const empty = document.getElementById("hist-empty");
    const table = document.getElementById("hist-table");
    const countEl = document.getElementById("hist-count");

    if (filtrado.length === 0) {
      empty.style.display="block"; table.style.display="none";
      if (countEl) countEl.textContent = historial.length===0 ? "" : "Sin resultados para este filtro";
    } else {
      empty.style.display="none"; table.style.display="block";
      if (countEl) countEl.textContent = `${filtrado.length} registro${filtrado.length>1?"s":""}${filtroMozo||filtroFecha?" (filtrado)":""}`;

      // Calcular contador acumulado una sola vez: cuántas veces cada mozo estuvo en cada slot (en TODO el historial)
      const countMap=new Map();
      historial.forEach(h=>{
        const nombreMozo=resolverNombreMozo(h);
        if(!nombreMozo) return;
        const slotLabel=h.subsector||h.sector||"";
        if(!slotLabel) return;
        const key=`${nombreMozo}||${slotLabel}`;
        countMap.set(key,(countMap.get(key)||0)+1);
      });

      document.getElementById("hist-rows").innerHTML = filtrado.map(h => {
        const nombre=resolverNombreMozo(h);
        const d = h.ts ? new Date(h.ts) : null;
        const hora  = d ? d.toLocaleTimeString("es-AR",{hour:"2-digit",minute:"2-digit",hour12:false}) : "--:--";
        const fecha = d ? d.toLocaleDateString("es-AR",{day:"2-digit",month:"2-digit",year:"numeric"}) : "";
        const slotLabel = h.subsector || h.sector || "";
        const key = `${nombre}||${slotLabel}`;
        const count = countMap.get(key)||0;
        return `<div class="hist-row">
          <span style="font-size:11px;color:var(--text3)">${fecha}</span>
          <span class="hist-hora">${hora}</span>
          <span>${nombre}</span>
          <span style="color:var(--gold2)">${h.sector||""}</span>
          <span style="color:var(--text2)">${h.subsector||""}</span>
          <span class="hist-count-badge" title="${nombre} estuvo ${count}x en ${slotLabel}">${count}</span>
        </div>`;
      }).join("");
    }

    renderResumen(filtroMozo, filtroFecha);
  }

  function renderResumen(filtroMozo, filtroFecha) {
    const resumenEmpty = document.getElementById("resumen-empty");
    const resumenTable = document.getElementById("resumen-table");
    if (historial.length === 0) { resumenEmpty.style.display="block"; resumenTable.innerHTML=""; return; }
    resumenEmpty.style.display="none";

    // Filtrar por fecha y/o mozo
    let base = historial.filter(h=>h.tipo!=="notas");
    if (filtroFecha) base = base.filter(h => {
      if (!h.ts) return false;
      return toYMD(h.ts) === filtroFecha;
    });
    if (filtroMozo) base = base.filter(h => resolverNombreMozo(h) === filtroMozo);

    // Obtener mozos únicos
    const mozosU = [...new Set(base.map(h=>resolverNombreMozo(h)).filter(Boolean))].sort();
    if (mozosU.length === 0) { resumenTable.innerHTML=`<div class="empty">Sin datos para este filtro.</div>`; return; }

    // Construir labels de columna: "Sector › SubSector" para evitar ambigüedad
    // Usar Set con label completo como clave única
    const slotLabels = [...new Set(base.map(h => {
      if(h.subsector) return h.sector+" › "+h.subsector;
      return h.sector||"";
    }).filter(Boolean))];

    // Ordenar por sector primero, luego subsector
    slotLabels.sort();

    // Precalcular conteos por (mozo, slotLabel) para no filtrar en cada celda
    const resumenCountMap = new Map();
    base.forEach(h=>{
      const nm=resolverNombreMozo(h);
      if(!nm) return;
      const lbl = h.subsector ? `${h.sector} › ${h.subsector}` : (h.sector||"");
      if(!lbl) return;
      const key = `${nm}||${lbl}`;
      resumenCountMap.set(key,(resumenCountMap.get(key)||0)+1);
    });

    // Construir tabla
    let html = `<table class="resumen-table"><thead><tr><th>Mozo</th>`;
    slotLabels.forEach(lbl => {
      // Mostrar en dos líneas: sector arriba, subsector abajo
      const parts = lbl.split(" › ");
      const display = parts.length>1
        ? `<span style="font-size:9px;color:var(--text3);display:block">${parts[0]}</span>${parts[1]}`
        : lbl;
      html += `<th style="min-width:52px">${display}</th>`;
    });
    html += `<th>Total</th></tr></thead><tbody>`;

    mozosU.forEach(mozo => {
      const filas = base.filter(h => resolverNombreMozo(h) === mozo);
      const total = filas.length;
      html += `<tr><td>${mozo}</td>`;
      slotLabels.forEach(lbl => {
        const key = `${mozo}||${lbl}`;
        const n = resumenCountMap.get(key)||0;
        html += `<td><span class="resumen-count ${n===0?"zero":""}">${n||"—"}</span></td>`;
      });
      html += `<td><strong style="color:var(--gold2)">${total}</strong></td></tr>`;
    });
    html += `</tbody></table>`;
    resumenTable.innerHTML = html;
  }

  // ===================== ROTACIONES VISUALES =====================
  let rotacionesAgrupadas=[];
  let rotPaginaActual=0;

  function agruparRotaciones() {
    rotacionesAgrupadas=[];
    // Agrupar por rotacionId si existe, si no por proximidad de timestamp (backward compat)
    const porId={};
    const sinId=[];
    historial.forEach(h=>{
      if(h.rotacionId){
        if(!porId[h.rotacionId]) porId[h.rotacionId]=[];
        porId[h.rotacionId].push(h);
      } else {
        sinId.push(h);
      }
    });
    // Rotaciones con ID, ordenadas por timestamp del primer registro desc
    const conId=Object.values(porId).sort((a,b)=>b[0].ts-a[0].ts);
    conId.forEach(g=>rotacionesAgrupadas.push(g));
    // Backward compat: agrupar registros sin rotacionId por proximidad
    let rotActual=[];
    const UMBRAL_MS=5000;
    for(const h of sinId){
      if(!h.ts) continue;
      if(rotActual.length===0){
        rotActual.push(h);
      } else {
        const diff=Math.abs(rotActual[rotActual.length-1].ts - h.ts);
        if(diff<UMBRAL_MS){
          rotActual.push(h);
        } else {
          if(rotActual.length>1) rotacionesAgrupadas.push(rotActual);
          rotActual=[h];
        }
      }
    }
    if(rotActual.length>1) rotacionesAgrupadas.push(rotActual);
    // Ordenar todo por timestamp desc
    rotacionesAgrupadas.sort((a,b)=>b[0].ts-a[0].ts);
  }

  function renderRotaciones() {
    const emptyEl = document.getElementById("rotaciones-empty");
    const listEl = document.getElementById("rotaciones-list");
    const navEl = document.getElementById("rotaciones-nav");
    if(!emptyEl||!listEl||!navEl) return;

    agruparRotaciones();

    if(rotacionesAgrupadas.length===0){
      emptyEl.style.display="block"; navEl.style.display="none"; listEl.innerHTML=""; return;
    }
    emptyEl.style.display="none";

    // Asegurar que la página actual sea válida
    if(rotPaginaActual>=rotacionesAgrupadas.length) rotPaginaActual=0;

    // Navegación
    navEl.style.display=rotacionesAgrupadas.length>1?"block":"none";
    document.getElementById("rot-counter").textContent=`${rotPaginaActual+1} de ${rotacionesAgrupadas.length}`;
    document.getElementById("rot-prev").disabled=rotPaginaActual===0;
    document.getElementById("rot-prev").style.opacity=rotPaginaActual===0?".3":"1";
    document.getElementById("rot-next").disabled=rotPaginaActual>=rotacionesAgrupadas.length-1;
    document.getElementById("rot-next").style.opacity=rotPaginaActual>=rotacionesAgrupadas.length-1?".3":"1";

    // Renderizar solo la rotación actual
    const rot=rotacionesAgrupadas[rotPaginaActual];
    const ts=rot[0].ts;
    const d=new Date(ts);
    const fecha=d.toLocaleDateString("es-AR",{weekday:"long",day:"2-digit",month:"long"});
    const hora=d.toLocaleTimeString("es-AR",{hour:"2-digit",minute:"2-digit",hour12:false});

    const mozosHist=rot.filter(h=>!h.tipo||h.tipo==="mozo");
    const barraHist=rot.filter(h=>h.tipo==="barra");
    const peonHist=rot.filter(h=>h.tipo==="peon");

    const porSector={};
    mozosHist.forEach(h=>{
      const sec=h.sector||"Sin sector";
      if(!porSector[sec]) porSector[sec]=[];
      porSector[sec].push(h);
    });

    const rotId=rot[0].rotacionId||"";
    let html=`<div class="rotacion-card${rotPaginaActual===0?" rotacion-ultima":""}">`;
    html+=`<div class="rotacion-header">`;
    html+=`<span class="rotacion-fecha">${rotPaginaActual===0?"Última rotación — ":""}${fecha}</span>`;
    html+=`<span class="rotacion-hora">${hora} hs</span>`;
    if(rotId) html+=`<span style="font-size:8px;color:var(--text3);opacity:.5;cursor:pointer" title="ID: ${rotId}" onclick="navigator.clipboard.writeText('${rotId}')">#${rotId.slice(-6)}</span>`;
    if(rotId && feedbackPorRotacion[rotId]) html+=`<span style="font-size:11px;opacity:.6;cursor:pointer;margin-left:4px" title="Ver comentario" onclick="verFeedbackRotacion('${rotId}')">💬</span>`;
    html+=`</div>`;
    html+=`<div class="rotacion-grid">`;

    const sectorOrder=sectores.map(s=>s.nombre);
    const sortedSectors=Object.keys(porSector).sort((a,b)=>{
      const ia=sectorOrder.indexOf(a), ib=sectorOrder.indexOf(b);
      if(ia===-1&&ib===-1) return a.localeCompare(b);
      if(ia===-1) return 1;
      if(ib===-1) return -1;
      return ia-ib;
    });
    for(const sector of sortedSectors){
      const registros=porSector[sector];
      html+=`<div class="rotacion-sector-label">${sector}</div>`;
      html+=`<div class="rotacion-sector-row">`;
      registros.forEach(h=>{
        const ssNombre=h.subsector||sector;
        html+=`<div class="rotacion-chip">`;
        html+=`<span class="rotacion-chip-ss">${ssNombre}</span>`;
        const esLargo=mozos.find(m=>m.id===h.mozoId)?.largo;
        html+=`<span class="rotacion-chip-mozo">${resolverNombreMozo(h)||"—"}${esLargo?' <b style="background:#c03020;color:#fff;font-size:11px;padding:1px 4px;border-radius:3px;vertical-align:middle">L</b>':''}</span>`;
        if(h.comentario) html+=`<span style="font-size:9px;color:#f0c060;font-style:italic">💬 ${h.comentario}</span>`;
        html+=`</div>`;
      });
      html+=`</div>`;
    }

    html+=`</div>`;

    // Barra y Peones del historial en línea (50/50)
    if(barraHist.length>0||peonHist.length>0){
      html+=`<div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:10px">`;

      if(barraHist.length>0){
        html+=`<div style="flex:1;min-width:200px"><div class="rotacion-grid">`;
        html+=`<div class="rotacion-sector-label" style="color:#5a8fa0">🍸 Barra</div>`;
        const barPorSector={};
        barraHist.forEach(h=>{
          const sec=h.sector||"";
          if(!barPorSector[sec]) barPorSector[sec]=[];
          barPorSector[sec].push(h);
        });
        for(const [sector,regs] of Object.entries(barPorSector)){
          if(sector) html+=`<div class="rotacion-sector-label" style="font-size:10px;color:var(--text2)">${sector}</div>`;
          html+=`<div class="rotacion-sector-row">`;
          regs.forEach(h=>{
            html+=`<div class="rotacion-chip" style="border-color:#5a8fa0;background:linear-gradient(135deg,#14293a,#1a3040)">`;
            if(h.subsector) html+=`<span class="rotacion-chip-ss">${h.subsector}</span>`;
            html+=`<span class="rotacion-chip-mozo" style="color:#90cfe0">${resolverNombreMozo(h)||"—"}</span>`;
            html+=`</div>`;
          });
          html+=`</div>`;
        }
        html+=`</div></div>`;
      }

      if(peonHist.length>0){
        html+=`<div style="flex:1;min-width:200px"><div class="rotacion-grid">`;
        html+=`<div class="rotacion-sector-label" style="color:#b080d0">🧹 Peones</div>`;
        const peonPorSector={};
        peonHist.forEach(h=>{
          const sec=h.sector||"";
          if(!peonPorSector[sec]) peonPorSector[sec]=[];
          peonPorSector[sec].push(h);
        });
        for(const [sector,regs] of Object.entries(peonPorSector)){
          if(sector) html+=`<div class="rotacion-sector-label" style="font-size:10px;color:var(--text2)">${sector}</div>`;
          html+=`<div class="rotacion-sector-row">`;
          regs.forEach(h=>{
            html+=`<div class="rotacion-chip" style="border-color:#b080d0;background:linear-gradient(135deg,#2a1a3a,#302040)">`;
            html+=`<span class="rotacion-chip-mozo" style="color:#d0a0f0">${resolverNombreMozo(h)||"—"}</span>`;
            html+=`</div>`;
          });
          html+=`</div>`;
        }
        html+=`</div></div>`;
      }

      html+=`</div>`;
    }

    // Notas de esta rotación (desde historial)
    {
      const notasReg=rot.find(h=>h.tipo==="notas");
      const n=notasReg||{};
      const tieneNotas=n.pesca||n.dolar||n.sugerencia||n.faltantes;
      if(tieneNotas&&notasActivas){
        html+=`<div style="margin-top:12px;padding-top:10px;border-top:1px solid var(--border)">`;
        html+=`<div style="font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:.4px;margin-bottom:8px">📝 Notas</div>`;
        html+=`<div style="display:flex;gap:10px;flex-wrap:wrap;font-size:12px">`;
        html+=`<div style="flex:2;min-width:180px;display:flex;flex-direction:column;gap:6px">`;
        if(n.pesca) html+=`<div style="border:1px solid var(--border2);border-radius:7px;padding:8px 10px"><span style="color:var(--text3)">🐟 Pesca:</span> <span style="color:var(--text)">${n.pesca}</span></div>`;
        if(n.dolar) html+=`<div style="border:1px solid var(--border2);border-radius:7px;padding:8px 10px"><span style="color:var(--text3)">💲 Dólar:</span> <span style="color:var(--gold2);font-weight:700">${n.dolar}</span></div>`;
        if(n.sugerencia) html+=`<div style="border:1px solid var(--border2);border-radius:7px;padding:8px 10px"><span style="color:var(--text3)">💡 Sugerencia:</span> <span style="color:var(--text)">${n.sugerencia}</span></div>`;
        html+=`</div>`;
        if(n.faltantes) html+=`<div style="flex:1;min-width:120px;border:1px solid var(--border2);border-radius:7px;padding:8px 10px"><span style="color:var(--text3)">⚠️ Faltantes:</span><div style="color:#f0a060;margin-top:4px;white-space:pre-wrap">${n.faltantes}</div></div>`;
        html+=`</div></div>`;
      }
    }

    html+=`</div>`;
    listEl.innerHTML=html;
  }

  window.rotPaginaAnterior = function() {
    if(rotPaginaActual>0){ rotPaginaActual--; renderRotaciones(); }
  };
  window.rotPaginaSiguiente = function() {
    if(rotPaginaActual<rotacionesAgrupadas.length-1){ rotPaginaActual++; renderRotaciones(); }
  };

  window.renderHistorial = renderHistorial;
  window.limpiarFiltros = function() {
    const sel = document.getElementById("filtro-mozo");
    const fecha = document.getElementById("filtro-fecha");
    if (sel) sel.value = "";
    if (fecha) fecha.value = "";
    renderHistorial();
  };

  // ===================== ROTACION =====================
  function fueraDeHorario() {
    const h=new Date().getHours();
    if(turno==="manana") return h<8||h>=17;   // disponible 08:00–17:00
    if(turno==="noche")  return h>=2&&h<17;   // disponible 17:00–02:00
    return false;
  }

  function renderAvisoRotacion() {
    const aviso=document.getElementById("rotar-aviso");
    const btn=document.getElementById("btn-rotar");
    const hint=document.getElementById("rotar-hint");
    if(!aviso||!btn) return;

    if(fueraDeHorario()){
      const label=turno==="manana"?"08:00 y las 17:00":"las 17:00 y las 02:00";
      aviso.style.display="block";hint.style.display="none";
      aviso.innerHTML=`<div class="aviso warn"><strong>⏰ Fuera de horario</strong>El turno ${turno==="manana"?"mañana":"noche"} solo puede generarse entre ${label}.</div>`;
      btn.style.display="none";return;
    }
    btn.style.display="";

    const mDisp=mozos.filter(m=>isDisp(m));
    const slots=getSlots();
    const totalMozos=mDisp.length, totalSlots=slots.length;

    // Slots ya asignados manualmente y mozos ya usados
    const slotsLibres=slots.filter(sl=>!asignaciones[sl.slotId]);
    const mozosYaAsignados=new Set(slots.filter(sl=>asignaciones[sl.slotId]).map(sl=>asignaciones[sl.slotId].mozoId));
    const mozosLibres=mDisp.filter(m=>!mozosYaAsignados.has(m.id));
    const nLibres=mozosLibres.length, pLibres=slotsLibres.length;

    if(pLibres===0&&totalSlots>0){
      aviso.style.display="block";hint.style.display="none";
      aviso.innerHTML=`<div class="aviso warn"><strong>⚠️ Todas las Plazas están asignadas</strong>Para volver a rotar, liberá al menos una plaza.</div>`;
      btn.disabled=true;btn.style.opacity=".4";return;
    }
    if(totalMozos===0||totalSlots===0){
      aviso.style.display="block";hint.style.display="none";
      aviso.innerHTML=`<div class="aviso error"><strong>⛔ Sin mozos o plazas</strong>Activá mozos en Personal y plazas en Salón.</div>`;
      btn.disabled=true;btn.style.opacity=".4";return;
    }
    hint.style.display="block";aviso.style.display="block";
    const preAsig=totalSlots-pLibres;
    const infoPreAsig=preAsig>0?` (${preAsig} ya asignado${preAsig>1?"s":""})`:"";
    if(nLibres===pLibres){
      btn.disabled=false;btn.style.opacity="1";
      aviso.innerHTML=`<div class="aviso info"><strong>✅ Listo para rotar</strong>${nLibres} mozo${nLibres>1?"s":""} · ${pLibres} plaza${pLibres>1?"s":""}${infoPreAsig}.</div>`;
    } else if(nLibres<pLibres){
      btn.disabled=true;btn.style.opacity=".4";
      aviso.innerHTML=`<div class="aviso warn"><strong>⚠️ Menos mozos que plazas</strong>${nLibres} mozo${nLibres>1?"s":""} para ${pLibres} plazas${infoPreAsig}. Ajustá en Mozos o Plazas.</div>`;
    } else {
      btn.disabled=true;btn.style.opacity=".4";
      aviso.innerHTML=`<div class="aviso warn"><strong>⚠️ Más mozos que Plazas</strong>${nLibres} mozos para ${pLibres} plazas${infoPreAsig}. Ajustá en Mozos o Plazas.</div>`;
    }
  }

  window.rotarAutomatico = async function() {
    const mDisp=mozos.filter(m=>isDisp(m));
    const slots=getSlots();

    // Identificar slots ya asignados manualmente y mozos ya usados
    const slotsPreAsignados=new Set();
    const mozosPreAsignados=new Set();
    slots.forEach(sl=>{
      const asig=asignaciones[sl.slotId];
      if(asig){
        slotsPreAsignados.add(sl.slotId);
        mozosPreAsignados.add(asig.mozoId);
      }
    });

    // Pre-asignar plazas fijas
    const batchFija=writeBatch(db);
    let fijaCount=0;
    const fijaAsignaciones={};
    mDisp.forEach(m=>{
      if(!m.plazaFija) return;
      if(mozosPreAsignados.has(m.id)) return;
      if(slotsPreAsignados.has(m.plazaFija)) return;
      if(!slots.find(sl=>sl.slotId===m.plazaFija)) return;
      if((m.restricciones||[]).includes(m.plazaFija)) return;
      slotsPreAsignados.add(m.plazaFija);
      mozosPreAsignados.add(m.id);
      fijaAsignaciones[m.plazaFija]={mozoId:m.id,desde:Date.now()};
      batchFija.set(doc(asigCol,m.plazaFija),fijaAsignaciones[m.plazaFija]);
      fijaCount++;
    });
    if(fijaCount>0) await batchFija.commit();
    // Actualizar asignaciones locales para que el historial las incluya
    Object.assign(asignaciones,fijaAsignaciones);

    const ahora=Date.now();

    // ── Calcular historial y grupos (se usa en ambos Hungarian) ──────────────
    const treintaDias=ahora-30*24*60*60*1000;
    const historialReciente=historial.filter(h=>h.ts>treintaDias);

    // Orden de sectores activos (define la secuencia de rotación)
    const sectoresActivos=sectores.filter(s=>isDisp(s));
    const ordenGrupos=[];
    sectoresActivos.forEach(s=>{ const g=s.grupo||s.id; if(!ordenGrupos.includes(g)) ordenGrupos.push(g); });
    const gruposEvitar=new Set();
    sectoresActivos.filter(s=>s.evitarRepetirSector).forEach(s=>{ gruposEvitar.add(s.grupo||s.id); });

    // Para cada mozo disponible, su último grupo y último slot exacto (últimos 30 días)
    const ultimoGrupoPorMozo={};
    const ultimoSlotPorMozo={};
    mDisp.forEach(m=>{
      for(const h of historialReciente){
        if(h.mozoId!==m.id) continue;
        if(h.tipo==="notas"||!h.slotId) continue;
        ultimoGrupoPorMozo[m.id]=grupoDeId(h.slotId.split("___")[0]);
        ultimoSlotPorMozo[m.id]=h.slotId;
        break;
      }
    });

    // Grupo ideal (siguiente en el orden circular)
    const grupoIdealPorMozo={};
    mDisp.forEach(m=>{
      const ultimoGrupo=ultimoGrupoPorMozo[m.id];
      if(!ultimoGrupo){ grupoIdealPorMozo[m.id]=null; return; }
      const idxActual=ordenGrupos.indexOf(ultimoGrupo);
      if(idxActual===-1){ grupoIdealPorMozo[m.id]=null; return; }
      grupoIdealPorMozo[m.id]=ordenGrupos[(idxActual+1)%ordenGrupos.length];
    });

    function distanciaGrupo(mozoId, sectorId) {
      const ideal=grupoIdealPorMozo[mozoId];
      if(!ideal) return 0;
      const grupoSlot=grupoDeId(sectorId);
      const idxIdeal=ordenGrupos.indexOf(ideal);
      const idxSlot=ordenGrupos.indexOf(grupoSlot);
      if(idxIdeal===-1||idxSlot===-1) return 0;
      return (idxSlot-idxIdeal+ordenGrupos.length)%ordenGrupos.length;
    }
    function penEvitarRepetir(mozoId, sectorId) {
      const grupoSlot=grupoDeId(sectorId);
      if(!gruposEvitar.has(grupoSlot)) return 0;
      return (ultimoGrupoPorMozo[mozoId]===grupoSlot)?1:0;
    }

    // Penalización dominical (turno mañana)
    const slotDomingoPorMozo={};
    if(new Date().getDay()===0&&turno==="manana"){
      const hoy=new Date(); hoy.setHours(0,0,0,0);
      const domAnterior=new Date(hoy); domAnterior.setDate(hoy.getDate()-7);
      const tsDomInicio=domAnterior.getTime();
      const tsDomFin=tsDomInicio+24*60*60*1000-1;
      const histDom=historial.filter(h=>h.ts>=tsDomInicio&&h.ts<=tsDomFin&&h.tipo!=="notas");
      mDisp.forEach(m=>{
        const hDom=histDom.find(h=>h.mozoId===m.id);
        if(!hDom) return;
        const sl=
          (hDom.slotId&&slots.find(s=>s.slotId===hDom.slotId))||
          (hDom.subsector?slots.find(s=>s.ssNombre===hDom.subsector&&s.sectorNombre===hDom.sector):null)||
          (!hDom.subsector?slots.find(s=>s.sectorNombre===hDom.sector&&!s.ssNombre):null);
        if(sl) slotDomingoPorMozo[m.id]=sl.slotId;
      });
    }

    // Conteo de veces por mozo×slot (últimos 30 días, todos los mozos disponibles)
    const conteo={};
    mDisp.forEach(m=>{ conteo[m.id]={}; slots.forEach(sl=>{ conteo[m.id][sl.slotId]=0; }); });
    for(const h of historialReciente){
      const mozo=mDisp.find(m=>m.id===h.mozoId);
      if(!mozo) continue;
      const sl=
        (h.slotId&&slots.find(s=>s.slotId===h.slotId))||
        (h.subsector?slots.find(s=>s.ssNombre===h.subsector&&s.sectorNombre===h.sector):null)||
        (!h.subsector?slots.find(s=>s.sectorNombre===h.sector&&!s.ssNombre):null);
      if(sl&&conteo[mozo.id]) conteo[mozo.id][sl.slotId]=(conteo[mozo.id][sl.slotId]||0)+1;
    }

    const INF = 1e9;

    // Algoritmo de Hungarian (Kuhn-Munkres O(n³)):
    // encuentra la asignación global de mínimo costo, evitando que
    // el último mozo quede forzado a repetir plaza por decisiones greedy previas.
    function hungarianAssign(c) {
      const N=c.length;
      const u=new Array(N+1).fill(0), v=new Array(N+1).fill(0);
      const p=new Array(N+1).fill(0), way=new Array(N+1).fill(0);
      for(let i=1;i<=N;i++){
        p[0]=i; let j0=0;
        const minD=new Array(N+1).fill(Infinity);
        const used=new Array(N+1).fill(false);
        do{
          used[j0]=true;
          let i0=p[j0], delta=Infinity, j1=-1;
          for(let j=1;j<=N;j++){
            if(!used[j]){
              const cur=c[i0-1][j-1]-u[i0]-v[j];
              if(cur<minD[j]){minD[j]=cur; way[j]=j0;}
              if(minD[j]<delta){delta=minD[j]; j1=j;}
            }
          }
          for(let j=0;j<=N;j++){
            if(used[j]){u[p[j]]+=delta; v[j]-=delta;}
            else minD[j]-=delta;
          }
          j0=j1;
        }while(p[j0]!==0);
        do{const j1=way[j0]; p[j0]=p[j1]; j0=j1;}while(j0);
      }
      const assign=new Array(N);
      for(let j=1;j<=N;j++) assign[p[j]-1]=j-1;
      return assign;
    }

    // ── HUNGARIAN 1: LARGOS ──────────────────────────────────────────────────
    // Columnas = sectores (uno por sector, fuerza la restricción naturalmente)
    // Filas dummy si hay más sectores que largos (matrix cuadrada requerida)
    const largosDisp=mDisp.filter(m=>m.largo&&!mozosPreAsignados.has(m.id));
    if(largosDisp.length>0){
      // Sectores ya ocupados por plaza fija de un largo pre-asignado
      const sectoresUsadosLargo=new Set();
      mDisp.filter(m=>m.largo&&mozosPreAsignados.has(m.id)).forEach(m=>{
        const sl=slots.find(s=>s.slotId===m.plazaFija);
        if(sl) sectoresUsadosLargo.add(sl.sectorId);
      });

      // Un candidato por sector: todos los slots libres agrupados por sectorId
      const slotsPorSector=new Map();
      slots.filter(sl=>!slotsPreAsignados.has(sl.slotId)&&!sectoresUsadosLargo.has(sl.sectorId))
        .forEach(sl=>{ if(!slotsPorSector.has(sl.sectorId)) slotsPorSector.set(sl.sectorId,[]); slotsPorSector.get(sl.sectorId).push(sl); });
      const sectoresCandidatos=[...slotsPorSector.entries()].map(([sectorId,slotsDelSector])=>({sectorId,slotsDelSector}));

      const NL=largosDisp.length, NS=sectoresCandidatos.length;
      const dim=Math.max(NL,NS);
      const costLargos=Array.from({length:dim},(_,li)=>
        Array.from({length:dim},(_,si)=>{
          if(li>=NL||si>=NS) return 0; // dummy
          const largo=largosDisp[li];
          const {sectorId,slotsDelSector}=sectoresCandidatos[si];
          const slotsValidos=slotsDelSector.filter(sl=>!(largo.restricciones||[]).includes(sl.slotId));
          if(slotsValidos.length===0) return INF;
          const vecesSector=slotsDelSector.reduce((sum,sl)=>sum+(conteo[largo.id]?.[sl.slotId]||0),0);
          const dist=distanciaGrupo(largo.id,sectorId);
          const penRep=penEvitarRepetir(largo.id,sectorId);
          const penDom=slotsDelSector.some(sl=>slotDomingoPorMozo[largo.id]===sl.slotId)?1:0;
          const penUltimoSlot=slotsDelSector.some(sl=>ultimoSlotPorMozo[largo.id]===sl.slotId)?1:0;
          return dist*10000+penUltimoSlot*12000+penRep*1000+penDom*500+vecesSector*10+li;
        })
      );

      const asignacionLargos=hungarianAssign(costLargos);
      const batchLargo=writeBatch(db);
      const largoAsignaciones={};
      asignacionLargos.slice(0,NL).forEach((sectorIdx,largoIdx)=>{
        if(sectorIdx>=NS) return; // fila dummy asignada a columna dummy
        const largo=largosDisp[largoIdx];
        const {slotsDelSector}=sectoresCandidatos[sectorIdx];
        if(costLargos[largoIdx][sectorIdx]>=INF) return;
        const slotsValidos2=slotsDelSector.filter(sl=>!(largo.restricciones||[]).includes(sl.slotId));
        const slotsPreferidos=slotsValidos2.filter(sl=>sl.slotId!==ultimoSlotPorMozo[largo.id]);
        const pool=slotsPreferidos.length>0?slotsPreferidos:slotsValidos2;
        const slotCandidato=pool.reduce((best,sl)=>(conteo[largo.id]?.[sl.slotId]||0)<(conteo[largo.id]?.[best.slotId]||0)?sl:best);
        if(!slotCandidato) return;
        slotsPreAsignados.add(slotCandidato.slotId);
        mozosPreAsignados.add(largo.id);
        largoAsignaciones[slotCandidato.slotId]={mozoId:largo.id,desde:ahora};
        batchLargo.set(doc(asigCol,slotCandidato.slotId),largoAsignaciones[slotCandidato.slotId]);
      });
      if(Object.keys(largoAsignaciones).length>0) await batchLargo.commit();
      Object.assign(asignaciones,largoAsignaciones);
    }

    // ── HUNGARIAN 2: RESTO ───────────────────────────────────────────────────
    const slotsLibres=slots.filter(sl=>!slotsPreAsignados.has(sl.slotId));
    const mozosLibres=mDisp.filter(m=>!mozosPreAsignados.has(m.id));

    if(mozosLibres.length===0||slotsLibres.length===0||mozosLibres.length!==slotsLibres.length) return;
    const n=mozosLibres.length;

    // Leer índice circular desde Firestore
    const idxSnap=await getDoc(doc(db,"meta","rotacion"+metaSuffix));
    const idx=idxSnap.exists()?(idxSnap.data().idx||0):0;

    const resultado=[], advertencias=[];
    const mozosUsados=new Set();

    // Construir matriz de costos (mozos en orden circular × slots)
    // Pesos: dist×10000 + penUltimoSlot×12000 + penRepetir×1000 + penDomingo×500 + veces×10 + circularPos
    // Las restricciones se marcan con INF para excluirlas de la asignación óptima
    const mozosCirular = Array.from({length:n}, (_,mi) => mozosLibres[(idx+mi)%n]);
    const costMatrix = mozosCirular.map((mozo,mi) =>
      slotsLibres.map(slot => {
        if((mozo.restricciones||[]).includes(slot.slotId)) return INF;
        const dist=distanciaGrupo(mozo.id,slot.sectorId);
        const penUltimoSlot=(ultimoSlotPorMozo[mozo.id]===slot.slotId)?1:0;
        const penRepetir=penEvitarRepetir(mozo.id,slot.sectorId);
        const penDomingo=(slotDomingoPorMozo[mozo.id]===slot.slotId)?1:0;
        const veces=conteo[mozo.id]?.[slot.slotId]||0;
        return dist*10000 + penUltimoSlot*12000 + penRepetir*1000 + penDomingo*500 + veces*10 + mi;
      })
    );

    const asignacion=hungarianAssign(costMatrix);
    const slotsUsados=new Set();
    asignacion.forEach((slotIdx,mozoCircIdx)=>{
      const mozo=mozosCirular[mozoCircIdx];
      const slot=slotsLibres[slotIdx];
      if(costMatrix[mozoCircIdx][slotIdx]>=INF){
        advertencias.push(`⛔ ${slot.ssNombre||slot.sectorNombre}: sin mozo válido (revisar restricciones)`);
        return;
      }
      resultado.push({slotId:slot.slotId,mozoId:mozo.id,slot});
      mozosUsados.add(mozo.id);
      slotsUsados.add(slot.slotId);
    });
    // Advertir slots que quedaron sin mozo (por restricciones)
    slotsLibres.forEach(slot=>{
      if(!slotsUsados.has(slot.slotId))
        advertencias.push(`⛔ ${slot.ssNombre||slot.sectorNombre}: sin mozo (revisar restricciones)`);
    });

    if(resultado.length===0){
      alert("⛔ No se pudo rotar:\n\n"+advertencias.join("\n"));
      return;
    }

    const nuevoIdx=(idx+1)%n;
    const batch=writeBatch(db);
    resultado.forEach(({slotId,mozoId})=>batch.set(doc(asigCol,slotId),{mozoId,desde:ahora}));
    batch.set(doc(db,"meta","rotacion"+metaSuffix),{idx:nuevoIdx},{merge:true});
    await batch.commit();

    mozoRotIdx=nuevoIdx;
    // Incluir en el historial tanto las asignaciones automáticas como las manuales previas
    const todasLasAsig=[];
    // Primero las pre-asignadas manualmente
    slots.forEach(sl=>{
      if(!slotsPreAsignados.has(sl.slotId)) return;
      const asig=asignaciones[sl.slotId];
      if(!asig) return;
      const mozo=mozos.find(m=>m.id===asig.mozoId);
      if(mozo){const h={mozoId:asig.mozoId,mozoNombre:mozo.nombre,sector:sl.sectorNombre,subsector:sl.ssNombre||"",tipo:"mozo",ts:ahora};if(asig.comentario)h.comentario=asig.comentario;todasLasAsig.push(h);}
    });
    // Después las rotadas automáticamente
    resultado.forEach(({mozoId,slot})=>{
      const mozo=mozos.find(m=>m.id===mozoId);
      if(mozo) todasLasAsig.push({mozoId,mozoNombre:mozo.nombre,sector:slot.sectorNombre,subsector:slot.ssNombre||"",tipo:"mozo",ts:ahora});
    });
    pendingHistorial=todasLasAsig.map((h,i)=>({...h,ts:ahora-i}));
    mostrarBannerPendiente();
    if(advertencias.length>0) setTimeout(()=>alert("Rotación con advertencias:\n\n"+advertencias.join("\n")),300);
  };

  let ultimaRotacionId=null;

  window.confirmarRotacion = async function() {
    // Reconstruir historial desde las asignaciones actuales (refleja cualquier cambio manual)
    const slots=getSlots();
    const slotsLibres=slots.filter(sl=>!asignaciones[sl.slotId]);
    if(slotsLibres.length>0){
      const nombres=slotsLibres.map(sl=>sl.ssNombre||sl.sectorNombre).join(", ");
      alert(`⛔ No se puede confirmar: hay ${slotsLibres.length} plaza${slotsLibres.length>1?"s":""} sin asignar:\n${nombres}`);
      return;
    }
    const ahora=Date.now();
    const histActual=[];
    slots.forEach(sl=>{
      const asig=asignaciones[sl.slotId];
      if(!asig) return;
      const mozo=mozos.find(m=>m.id===asig.mozoId);
      if(mozo){const h={mozoId:asig.mozoId,mozoNombre:mozo.nombre,sector:sl.sectorNombre,subsector:sl.ssNombre||"",slotId:sl.slotId,tipo:"mozo",ts:ahora};if(asig.comentario)h.comentario=asig.comentario;histActual.push(h);}
    });
    // Barra
    const slotsBar=getSlotsBar();
    slotsBar.forEach(sl=>{
      const asig=asignaciones[sl.slotId];
      if(!asig) return;
      const mozo=mozosBar.find(m=>m.id===asig.mozoId);
      if(mozo) histActual.push({mozoId:asig.mozoId,mozoNombre:mozo.nombre,sector:sl.sectorNombre,subsector:sl.ssNombre||"",tipo:"barra",ts:ahora});
    });
    // Peones
    Object.entries(asignaciones).filter(([k])=>k.startsWith("peon_")).forEach(([k,v])=>{
      const parts=k.replace("peon_","").split("___");
      const sectorId=parts[0];
      const s=sectoresPeon.find(s=>s.id===sectorId);
      const p=peones.find(p=>p.id===v.mozoId);
      if(p) histActual.push({mozoId:v.mozoId,mozoNombre:p.nombre,sector:s?s.nombre:"",subsector:"",tipo:"peon",ts:ahora});
    });
    if(histActual.length===0) return;

    const rotacionId=ultimaRotacionId||ahora.toString();

    // Si estamos re-confirmando (editando formación), borrar los registros anteriores por rotacionId
    const oldSnap = await getDocs(query(histCol, where("rotacionId","==",rotacionId)));
    const batch=writeBatch(db);
    oldSnap.docs.forEach(d=>batch.delete(d.ref));

    // Guardar notas como parte de la rotación
    if(notasActivas&&(notas.pesca||notas.dolar||notas.sugerencia||notas.faltantes)){
      histActual.push({tipo:"notas",pesca:notas.pesca||"",dolar:notas.dolar||"",sugerencia:notas.sugerencia||"",faltantes:notas.faltantes||"",ts:ahora});
    }

    // Guardar nuevos registros
    histActual.forEach((h,i)=>{
      const ref=doc(histCol);
      batch.set(ref,{...h,rotacionId,ts:ahora-(h.tipo==="notas"?0:i)});
    });

    // Calcular límite de edición: mañana→17:00, noche→02:00 del día siguiente
    const hoy=new Date(ahora);
    let editableHasta;
    if(turno==="noche"){
      const manana=new Date(hoy); manana.setDate(manana.getDate()+1); manana.setHours(2,0,0,0);
      editableHasta=manana.getTime();
    } else {
      const limite=new Date(hoy); limite.setHours(17,0,0,0);
      editableHasta=limite.getTime();
    }

    batch.set(doc(db,"meta","ultimaRotacion"+metaSuffix),{ts:ahora,notas:{...notas},rotacionId,editableHasta},{merge:true});
    await batch.commit();

    ultimaRotacionId=rotacionId;
    editableHastaLocal=editableHasta;
    feedbackGuardado="";
    pendingHistorial=[];
    formacionBloqueada=true;
    document.getElementById("acciones-formacion").style.display="flex";
    renderFeedbackStrip();
    ocultarBannerPendiente();
    renderAll();
  };

  // ===================== EDITAR FORMACION =====================
  window.editarFormacion = function() {
    // Reconstruir pendingHistorial con las asignaciones actuales
    const slots=getSlots();
    const ahora=Date.now();
    const todasLasAsig=[];
    slots.forEach(sl=>{
      const asig=asignaciones[sl.slotId];
      if(!asig) return;
      const mozo=mozos.find(m=>m.id===asig.mozoId);
      if(mozo) todasLasAsig.push({mozoId:asig.mozoId,sector:sl.sectorNombre,subsector:sl.ssNombre||"",ts:ahora});
    });
    pendingHistorial=todasLasAsig.map((h,i)=>({...h,ts:ahora-i}));

    // Desbloquear y mostrar banner
    formacionBloqueada=false;
    document.getElementById("acciones-formacion").style.display="none";
    mostrarBannerPendiente();
    renderAll();
  };

  // ===================== POPUP ASIGNACION =====================
  window.chipClick = function(slotId) {
    if(formacionBloqueada) return;
    if(asignaciones[slotId]){abrirComentario(slotId);return;}
    abrirPopup(slotId);
  };
  window.abrirPopup = function(slotId) {
    popupSlotId=slotId;
    const slot=getSlots().find(s=>s.slotId===slotId);
    const label=slot?(slot.ssNombre?`${slot.sectorNombre} › ${slot.ssNombre}`:slot.sectorNombre):slotId;
    const libres=mozos.filter(m=>isDisp(m)&&!Object.values(asignaciones).some(a=>a.mozoId===m.id)).sort((a,b)=>a.nombre.localeCompare(b.nombre));
    document.getElementById("popup-title").textContent="Asignar mozo";
    document.getElementById("popup-sub").textContent="📍 "+label;
    const opc=document.getElementById("popup-opciones");
    if(libres.length===0) opc.innerHTML=`<div class="empty">No hay mozos libres.</div>`;
    else opc.innerHTML=libres.map(m=>{
      const rest=(m.restricciones||[]).includes(slotId);
      return `<div class="mozo-option ${rest?"restringido":""}" onclick="${rest?`asignarExcepcion('${m.id}')`:`asignarManual('${m.id}')`}">
        <span class="emoji">${m.emoji}</span><span class="mname">${m.nombre}</span>
        ${rest?`<span class="rest-label">🚫 restringido</span>`:""}
      </div>`;
    }).join("");
    document.getElementById("popup-overlay").classList.add("show");
  };
  window.cerrarPopup = function() { document.getElementById("popup-overlay").classList.remove("show"); popupSlotId=null; };
  window.asignarExcepcion = function(mozoId) {
    const mozo=mozos.find(m=>m.id===mozoId);
    if(!mozo) return;
    if(confirm(`${mozo.nombre} tiene restricción en esta plaza.\n¿Asignar por esta vez?`)) asignarManual(mozoId);
  };
  window.asignarManual = async function(mozoId) {
    if(!popupSlotId) return;
    const slot=getSlots().find(s=>s.slotId===popupSlotId);
    const ahora=Date.now();
    if(pendingHistorial&&pendingHistorial.length>0){
      // En modo edición/rotación pendiente: solo asignar, el historial se graba al confirmar
      await setDoc(doc(asigCol,popupSlotId),{mozoId,desde:ahora});
    } else {
      // Asignación suelta: solo asignar, el historial se graba al confirmar
      await setDoc(doc(asigCol,popupSlotId),{mozoId,desde:ahora});
    }
    cerrarPopup();
  };

  // ===================== LIBERAR =====================
  window.liberarSlot = async function(slotId) {
    if(formacionBloqueada) return;
    await deleteDoc(doc(asigCol,slotId));
  };
  window.liberarTodo = async function() {
    const ocupadas=Object.keys(asignaciones);
    if(ocupadas.length===0) return;
    if(!confirm(`¿Liberar los ${ocupadas.length} slot${ocupadas.length>1?"s":""} asignados (mozos, barra y peones)?`)) return;
    const batch=writeBatch(db);
    ocupadas.forEach(id=>batch.delete(doc(asigCol,id)));
    batch.set(doc(db,"meta","notas"+metaSuffix),{pesca:"",dolar:"",sugerencia:"",faltantes:""});
    batch.set(doc(db,"meta","ultimaRotacion"+metaSuffix),{ts:Date.now(),notas:{},rotacionId:null,editableHasta:null});
    await batch.commit();
    notas={pesca:"",dolar:"",sugerencia:"",faltantes:""};
    const np=document.getElementById("nota-pesca"); if(np) np.value="";
    const nd=document.getElementById("nota-dolar"); if(nd) nd.value="";
    const ns=document.getElementById("nota-sugerencia"); if(ns) ns.value="";
    const nf=document.getElementById("nota-faltantes"); if(nf) nf.value="";
    pendingHistorial=[];
    ultimaRotacionId=null;
    editableHastaLocal=null;
    feedbackGuardado="";
    formacionBloqueada=false;
    document.getElementById("acciones-formacion").style.display="none";
    renderFeedbackStrip();
    ocultarBannerPendiente();
  };

  // ===================== RESTRICCIONES =====================
  window.abrirRestricciones = function(mozoId) {
    restMozoId=mozoId;
    const mozo=mozos.find(m=>m.id===mozoId);
    const slots=getSlots();
    document.getElementById("rest-title").textContent="🚫 "+mozo.nombre;
    const opc=document.getElementById("rest-opciones");
    if(slots.length===0){opc.innerHTML=`<div class="empty">No hay slots activos.</div>`;return;}
    opc.innerHTML=slots.map(sl=>{
      const label=sl.ssNombre?`${sl.sectorNombre} › ${sl.ssNombre}`:sl.sectorNombre;
      const rest=(mozo.restricciones||[]).includes(sl.slotId);
      return `<div style="display:flex;align-items:center;gap:8px;padding:8px;border-radius:8px;background:var(--bg);border:1px solid ${rest?"var(--orange)":"var(--border)"};margin-bottom:6px">
        <span style="flex:1;font-size:12px;color:${rest?"#e8903a":"var(--text)"}">${label}</span>
        <button class="btn ${rest?"btn-orange":"btn-ghost"}" onclick="toggleRestriccion('${mozoId}','${sl.slotId}')">${rest?"Quitar":"Agregar"}</button>
      </div>`;
    }).join("");
    document.getElementById("rest-overlay").classList.add("show");
  };
  window.cerrarRestricciones = function() { document.getElementById("rest-overlay").classList.remove("show"); restMozoId=null; };
  window.toggleRestriccion = async function(mozoId,slotId) {
    const mozo=mozos.find(m=>m.id===mozoId);
    let rests=[...(mozo.restricciones||[])];
    rests=rests.includes(slotId)?rests.filter(r=>r!==slotId):[...rests,slotId];
    await setDoc(doc(mozosCol,mozoId),{restricciones:rests},{merge:true});
    setTimeout(()=>abrirRestricciones(mozoId),50);
  };
  window.quitarRestriccion = async function(mozoId,slotId) {
    const mozo=mozos.find(m=>m.id===mozoId);
    await setDoc(doc(mozosCol,mozoId),{restricciones:(mozo.restricciones||[]).filter(r=>r!==slotId)},{merge:true});
  };

  // ===================== PLAZA FIJA =====================
  window.abrirPlazaFija = function(mozoId) {
    fijaMozoId=mozoId;
    const mozo=mozos.find(m=>m.id===mozoId);
    const slots=getSlots(false);
    document.getElementById("fija-title").textContent="📌 "+mozo.nombre;
    const opc=document.getElementById("fija-opciones");
    if(slots.length===0){opc.innerHTML=`<div class="empty">No hay slots.</div>`;return;}
    opc.innerHTML=`<div style="display:flex;align-items:center;gap:8px;padding:8px;border-radius:8px;background:var(--bg);border:1px solid ${!mozo.plazaFija?"var(--gold)":"var(--border)"};margin-bottom:6px">
      <span style="flex:1;font-size:12px;color:var(--text)">— Sin plaza fija —</span>
      <button class="btn ${!mozo.plazaFija?"btn-gold":"btn-ghost"}" onclick="setPlazaFija('${mozoId}',null)">${!mozo.plazaFija?"✓ Actual":"Quitar"}</button>
    </div>`+slots.map(sl=>{
      const label=sl.ssNombre?`${sl.sectorNombre} › ${sl.ssNombre}`:sl.sectorNombre;
      const selected=mozo.plazaFija===sl.slotId;
      return `<div style="display:flex;align-items:center;gap:8px;padding:8px;border-radius:8px;background:var(--bg);border:1px solid ${selected?"var(--gold)":"var(--border)"};margin-bottom:6px">
        <span style="flex:1;font-size:12px;color:${selected?"var(--gold2)":"var(--text)"}">${label}</span>
        <button class="btn ${selected?"btn-gold":"btn-ghost"}" onclick="setPlazaFija('${mozoId}','${sl.slotId}')">${selected?"✓ Fija":"Fijar"}</button>
      </div>`;
    }).join("");
    document.getElementById("fija-overlay").classList.add("show");
  };
  window.cerrarPlazaFija = function() { document.getElementById("fija-overlay").classList.remove("show"); fijaMozoId=null; };
  window.toggleLargo = async function(mozoId, valor) {
    await setDoc(doc(mozosCol,mozoId),{largo:!!valor},{merge:true});
  };

  window.setPlazaFija = async function(mozoId,slotId) {
    await setDoc(doc(mozosCol,mozoId),{plazaFija:slotId||null},{merge:true});
    if(fijaMozoId) setTimeout(()=>abrirPlazaFija(mozoId),50);
  };

  // ===================== COMENTARIO POR SLOT =====================
  window.abrirComentario = function(slotId) {
    const asig=asignaciones[slotId];
    if(!asig) return;
    const mozo=mozos.find(m=>m.id===asig.mozoId);
    const slot=getSlots(false).find(s=>s.slotId===slotId);
    const label=slot?(slot.ssNombre?`${slot.sectorNombre} › ${slot.ssNombre}`:slot.sectorNombre):slotId;
    document.getElementById("comentario-title").textContent="💬 "+(mozo?mozo.nombre:"");
    document.getElementById("comentario-sub").textContent="📍 "+label;
    document.getElementById("comentario-input").value=asig.comentario||"";
    document.getElementById("comentario-slotId").value=slotId;
    document.getElementById("comentario-overlay").classList.add("show");
    setTimeout(()=>document.getElementById("comentario-input").focus(),100);
  };
  window.cerrarComentario = function() { document.getElementById("comentario-overlay").classList.remove("show"); };
  window.guardarComentario = async function() {
    const slotId=document.getElementById("comentario-slotId").value;
    const comentario=document.getElementById("comentario-input").value.trim();
    await setDoc(doc(asigCol,slotId),{comentario},{merge:true});
    cerrarComentario();
  };
  window.borrarComentario = async function() {
    const slotId=document.getElementById("comentario-slotId").value;
    await setDoc(doc(asigCol,slotId),{comentario:""},{merge:true});
    cerrarComentario();
  };

  // ===================== EDICION =====================
  window.abrirEdicion = function(tipo,id,idx) {
    editCtx={tipo,id,idx};
    let nombre="", desc="", showDesc=false;
    if(tipo==="mozo"){ nombre=mozos.find(m=>m.id===id)?.nombre||""; }
    else if(tipo==="sector"){ const s=sectores.find(s=>s.id===id); nombre=s?.nombre||""; desc=s?.descripcion||""; showDesc=true; }
    else if(tipo==="subsector"){ const ss=sectores.find(s=>s.id===id)?.subsectores?.[idx]; nombre=ss?.["nombre_"+turno]||ss?.nombre||""; desc=ss?.descripcion||""; showDesc=true; }
    document.getElementById("edit-title").textContent=`✏️ Editar ${tipo}`;
    document.getElementById("edit-nombre").value=nombre;
    document.getElementById("edit-desc").value=desc;
    document.getElementById("edit-desc-row").style.display=showDesc?"block":"none";
    document.getElementById("edit-overlay").classList.add("show");
    setTimeout(()=>document.getElementById("edit-nombre").focus(),100);
  };
  window.cerrarEdicion = function() { document.getElementById("edit-overlay").classList.remove("show"); editCtx=null; };
  window.guardarEdicion = async function() {
    const nombre=document.getElementById("edit-nombre").value.trim();
    if(!nombre||!editCtx) return;
    const {tipo,id,idx}=editCtx;
    const desc=document.getElementById("edit-desc").value.trim();
    if(tipo==="mozo"){
      await setDoc(doc(mozosCol,id),{nombre},{merge:true});
    } else if(tipo==="sector"){
      const nombreViejo=sectores.find(s=>s.id===id)?.nombre;
      await setDoc(doc(sectoresCol,id),{nombre,descripcion:desc},{merge:true});
      if(nombreViejo&&nombreViejo!==nombre){const snap=await getDocs(histCol);const batch=writeBatch(db);snap.docs.forEach(d=>{if(d.data().sector===nombreViejo)batch.set(d.ref,{sector:nombre},{merge:true});});await batch.commit();}
    } else if(tipo==="subsector"){
      const s=sectores.find(s=>s.id===id);
      const subs=[...(s.subsectores||[])];
      subs[idx]={...subs[idx],["nombre_"+turno]:nombre,descripcion:desc};
      await setDoc(doc(sectoresCol,id),{subsectores:subs},{merge:true});
    }
    cerrarEdicion();
  };

  // ===================== SECTORES CRUD =====================
  window.agregarSector = async function() {
    const inp=document.getElementById("nuevo-sector");
    const nombre=inp.value.trim();
    if(!nombre) return;
    const maxOrden=sectores.length>0?Math.max(...sectores.map(s=>s.orden??0))+1:0;
    await addDoc(sectoresCol,{nombre,[dispKey]:true,subsectores:[],orden:maxOrden});
    inp.value="";
  };

  // ── SECTORES DE BARRA ──
  window.agregarSectorBar = async function() {
    const inp=document.getElementById("nuevo-sector-bar");
    const nombre=inp.value.trim(); if(!nombre) return;
    const snap=await getDocs(sectoresBarCol);
    const maxOrden=snap.docs.reduce((m,d)=>Math.max(m,d.data().orden||0),0);
    await addDoc(sectoresBarCol,{nombre,[dispKey]:true,orden:maxOrden+1,subsectores:[]});
    inp.value="";
  };

  window.toggleSectorBar = async function(id,disp) {
    await setDoc(doc(sectoresBarCol,id),{[dispKey]:disp},{merge:true});
    if(!disp){
      const s=sectoresBar.find(s=>s.id===id);
      const batch=writeBatch(db);
      (s?.subsectores||[]).forEach(ss=>batch.delete(doc(asigCol,"bar_"+id+"___"+ss.id)));
      await batch.commit();
    }
  };

  window.eliminarSectorBar = async function(id) {
    if(!confirm("¿Eliminar este sector de barra?")) return;
    const s=sectoresBar.find(s=>s.id===id);
    const batch=writeBatch(db);
    batch.delete(doc(sectoresBarCol,id));
    (s?.subsectores||[]).forEach(ss=>batch.delete(doc(asigCol,"bar_"+id+"___"+ss.id)));
    await batch.commit();
  };

  window.agregarSubsectorBar = async function(sectorId) {
    const inp=document.getElementById("nuevo-ss-bar-"+sectorId);
    const nombre=inp?.value.trim(); if(!nombre) return;
    const s=sectoresBar.find(s=>s.id===sectorId); if(!s) return;
    const subs=[...(s.subsectores||[]),{id:"ss"+Date.now(),nombre,[dispKey]:true,descripcion:""}];
    await setDoc(doc(sectoresBarCol,sectorId),{subsectores:subs},{merge:true});
    inp.value="";
  };

  window.toggleSubsectorBar = async function(sectorId,ssIdx,disp) {
    const s=sectoresBar.find(s=>s.id===sectorId); if(!s) return;
    const subs=[...(s.subsectores||[])];
    subs[ssIdx]={...subs[ssIdx],[dispKey]:disp};
    if(!disp) await deleteDoc(doc(asigCol,"bar_"+sectorId+"___"+subs[ssIdx].id));
    await setDoc(doc(sectoresBarCol,sectorId),{subsectores:subs},{merge:true});
  };

  window.eliminarSubsectorBar = async function(sectorId,ssIdx) {
    const s=sectoresBar.find(s=>s.id===sectorId); if(!s) return;
    const subs=[...(s.subsectores||[])];
    const ssId=subs[ssIdx].id;
    subs.splice(ssIdx,1);
    const batch=writeBatch(db);
    batch.set(doc(sectoresBarCol,sectorId),{subsectores:subs},{merge:true});
    batch.delete(doc(asigCol,"bar_"+sectorId+"___"+ssId));
    await batch.commit();
  };

  window.editarNombreSectorBar = async function(id) {
    const s=sectoresBar.find(s=>s.id===id); if(!s) return;
    const nuevo=prompt("Nombre del sector:",s.nombre);
    if(nuevo&&nuevo.trim()) await setDoc(doc(sectoresBarCol,id),{nombre:nuevo.trim()},{merge:true});
  };

  window.toggleEvitarRepetir = async function(sectorId, val) {
    await setDoc(doc(sectoresCol, sectorId), { evitarRepetirSector: val }, { merge: true });
  };

  // Diagnóstico de rotación — llamar desde consola: diagnosticarRotacion()
  // No modifica nada, solo muestra cómo vería el algoritmo a cada mozo
  window.diagnosticarRotacion = function() {
    const mDisp=mozos.filter(m=>isDisp(m));
    const slots=getSlots();
    const ahora=Date.now();
    const treintaDias=ahora-30*24*60*60*1000;
    const historialReciente=historial.filter(h=>h.ts>treintaDias);
    const sectoresActivos=sectores.filter(s=>isDisp(s));
    const ordenGrupos=[];
    sectoresActivos.forEach(s=>{ const g=s.grupo||s.id; if(!ordenGrupos.includes(g)) ordenGrupos.push(g); });
    const gruposEvitar=new Set();
    sectoresActivos.filter(s=>s.evitarRepetirSector).forEach(s=>{ gruposEvitar.add(s.grupo||s.id); });
    const ultimoGrupoPorMozo={}, ultimoSlotPorMozo={};
    mDisp.forEach(m=>{
      for(const h of historialReciente){
        if(h.mozoId!==m.id) continue;
        if(h.tipo==="notas"||!h.slotId) continue;
        ultimoGrupoPorMozo[m.id]=grupoDeId(h.slotId.split("___")[0]);
        ultimoSlotPorMozo[m.id]=h.slotId;
        break;
      }
    });
    const grupoIdealPorMozo={};
    mDisp.forEach(m=>{
      const ug=ultimoGrupoPorMozo[m.id]; if(!ug){ grupoIdealPorMozo[m.id]=null; return; }
      const idx=ordenGrupos.indexOf(ug); if(idx===-1){ grupoIdealPorMozo[m.id]=null; return; }
      grupoIdealPorMozo[m.id]=ordenGrupos[(idx+1)%ordenGrupos.length];
    });
    console.log("=== DIAGNÓSTICO DE ROTACIÓN ===");
    console.log("ordenGrupos:", ordenGrupos);
    console.log("gruposEvitar (no repetir):", [...gruposEvitar]);
    console.table(mDisp.map(m=>({
      nombre: m.nombre,
      largo: m.largo?"sí":"no",
      ultimoGrupo: ultimoGrupoPorMozo[m.id]||"❌ sin historial",
      ultimoSlot: ultimoSlotPorMozo[m.id]||"—",
      grupoIdeal: grupoIdealPorMozo[m.id]||"❌ null (sin penalización dist)"
    })));
    console.log("Slots activos:", slots.map(s=>s.sectorNombre+(s.ssNombre?" › "+s.ssNombre:"")+" ["+s.slotId+"]"));
  };

  window.setGrupo = async function(sectorId, grupo) {
    await setDoc(doc(sectoresCol, sectorId), { grupo: grupo || null }, { merge: true });
  };

  window.crearGrupo = async function(sectorId) {
    const input=document.getElementById("nuevo-grupo-"+sectorId);
    const nombre=input?.value.trim();
    if(!nombre) return;
    await setDoc(doc(sectoresCol, sectorId), { grupo: nombre }, { merge: true });
    input.value="";
  };

  window.toggleSector = async function(id,disp) {
    await setDoc(doc(sectoresCol,id),{[dispKey]:disp},{merge:true});
    if(!disp){
      // Liberar todos los subsectores de este sector
      const s=sectores.find(s=>s.id===id);
      const subs=(s?.subsectores||[]);
      const batch=writeBatch(db);
      subs.forEach(ss=>batch.delete(doc(asigCol,id+"___"+ss.id)));
      await batch.commit();
    }
  };
  window.eliminarSector = async function(id) {
    if(!confirm("¿Eliminar este sector y todos sus sub sectores?")) return;
    const s=sectores.find(s=>s.id===id);
    const subs=(s?.subsectores||[]);
    const batch=writeBatch(db);
    batch.delete(doc(sectoresCol,id));
    subs.forEach(ss=>batch.delete(doc(asigCol,id+"___"+ss.id)));
    // Limpiar restricciones de mozos
    mozos.forEach(m=>{
      const rests=(m.restricciones||[]).filter(r=>!r.startsWith(id+"___"));
      if(rests.length!==(m.restricciones||[]).length) batch.set(doc(mozosCol,m.id),{restricciones:rests},{merge:true});
    });
    await batch.commit();
  };
  window.agregarSubsector = async function(sectorId) {
    const inp=document.getElementById(`new-ss-${sectorId}`);
    const nombre=inp.value.trim();
    if(!nombre) return;
    const s=sectores.find(s=>s.id===sectorId);
    const subs=[...(s.subsectores||[])];
    if(subs.length>=MAX_SS_PER_SECTOR) return;
    subs.push({id:"ss"+Date.now(),nombre,[dispKey]:true});
    await setDoc(doc(sectoresCol,sectorId),{subsectores:subs},{merge:true});
    inp.value="";
  };
  window.toggleSubsector = async function(sectorId,idx,disp) {
    const s=sectores.find(s=>s.id===sectorId);
    const subs=[...(s.subsectores||[])];
    const ssId=subs[idx].id;
    subs[idx]={...subs[idx],[dispKey]:disp};
    await setDoc(doc(sectoresCol,sectorId),{subsectores:subs},{merge:true});
    if(!disp) await deleteDoc(doc(asigCol,sectorId+"___"+ssId));
  };
  window.eliminarSubsector = async function(sectorId,idx) {
    if(!confirm("¿Eliminar este sub sector?")) return;
    const s=sectores.find(s=>s.id===sectorId);
    const subs=[...(s.subsectores||[])];
    const slotId=sectorId+"___"+subs[idx].id;
    subs.splice(idx,1);
    const batch=writeBatch(db);
    batch.set(doc(sectoresCol,sectorId),{subsectores:subs},{merge:true});
    batch.delete(doc(asigCol,slotId));
    mozos.forEach(m=>{const rests=(m.restricciones||[]).filter(r=>r!==slotId);if(rests.length!==(m.restricciones||[]).length)batch.set(doc(mozosCol,m.id),{restricciones:rests},{merge:true});});
    await batch.commit();
  };

  // ===================== MOZOS CRUD =====================
  window.toggleMozo = async function(id,disp) {
    await setDoc(doc(mozosCol,id),{[dispKey]:disp},{merge:true});
    if(!disp) for(const [slotId,a] of Object.entries(asignaciones)) if(a.mozoId===id) await deleteDoc(doc(asigCol,slotId));
  };
  window.eliminarMozo = async function(id) {
    if(!confirm("¿Eliminar este mozo?")) return;
    await deleteDoc(doc(mozosCol,id));
    for(const [slotId,a] of Object.entries(asignaciones)) if(a.mozoId===id) await deleteDoc(doc(asigCol,slotId));
  };
  window.agregarMozo = async function() {
    const inp=document.getElementById("nuevo-mozo");
    const nombre=inp.value.trim();
    if(!nombre) return;
    const emojis=["👨‍🍳","👩‍🍳","🧑‍🍳"];
    await addDoc(mozosCol,{nombre,emoji:emojis[mozos.length%3],[dispKey]:true,restricciones:[]});
    inp.value="";
  };

  window.agregarMozoBar = async function() {
    const inp=document.getElementById("nuevo-mozo-bar");
    const nombre=inp.value.trim();
    if(!nombre) return;
    await addDoc(barraCol,{nombre,[dispKey]:true});
    inp.value="";
  };

  window.toggleMozoBar = async function(id,disp) {
    await setDoc(doc(barraCol,id),{[dispKey]:disp},{merge:true});
  };

  window.eliminarMozoBar = async function(id) {
    if(!confirm("¿Eliminar este mozo de barra?")) return;
    // Liberar sus asignaciones en sectores de barra
    const slotsBar=getSlotsBar();
    const batch=writeBatch(db);
    slotsBar.forEach(sl=>{
      const asig=asignaciones[sl.slotId];
      if(asig&&asig.mozoId===id) batch.delete(doc(asigCol,sl.slotId));
    });
    batch.delete(doc(barraCol,id));
    await batch.commit();
  };

  window.abrirEdicionBar = function(id) {
    const m=mozosBar.find(m=>m.id===id);
    if(!m) return;
    const nuevo=prompt("Nombre del mozo de barra:",m.nombre);
    if(nuevo&&nuevo.trim()&&nuevo.trim()!==m.nombre)
      setDoc(doc(barraCol,id),{nombre:nuevo.trim()},{merge:true});
  };

  // ── PEONES CRUD ──
  window.agregarPeon = async function() {
    const inp=document.getElementById("nuevo-peon");
    const nombre=inp.value.trim();
    if(!nombre) return;
    await addDoc(peonesCol,{nombre,[dispKey]:true});
    inp.value="";
  };

  window.togglePeon = async function(id,disp) {
    await setDoc(doc(peonesCol,id),{[dispKey]:disp},{merge:true});
    if(!disp){
      // Liberar asignaciones de este peón
      const batch=writeBatch(db);
      Object.entries(asignaciones).filter(([k,v])=>k.startsWith("peon_")&&v.mozoId===id).forEach(([k])=>batch.delete(doc(asigCol,k)));
      await batch.commit();
    }
  };

  window.eliminarPeon = async function(id) {
    if(!confirm("¿Eliminar este peón?")) return;
    const batch=writeBatch(db);
    Object.entries(asignaciones).filter(([k,v])=>k.startsWith("peon_")&&v.mozoId===id).forEach(([k])=>batch.delete(doc(asigCol,k)));
    batch.delete(doc(peonesCol,id));
    await batch.commit();
  };

  window.abrirEdicionPeon = function(id) {
    const p=peones.find(p=>p.id===id);
    if(!p) return;
    const nuevo=prompt("Nombre del peón:",p.nombre);
    if(nuevo&&nuevo.trim()&&nuevo.trim()!==p.nombre)
      setDoc(doc(peonesCol,id),{nombre:nuevo.trim()},{merge:true});
  };

  // ── SECTORES DE PEONES ──
  window.agregarSectorPeon = async function() {
    const inp=document.getElementById("nuevo-sector-peon");
    const nombre=inp.value.trim(); if(!nombre) return;
    const snap=await getDocs(sectoresPeonCol);
    const maxOrden=snap.docs.reduce((m,d)=>Math.max(m,d.data().orden||0),0);
    await addDoc(sectoresPeonCol,{nombre,[dispKey]:true,orden:maxOrden+1});
    inp.value="";
  };

  window.toggleSectorPeon = async function(id,disp) {
    await setDoc(doc(sectoresPeonCol,id),{[dispKey]:disp},{merge:true});
    if(!disp){
      const batch=writeBatch(db);
      Object.keys(asignaciones).filter(k=>k.startsWith("peon_"+id+"___")).forEach(k=>batch.delete(doc(asigCol,k)));
      await batch.commit();
    }
  };

  window.eliminarSectorPeon = async function(id) {
    if(!confirm("¿Eliminar este sector de peones?")) return;
    const batch=writeBatch(db);
    batch.delete(doc(sectoresPeonCol,id));
    Object.keys(asignaciones).filter(k=>k.startsWith("peon_"+id+"___")).forEach(k=>batch.delete(doc(asigCol,k)));
    await batch.commit();
  };

  window.editarNombreSectorPeon = async function(id) {
    const s=sectoresPeon.find(s=>s.id===id); if(!s) return;
    const nuevo=prompt("Nombre del sector:",s.nombre);
    if(nuevo&&nuevo.trim()) await setDoc(doc(sectoresPeonCol,id),{nombre:nuevo.trim()},{merge:true});
  };

  // Popup de asignación para sectores de peones
  window.chipPeonClick = function(sectorId) {
    if(formacionBloqueada) return;
    const peonDisp=peones.filter(p=>isDisp(p));
    if(peonDisp.length===0){alert("No hay peones activos. Agregalos en la pestaña 🧹 Peones.");return;}
    const s=sectoresPeon.find(s=>s.id===sectorId);
    const label=s?s.nombre:sectorId;
    document.getElementById("popup-title").textContent="Asignar peón";
    document.getElementById("popup-sub").textContent="📍 "+label;
    // Peones ya asignados a este sector
    const asigEnSector=Object.entries(asignaciones).filter(([k])=>k.startsWith("peon_"+sectorId+"___")).map(([,v])=>v.mozoId);
    const asigSet=new Set(asigEnSector);
    // Peones ya asignados en cualquier sector
    const peonAsigGlobal=new Set(Object.entries(asignaciones).filter(([k])=>k.startsWith("peon_")).map(([,v])=>v.mozoId));
    document.getElementById("popup-opciones").innerHTML=[...peonDisp].sort((a,b)=>a.nombre.localeCompare(b.nombre)).map(p=>{
      const enEsteSector=asigSet.has(p.id);
      const enOtro=!enEsteSector&&peonAsigGlobal.has(p.id);
      const disabled=enEsteSector;
      return `<div class="popup-opcion ${disabled?"restringido":""}" onclick="${disabled?"":"window.asignarPeonSlot('"+sectorId+"','"+p.id+"')"}">
        🧹 ${p.nombre}${enEsteSector?" <small style='color:var(--text3)'>(ya en este sector)</small>":""}${enOtro?" <small style='color:var(--text3)'>(en otro sector)</small>":""}
      </div>`;
    }).join("");
    document.getElementById("popup-overlay").classList.add("show");
  };

  window.asignarPeonSlot = async function(sectorId, peonId) {
    document.getElementById("popup-overlay").classList.remove("show");
    const slotId="peon_"+sectorId+"___"+peonId;
    await setDoc(doc(asigCol,slotId),{mozoId:peonId,desde:Date.now()});
  };

  // Popup de asignación para sectores de barra
  window.chipBarClick = function(slotId) {
    if(formacionBloqueada) return;
    if(asignaciones[slotId]) return;
    const barDisp=mozosBar.filter(m=>isDisp(m));
    if(barDisp.length===0){alert("No hay mozos de barra activos. Agregalos en la pestaña 🍸 Barra.");return;}
    const sl=getSlotsBar().find(s=>s.slotId===slotId);
    const label=sl?`${sl.sectorNombre} › ${sl.ssNombre}`:slotId;
    document.getElementById("popup-title").textContent="Asignar mozo de barra";
    document.getElementById("popup-sub").textContent="📍 "+label;
    const asignados=new Set(Object.entries(asignaciones)
      .filter(([k])=>k.startsWith("bar_"))
      .map(([,v])=>v.mozoId));
    document.getElementById("popup-opciones").innerHTML=[...barDisp].sort((a,b)=>a.nombre.localeCompare(b.nombre)).map(m=>{
      const ocupado=asignados.has(m.id);
      return `<div class="popup-opcion ${ocupado?"restringido":""}" onclick="${ocupado?"":"window.asignarBarSlot('"+slotId+"','"+m.id+"')"}">
        🍸 ${m.nombre}${ocupado?" <small style='color:var(--text3)'>(ya asignado)</small>":""}
      </div>`;
    }).join("");
    document.getElementById("popup-overlay").classList.add("show");
  };

  window.asignarBarSlot = async function(slotId, mozoId) {
    document.getElementById("popup-overlay").classList.remove("show");
    await setDoc(doc(asigCol,slotId),{mozoId,desde:Date.now()});
  };




  // ===================== LONG PRESS → EDITAR DESCRIPCION =====================
  let lpTimer=null, lpTarget=null;

  window.startLongPress = function(e, slotId, tipo, sectorId, ssIdx) {
    lpTarget={slotId,tipo,sectorId,ssIdx};
    const el=e.currentTarget;
    el.classList.add("press");
    lpTimer=setTimeout(()=>{
      el.classList.remove("press");
      abrirEditDesc(slotId,tipo,sectorId,ssIdx);
    },LONG_PRESS_MS);
  };
  window.cancelLongPress = function() {
    clearTimeout(lpTimer);
    document.querySelectorAll(".ss-chip.press").forEach(el=>el.classList.remove("press"));
    lpTarget=null;
  };

  window.abrirEditDesc = function(slotId, tipo, sectorId, ssIdx) {
    const s=sectores.find(s=>s.id===sectorId);
    let desc="", label="";
    if(tipo==="ss"){
      const ss=s?.subsectores?.[ssIdx];
      desc=ss?.descripcion||""; label=`${s?.nombre} › ${ss?.nombre}`;
    } else {
      desc=s?.descripcion||""; label=s?.nombre||"";
    }
    editCtx={tipo:tipo==="ss"?"subsector":"sector", id:sectorId, idx:ssIdx, fromOp:true};
    document.getElementById("edit-title").textContent=`📝 ${label}`;
    document.getElementById("edit-nombre").value= tipo==="ss"?(s?.subsectores?.[ssIdx]?.nombre||""):(s?.nombre||"");
    document.getElementById("edit-desc").value=desc;
    document.getElementById("edit-desc-row").style.display="block";
    document.getElementById("edit-overlay").classList.add("show");
    setTimeout(()=>document.getElementById("edit-desc").focus(),100);
  };


  // ===================== GENERAR PDF =====================
  window.generarPDF = function() {
    window.print();
  };

  // ===================== MODO PRESENTACION =====================
  window.abrirPresentacion = function() {
    const overlay = document.getElementById("presentacion-overlay");
    const grid = document.getElementById("pres-grid");
    const titulo = document.getElementById("pres-titulo");
    const fecha = document.getElementById("pres-fecha");

    const turnoIcon = turno==="noche" ? "🌙" : "☀️";
    const turnoLabel = TURNO_NOMBRES[turno] || "Mañana";
    titulo.textContent = turnoIcon + " Rotación " + turnoLabel + " — " + new Date().toLocaleDateString("es-AR", {weekday:"long",day:"2-digit",month:"long"});
    fecha.textContent = new Date().toLocaleTimeString("es-AR", {hour:"2-digit",minute:"2-digit",hour12:false}) + " hs";

    let html = "";

    // Sectores normales — mismo orden que la grilla de operación, agrupados por sector
    sectores.filter(s=>isDisp(s)).forEach(s=>{
      const subs = (s.subsectores||[]).filter(ss=>isDisp(ss));

      // Sector label
      html += `<div class="pres-sector-label">${s.nombre}</div>`;
      html += `<div class="pres-sector-row">`;

      if(subs.length > 0){
        subs.forEach(ss=>{
          const slotId = s.id+"___"+ss.id;
          const asig = asignaciones[slotId];
          const mozo = asig ? mozos.find(m=>m.id===asig.mozoId) : null;
          html += presCard(ss["nombre_"+turno]||ss.nombre, mozo, false, null, asig?.comentario);
        });
      } else {
        const asig = asignaciones[s.id];
        const mozo = asig ? mozos.find(m=>m.id===asig.mozoId) : null;
        html += presCard(s.nombre, mozo, false, null, asig?.comentario);
      }

      html += `</div>`;
    });

    // Barra y Peones en línea (50/50)
    const hasBarSlotsAsig = sectoresBar.filter(s=>isDisp(s)).some(s=>(s.subsectores||[]).filter(ss=>isDisp(ss)).length>0);
    const hasPeonAsig = Object.keys(asignaciones).some(k=>k.startsWith("peon_"));
    if(hasBarSlotsAsig||hasPeonAsig){
      html += `<div style="display:flex;gap:12px;flex-wrap:wrap">`;

      if(hasBarSlotsAsig){
        html += `<div style="flex:1;min-width:200px">`;
        html += `<div class="pres-sector-label" style="color:#5a8fa0;border-color:#5a8fa0">🍸 Barra</div>`;
        sectoresBar.filter(s=>isDisp(s)).forEach(s=>{
          const subs = (s.subsectores||[]).filter(ss=>isDisp(ss));
          if(subs.length === 0) return;
          html += `<div class="pres-sector-label pres-sector-label-sub">${s.nombre}</div>`;
          html += `<div class="pres-sector-row">`;
          subs.forEach(ss=>{
            const slotId = "bar_"+s.id+"___"+ss.id;
            const asig = asignaciones[slotId];
            const mozo = asig ? mozosBar.find(m=>m.id===asig.mozoId) : null;
            html += presCard(ss.nombre, mozo, true);
          });
          html += `</div>`;
        });
        html += `</div>`;
      }

      if(hasPeonAsig){
        html += `<div style="flex:1;min-width:200px">`;
        html += `<div class="pres-sector-label" style="color:#b080d0;border-color:#b080d0">🧹 Peones</div>`;
        sectoresPeon.filter(s=>isDisp(s)).forEach(s=>{
          const peonEnSector = Object.entries(asignaciones).filter(([k])=>k.startsWith("peon_"+s.id+"___"));
          if(peonEnSector.length===0) return;
          html += `<div class="pres-sector-label pres-sector-label-sub">${s.nombre}</div>`;
          html += `<div class="pres-sector-row">`;
          peonEnSector.forEach(([,a])=>{
            const p = peones.find(p=>p.id===a.mozoId);
            html += presCard(p?p.nombre:"", p?{nombre:p.nombre,emoji:"🧹"}:null, false, "#b080d0");
          });
          html += `</div>`;
        });
        html += `</div>`;
      }

      html += `</div>`;
    }

    // Notas del turno
    const tieneNotas = notas.pesca||notas.dolar||notas.sugerencia||notas.faltantes;
    if(tieneNotas&&notasActivas){
      html += `<div class="pres-sector-label" style="color:var(--text2);border-color:var(--border2)">📝 Notas</div>`;
      html += `<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">`;
      if(notas.pesca) html+=`<div style="flex:1;min-width:120px;border:1px solid var(--border2);border-radius:6px;padding:6px 10px"><span style="font-size:9px;color:var(--text3);text-transform:uppercase">🐟 Pesca</span><div style="font-size:13px;color:var(--text);margin-top:2px;white-space:pre-wrap">${notas.pesca}</div></div>`;
      if(notas.dolar) html+=`<div style="flex:0 0 auto;border:1px solid var(--border2);border-radius:6px;padding:6px 10px"><span style="font-size:9px;color:var(--text3);text-transform:uppercase">💲 Dólar</span><div style="font-size:15px;font-weight:700;color:var(--gold2);margin-top:2px">${notas.dolar}</div></div>`;
      if(notas.sugerencia) html+=`<div style="flex:1;min-width:120px;border:1px solid var(--border2);border-radius:6px;padding:6px 10px"><span style="font-size:9px;color:var(--text3);text-transform:uppercase">💡 Sugerencia</span><div style="font-size:13px;color:var(--text);margin-top:2px;white-space:pre-wrap">${notas.sugerencia}</div></div>`;
      if(notas.faltantes) html+=`<div style="flex:1;min-width:120px;border:1px solid var(--border2);border-radius:6px;padding:6px 10px"><span style="font-size:9px;color:var(--text3);text-transform:uppercase">⚠️ Faltantes</span><div style="font-size:12px;color:#f0a060;margin-top:2px;white-space:pre-wrap">${notas.faltantes}</div></div>`;
      html += `</div>`;
    }

    grid.innerHTML = html;
    overlay.style.display = "block";
    if(overlay.requestFullscreen) overlay.requestFullscreen().catch(()=>{});
  };

  function presCard(nombre, mozo, esBarra=false, customColor=null, comentario=null) {
    const borderColor = customColor || (esBarra ? "#5a8fa0" : "var(--gold)");
    const mozoColor = customColor || (esBarra ? "#90cfe0" : "#a8d878");
    return `<div class="pres-card" style="border-color:${borderColor}">
      <div class="pres-card-nombre">${nombre}</div>
      ${mozo
        ? `<div class="pres-card-mozo" style="color:${mozoColor}">${mozo.emoji||"🍸"} ${mozo.nombre}</div>`
        : `<div class="pres-card-libre">libre</div>`
      }
      ${comentario?`<div style="font-size:9px;color:#f0c060;font-style:italic;margin-top:2px">💬 ${comentario}</div>`:""}
    </div>`;
  }

  window.cerrarPresentacion = function() {
    const overlay = document.getElementById("presentacion-overlay");
    overlay.style.display = "none";
    if(document.fullscreenElement) document.exitFullscreen().catch(()=>{});
  };

  // ===================== DRAG & DROP SECTORES =====================
  let dragSrcId = null;

  window.onDragStart = function(e, id) {
    dragSrcId = id;
    e.currentTarget.classList.add("dragging");
    e.dataTransfer.effectAllowed = "move";
  };
  window.onDragOver = function(e) {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    e.currentTarget.classList.add("drag-over");
  };
  window.onDragLeave = function(e) {
    e.currentTarget.classList.remove("drag-over");
  };
  window.onDragEnd = function(e) {
    e.currentTarget.classList.remove("dragging");
    document.querySelectorAll(".sector-cfg-row").forEach(el=>el.classList.remove("drag-over"));
  };
  window.onDrop = async function(e, targetId) {
    e.preventDefault();
    e.currentTarget.classList.remove("drag-over");
    if(!dragSrcId || dragSrcId === targetId) return;
    // Reorder: swap positions
    const ids = sectores.map(s=>s.id);
    const srcIdx = ids.indexOf(dragSrcId);
    const tgtIdx = ids.indexOf(targetId);
    if(srcIdx<0||tgtIdx<0) return;
    // Build new order
    const reordered = [...sectores];
    const [moved] = reordered.splice(srcIdx,1);
    reordered.splice(tgtIdx,0,moved);
    // Write orden to Firestore
    const batch = writeBatch(db);
    reordered.forEach((s,i)=>batch.set(doc(sectoresCol,s.id),{orden:i},{merge:true}));
    await batch.commit();
    dragSrcId = null;
  };

  // ===================== BANNER ROTACIÓN PENDIENTE =====================
  function mostrarBannerPendiente() {
    const banner=document.getElementById("pendiente-banner");
    if(!banner) return;
    banner.style.display="flex";
    banner.innerHTML=`
      <span class="pb-text">⚠️ Rotación sin confirmar</span>
      <button class="pb-btn" onclick="confirmarRotacion()">✅ Confirmar</button>
      <button class="pb-btn-descartar" onclick="descartarRotacion()">✕ Descartar</button>`;
  }

  function ocultarBannerPendiente() {
    const banner=document.getElementById("pendiente-banner");
    if(banner) banner.style.display="none";
  }

  window.descartarRotacion = async function() {
    const estaEditando=ultimaRotacionId!==null;
    const msg=estaEditando
      ? "¿Descartar los cambios? La formación original se mantiene en el historial."
      : "¿Descartar la rotación pendiente? Las asignaciones actuales se mantienen pero no se guardan en el historial.";
    if(!confirm(msg)) return;
    pendingHistorial=[];
    ocultarBannerPendiente();
    // Si estaba editando, volver a mostrar los botones post-confirmación
    if(estaEditando){
      document.getElementById("acciones-formacion").style.display="flex";
    }
  };

  // Avisar al cerrar/recargar la página si hay rotación pendiente
  window.addEventListener("beforeunload", function(e) {
    if(pendingHistorial&&pendingHistorial.length>0){
      e.preventDefault();
      e.returnValue="";
    }
  });

  // ===================== NOTAS DEL TURNO =====================
  let notasTimer=null;
  window.toggleNotas = async function(val) {
    notasActivas=val;
    await setDoc(doc(db,"meta","config"),{notasActivas:val},{merge:true});
    const ns=document.getElementById("notas-section"); if(ns) ns.style.display=val?"":"none";
  };

  window.guardarNotas = function() {
    clearTimeout(notasTimer);
    notasTimer=setTimeout(()=>{
      const data={
        pesca: document.getElementById("nota-pesca")?.value||"",
        dolar: document.getElementById("nota-dolar")?.value||"",
        sugerencia: document.getElementById("nota-sugerencia")?.value||"",
        faltantes: document.getElementById("nota-faltantes")?.value||""
      };
      notas=data;
      setDoc(doc(db,"meta","notas"+metaSuffix),data);
    },500);
  };

  window.formatDolar = function(el) {
    let v=el.value.replace(/[^0-9.,]/g,"");
    if(!v.startsWith("$")) v="$"+v;
    else v="$"+v.substring(1).replace(/[^0-9.,]/g,"");
    el.value=v;
  };

  // ===================== TABS =====================
  window.switchTab = function(id,el) {
    // Si hay rotación pendiente y se va de Operación, avisar
    if(pendingHistorial&&pendingHistorial.length>0&&id!=="operacion"){
      if(!confirm("Tenés una rotación sin confirmar. ¿Querés ir a otra pestaña sin confirmar?\n\nPodés confirmar desde el banner superior.")) return;
    }
    document.querySelectorAll(".tab-content").forEach(t=>t.classList.remove("active"));
    document.querySelectorAll(".tab-btn").forEach(b=>b.classList.remove("active"));
    document.getElementById("tab-"+id).classList.add("active");
    el.classList.add("active");
  };

  document.getElementById("nuevo-mozo").addEventListener("keydown",  e=>e.key==="Enter"&&window.agregarMozo());
  document.getElementById("nuevo-sector").addEventListener("keydown",e=>e.key==="Enter"&&window.agregarSector());
  document.getElementById("nuevo-peon").addEventListener("keydown",  e=>e.key==="Enter"&&window.agregarPeon());
  document.getElementById("nuevo-sector-peon").addEventListener("keydown",e=>e.key==="Enter"&&window.agregarSectorPeon());
  document.getElementById("edit-nombre").addEventListener("keydown", e=>e.key==="Enter"&&window.guardarEdicion());