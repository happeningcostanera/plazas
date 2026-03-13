import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
  import {
    getFirestore, collection, doc, onSnapshot,
    setDoc, deleteDoc, addDoc, query, orderBy, limit, writeBatch, getDocs, getDoc
  } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
  import { getAuth, signInAnonymously } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";

  const firebaseConfig = {
    apiKey: "AIzaSyA-kFIqSDY-kXq7xn6kLSUtnzaPSq7Apbc",
    authDomain: "plazas-5e478.firebaseapp.com",
    projectId: "plazas-5e478",
    storageBucket: "plazas-5e478.firebasestorage.app",
    messagingSenderId: "48069807441",
    appId: "1:48069807441:web:e1aa77aac78cb4ad659770"
  };

  const app = initializeApp(firebaseConfig);
  const db  = getFirestore(app);

  const LONG_PRESS_MS = 600;
  const MAX_SS_PER_SECTOR = 5;
  const MAX_SS_PER_BAR_SECTOR = 10;

  let popupSlotId=null, restMozoId=null, editCtx=null;
  let mozos=[], mozosBar=[], sectores=[], sectoresBar=[], asignaciones={}, historial=[], ultimaRotacionTs=null;
  let pendingHistorial=null, mozoRotIdx=0;

  const mozosCol    = collection(db,"mozos");
  const barraCol      = collection(db,"mozosBar");
  const sectoresBarCol = collection(db,"sectoresBar");
  const sectoresCol = collection(db,"sectores");
  const asigCol     = collection(db,"asignaciones");
  const histCol     = collection(db,"historial");

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
  onSnapshot(query(sectoresCol,orderBy("orden","asc")), snap => { sectores=snap.docs.map(d=>({id:d.id,...d.data()})); scheduleRenderAll(); });
  onSnapshot(asigCol, snap => { asignaciones={}; snap.docs.forEach(d=>{asignaciones[d.id]=d.data();}); scheduleRenderAll(); });
  onSnapshot(query(histCol,orderBy("ts","desc")), snap => { historial=snap.docs.map(d=>({id:d.id,...d.data()})); renderHistorial(); });
  onSnapshot(doc(db,"meta","ultimaRotacion"), snap => { ultimaRotacionTs=snap.exists()?snap.data().ts:null; renderUltimaRotacion(); });
  onSnapshot(doc(db,"meta","rotacion"), snap => { if(snap.exists()) mozoRotIdx=snap.data().idx||0; });

  window.addEventListener("online",  ()=>setConn(true));
  window.addEventListener("offline", ()=>setConn(false));
  setConn(navigator.onLine);
  function setConn(ok) {
    const el=document.getElementById("conn-status");
    el.textContent=ok?"● en línea":"● sin conexión";
    el.className=ok?"online":"offline";
  }

  const auth = getAuth(app);
  signInAnonymously(auth).then(()=>{
    document.getElementById("loader").style.display="none";
    document.getElementById("app").style.display="block";
    seedIfEmpty();
  }).catch(err=>{
    document.getElementById("loader").innerHTML=`<div style="color:#f0a0a0;font-size:13px;text-align:center">⛔ Error de autenticación<br><small>${err.message}</small></div>`;
  });

  async function seedIfEmpty() {
    const snap=await getDocs(mozosCol);
    if(!snap.empty) return;
    const batch=writeBatch(db);
    [["Carlos","👨‍🍳"],["Laura","👩‍🍳"],["Martín","👨‍🍳"],["Sofía","👩‍🍳"],["Diego","👨‍🍳"],["Ana","👩‍🍳"]]
      .forEach(([nombre,emoji])=>batch.set(doc(mozosCol),{nombre,emoji,disponible:true,restricciones:[]}));
    [
      {nombre:"Parque",subsectores:[]},
      {nombre:"Deck",subsectores:[{id:"ss1",nombre:"Deck 1",disponible:true},{id:"ss2",nombre:"Deck 2",disponible:true}]},
      {nombre:"Salón",subsectores:[{id:"ss1",nombre:"Salón 1",disponible:true},{id:"ss2",nombre:"Salón 2",disponible:true},{id:"ss3",nombre:"Salón 3",disponible:true},{id:"ss4",nombre:"Salón 4",disponible:true}]},
      {nombre:"Pasillo",subsectores:[]},
      {nombre:"Cava",subsectores:[]},
      {nombre:"Cafetería",subsectores:[]},
      {nombre:"Barra",subsectores:[]}
    ].forEach((s,i)=>batch.set(doc(sectoresCol),{nombre:s.nombre,disponible:true,subsectores:s.subsectores,orden:i}));
    await batch.commit();
  }

  function getSlots() {
    // Solo subsectores activos dentro de sectores activos (excluye sectores de barra)
    const slots=[];
    sectores.filter(s=>s.disponible).forEach(s=>{
      const subs=(s.subsectores||[]).filter(ss=>ss.disponible);
      subs.forEach(ss=>slots.push({slotId:s.id+"___"+ss.id,sectorId:s.id,ssId:ss.id,sectorNombre:s.nombre,ssNombre:ss.nombre}));
    });
    return slots;
  }

  function getSlotsBar() {
    // Subsectores activos de sectores de barra (colección separada)
    const slots=[];
    sectoresBar.filter(s=>s.disponible).forEach(s=>{
      const subs=(s.subsectores||[]).filter(ss=>ss.disponible);
      subs.forEach(ss=>slots.push({slotId:"bar_"+s.id+"___"+ss.id,sectorId:s.id,ssId:ss.id,sectorNombre:s.nombre,ssNombre:ss.nombre}));
    });
    return slots;
  }

  function renderUltimaRotacion() {
    const el=document.getElementById("ultima-rotacion");
    if(!el) return;
    if(!ultimaRotacionTs){el.style.display="none";return;}
    const d=new Date(ultimaRotacionTs);
    const fecha=d.toLocaleDateString("es-AR",{weekday:"long",day:"2-digit",month:"long"});
    const hora=d.toLocaleTimeString("es-AR",{hour:"2-digit",minute:"2-digit"});
    el.style.display="block";
    el.innerHTML=`<div style="padding:10px 12px;border-radius:8px 8px 0 0;background:linear-gradient(135deg,rgba(201,147,58,.15),rgba(232,184,102,.08));border:1px solid var(--gold);border-bottom:none;display:flex;align-items:baseline;justify-content:space-between;flex-wrap:wrap;gap:6px">
      <span style="font-size:15px;font-weight:700;color:var(--gold2)">🌅 TURNO MAÑANA — ${fecha}</span>
      <span style="font-size:11px;color:var(--text3)">confirmada ${hora} · <span style="cursor:pointer;color:var(--gold);text-decoration:underline" onclick="generarPDF()">📄 PDF</span></span>
    </div>`;
  }

  function renderAll() {
    renderStats(); renderAvisoGlobal(); renderAvisoRotacion(); renderUltimaRotacion();
    const btnLib=document.getElementById("btn-liberar-todo");
    if(btnLib) btnLib.style.display=Object.keys(asignaciones).length>0?"inline-block":"none";
    renderSectoresGrid(); renderLibres(); renderPersonal(); renderSectoresConfig(); renderBarraGrid();
  }

  function renderStats() {
    const mDisp=mozos.filter(m=>m.disponible);
    const slots=getSlots();
    const libres=mDisp.filter(m=>!Object.values(asignaciones).some(a=>a.mozoId===m.id));
    const asigNormales=Object.keys(asignaciones).filter(id=>!id.startsWith("bar_")).length;
    document.getElementById("st-mozos").textContent=mDisp.length;
    document.getElementById("st-slots").textContent=slots.length;
    document.getElementById("st-asig").textContent=asigNormales;
    document.getElementById("st-libres").textContent=libres.length;
  }

  function renderAvisoGlobal() {
    const el=document.getElementById("aviso-global");
    const slots=getSlots();
    const mozosLibres=mozos.filter(m=>m.disponible&&!Object.values(asignaciones).some(a=>a.mozoId===m.id));
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

  function renderSectoresGrid() {
    const grid=document.getElementById("sectores-grid");
    if(sectores.length===0){grid.innerHTML=`<div class="empty">No hay sectores. Creá uno en Sectores.</div>`;return;}

    let html=`<div class="slots-grid">`;

    let firstSector=true;
    sectores.forEach(s=>{
      const subs=s.subsectores||[];
      const subsActivos=subs.filter(ss=>ss.disponible);
      const tieneSubsActivos=subsActivos.length>0;

      if(!s.disponible) return; // skip inactive sectors entirely

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
          html+=`<span class="ss-nombre">${ss.nombre}</span>`;
          if(mozo){
            html+=`<span class="ss-mozo">${mozo.emoji} ${mozo.nombre}</span>`;
            if(ss.descripcion) html+=`<span class="ss-desc">${ss.descripcion}</span>`;
            html+=`<span class="ss-hora">desde ${asig.desde?fmtHora(asig.desde):""}</span>`;
            html+=`<button class="ss-liberar" onclick="event.stopPropagation();liberarSlot('${slotId}')">Liberar</button>`;
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
          html+=`<span class="ss-mozo">${mozo.emoji} ${mozo.nombre}</span>`;
          if(s.descripcion) html+=`<span class="ss-desc">${s.descripcion}</span>`;
          html+=`<span class="ss-hora">desde ${asig.desde?fmtHora(asig.desde):""}</span>`;
          html+=`<button class="ss-liberar" onclick="event.stopPropagation();liberarSlot('${slotId}')">Liberar</button>`;
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
    const libres=mozos.filter(m=>m.disponible&&!Object.values(asignaciones).some(a=>a.mozoId===m.id));
    document.getElementById("libres-section").style.display=libres.length?"block":"none";
    document.getElementById("libres-list").innerHTML=libres.map(m=>`<span class="chip on">${m.emoji} ${m.nombre}</span>`).join("");
    renderBarraGrid();
  }

  function renderBarraGrid() {
    // Operación barra
    // Construir HTML de grilla de barra (reutilizado en dos lugares)
    const buildBarraHtml = () => {
      let html="";
      sectoresBar.filter(s=>s.disponible).forEach(s=>{
        const subs=(s.subsectores||[]).filter(ss=>ss.disponible);
        html+=`<div class="sector-row"><div class="sector-label">${s.nombre}</div><div class="sector-chips">`;
        if(subs.length===0){
          html+=`<span style="font-size:11px;color:var(--text3);font-style:italic">Sin sub sectores activos</span>`;
        } else {
          subs.forEach(ss=>{
            const slotId="bar_"+s.id+"___"+ss.id;
            const asig=asignaciones[slotId];
            const mozo=asig?mozosBar.find(m=>m.id===asig.mozoId):null;
            html+=`<div class="ss-chip ${mozo?"ocupada":"libre"}" onclick="chipBarClick('${slotId}')">`;
            html+=`<span class="ss-nombre">${ss.nombre}</span>`;
            if(mozo){
              html+=`<span class="ss-mozo">🍸 ${mozo.nombre}</span>`;
              html+=`<span class="ss-hora">desde ${asig.desde?fmtHora(asig.desde):""}</span>`;
              html+=`<button class="ss-liberar" onclick="event.stopPropagation();liberarSlot('${slotId}')">Liberar</button>`;
            } else {
              html+=`<span class="ss-libre-txt">Libre — tap para asignar</span>`;
            }
            html+=`</div>`;
          });
        }
        html+=`</div></div>`;
      });
      return html;
    };

    const hasBarSectors = sectoresBar.filter(s=>s.disponible).length>0;

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
    const barLibres=mozosBar.filter(m=>m.disponible&&!mozoBarAsignadosIds.has(m.id));
    const barLibresSec=document.getElementById("barra-libres-section");
    const barLibresList=document.getElementById("barra-libres-list");
    if(barLibresSec&&barLibresList){
      barLibresSec.style.display = (hasBarSectors && barLibres.length) ? "block" : "none";
      barLibresList.innerHTML = barLibres.map(m=>`<span class="chip on">🍸 ${m.nombre}</span>`).join("");
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
          <span class="ss-cfg-name ${ss.disponible?"":"off"}">${ss.nombre}</span>
          <button class="btn btn-ghost" onclick="editarNombreSubsectorBar('${s.id}',${i})">✏️</button>
          <button class="btn ${ss.disponible?"btn-gold":"btn-green"}" onclick="toggleSubsectorBar('${s.id}',${i},${!ss.disponible})">${ss.disponible?"Desact.":"Activ."}</button>
          <button class="btn btn-red" onclick="eliminarSubsectorBar('${s.id}',${i})">✕</button>
        </div>`).join("");
      return `<div class="sector-cfg-block">
        <div class="sector-cfg-header" style="flex-wrap:wrap">
          <span class="sector-cfg-name ${s.disponible?"":"off"}">${s.nombre}</span>
          <button class="btn btn-ghost" onclick="editarNombreSectorBar('${s.id}')">✏️</button>
          <button class="btn ${s.disponible?"btn-gold":"btn-green"}" onclick="toggleSectorBar('${s.id}',${!s.disponible})">${s.disponible?"Desact.":"Activ."}</button>
          <button class="btn btn-red" onclick="eliminarSectorBar('${s.id}')">✕</button>
        </div>
        ${subsHtml}
        ${canAdd?`<div class="input-row" style="margin-top:8px">
          <input type="text" id="nuevo-ss-bar-${s.id}" placeholder="Nuevo sub sector"/>
          <button class="btn btn-ghost" onclick="agregarSubsectorBar('${s.id}')">+ Sub sector</button>
        </div>`:`<div style="font-size:11px;color:var(--text3)">Máximo 10 sub sectores</div>`}
      </div>`;
    }).join("");
  }

  function renderPersonal() {
    document.getElementById("mozos-list").innerHTML=mozos.map(m=>{
      const restTags=(m.restricciones||[]).map(slotId=>{
        const sl=getSlots().find(s=>s.slotId===slotId);
        const label=sl?(sl.ssNombre?`${sl.sectorNombre} › ${sl.ssNombre}`:sl.sectorNombre):slotId;
        return `<span class="rest-tag">🚫 ${label} <button onclick="quitarRestriccion('${m.id}','${slotId}')">×</button></span>`;
      }).join("");
      return `<div class="person-row">
        <span style="font-size:18px">${m.emoji}</span>
        <div class="person-info">
          <div class="person-name ${m.disponible?"":"off"}">${m.nombre}</div>
          <div class="restricciones-tags">${restTags}</div>
        </div>
        <div class="person-actions">
          <button class="btn btn-orange" onclick="abrirRestricciones('${m.id}')">🚫</button>
          <button class="btn btn-ghost"  onclick="abrirEdicion('mozo','${m.id}')">✏️</button>
          <button class="btn ${m.disponible?"btn-gold":"btn-green"}" onclick="toggleMozo('${m.id}',${!m.disponible})">${m.disponible?"Desactivar":"Activar"}</button>
          <button class="btn btn-red" onclick="eliminarMozo('${m.id}')">✕</button>
        </div>
      </div>`;
    }).join("");

    // Mozos de barra
    const barList=document.getElementById("mozos-bar-list");
    if(barList) barList.innerHTML=mozosBar.length===0
      ? `<div class="empty">No hay mozos de barra aún.</div>`
      : mozosBar.map(m=>`<div class="person-row">
        <span style="font-size:18px">🍸</span>
        <div class="person-info">
          <div class="person-name ${m.disponible?"":"off"}">${m.nombre}</div>
        </div>
        <div class="person-actions">
          <button class="btn btn-ghost" onclick="abrirEdicionBar('${m.id}')">✏️</button>
          <button class="btn ${m.disponible?"btn-gold":"btn-green"}" onclick="toggleMozoBar('${m.id}',${!m.disponible})">${m.disponible?"Desactivar":"Activar"}</button>
          <button class="btn btn-red" onclick="eliminarMozoBar('${m.id}')">✕</button>
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
          <span class="ss-cfg-name ${ss.disponible?"":"off"}" style="min-width:80px">${ss.nombre}</span>
          ${ss.descripcion?`<span style="font-size:10px;color:var(--text3);flex:1;font-style:italic">${ss.descripcion}</span>`:""}
          <button class="btn btn-ghost" onclick="abrirEdicion('subsector','${s.id}',${i})">✏️</button>
          <button class="btn ${ss.disponible?"btn-gold":"btn-green"}" onclick="toggleSubsector('${s.id}',${i},${!ss.disponible})">${ss.disponible?"Desact.":"Activ."}</button>
          <button class="btn btn-red" onclick="eliminarSubsector('${s.id}',${i})">✕</button>
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
          <span class="sector-cfg-name ${s.disponible?"":"off"}">${s.nombre}</span>
          ${s.descripcion?`<span style="font-size:10px;color:var(--text3);flex:1;font-style:italic">${s.descripcion}</span>`:""}
          <button class="btn btn-ghost" onclick="abrirEdicion('sector','${s.id}')">✏️</button>
          <button class="btn ${s.evitarRepetirSector?"btn-orange":"btn-ghost"}" onclick="toggleEvitarRepetir('${s.id}',${!s.evitarRepetirSector})" title="Evitar repetir sector completo en ciclo siguiente">${s.evitarRepetirSector?"🔒 No repetir sector":"🔓 Permitir repetir"}</button>
          <button class="btn ${s.disponible?"btn-gold":"btn-green"}" onclick="toggleSector('${s.id}',${!s.disponible})">${s.disponible?"Desact.":"Activ."}</button>
          <button class="btn btn-red" onclick="eliminarSector('${s.id}')">✕</button>
        </div>
        <div class="ss-cfg-list">${subsHtml}</div>
        ${canAdd?`<div class="add-ss-row">
          <input type="text" id="new-ss-${s.id}" placeholder="Nuevo sub sector" style="font-size:12px;padding:6px 10px"/>
          <button class="btn btn-green" onclick="agregarSubsector('${s.id}')">+ Sub</button>
        </div>`:`<div style="font-size:10px;color:var(--text3);padding-left:12px;margin-top:6px">Máximo 5 sub sectores</div>`}
      </div>`;
    }).join("");
  }

  function renderHistorial() {
    // Leer filtros ANTES de tocar el DOM del selector
    const filtroMozo  = document.getElementById("filtro-mozo")?.value || "";
    const filtroFecha = document.getElementById("filtro-fecha")?.value || "";

    // Actualizar opciones del selector sin destruir la selección actual
    const sel = document.getElementById("filtro-mozo");
    if (sel) {
      const nombres = [...new Set(historial.map(h=>h.mozo).filter(Boolean))].sort();
      // Solo reconstruir si cambió la lista de mozos
      const optsActuales = [...sel.options].map(o=>o.value).filter(Boolean).join(",");
      const optsNuevas = nombres.join(",");
      if (optsActuales !== optsNuevas) {
        sel.innerHTML = `<option value="">Todos los mozos</option>` + nombres.map(n=>`<option value="${n}">${n}</option>`).join("");
        if (filtroMozo) sel.value = filtroMozo;
      }
    }

    let filtrado = historial;
    if (filtroMozo)  filtrado = filtrado.filter(h => h.mozo === filtroMozo);
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
        if(!h.mozo) return;
        const slotLabel=h.subsector||h.sector||"";
        if(!slotLabel) return;
        const key=`${h.mozo}||${slotLabel}`;
        countMap.set(key,(countMap.get(key)||0)+1);
      });

      document.getElementById("hist-rows").innerHTML = filtrado.map(h => {
        const d = h.ts ? new Date(h.ts) : null;
        const hora  = d ? d.toLocaleTimeString("es-AR",{hour:"2-digit",minute:"2-digit"}) : "--:--";
        const fecha = d ? d.toLocaleDateString("es-AR",{day:"2-digit",month:"2-digit"}) : "";
        const slotLabel = h.subsector || h.sector || "";
        const key = `${h.mozo||""}||${slotLabel}`;
        const count = countMap.get(key)||0;
        return `<div class="hist-row">
          <span class="hist-hora">${hora}</span>
          <span style="font-size:11px;color:var(--text3)">${fecha}</span>
          <span>${h.mozo||""}</span>
          <span style="color:var(--gold2)">${h.sector||""}</span>
          <span style="color:var(--text2)">${h.subsector||""}</span>
          <span class="hist-count-badge" title="${h.mozo} estuvo ${count}x en ${slotLabel}">${count}</span>
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
    let base = historial;
    if (filtroFecha) base = base.filter(h => {
      if (!h.ts) return false;
      return toYMD(h.ts) === filtroFecha;
    });
    if (filtroMozo) base = base.filter(h => h.mozo === filtroMozo);

    // Obtener mozos únicos
    const mozosU = [...new Set(base.map(h=>h.mozo).filter(Boolean))].sort();
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
      if(!h.mozo) return;
      const lbl = h.subsector ? `${h.sector} › ${h.subsector}` : (h.sector||"");
      if(!lbl) return;
      const key = `${h.mozo}||${lbl}`;
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
      const filas = base.filter(h => h.mozo === mozo);
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

  window.renderHistorial = renderHistorial;
  window.limpiarFiltros = function() {
    const sel = document.getElementById("filtro-mozo");
    const fecha = document.getElementById("filtro-fecha");
    if (sel) sel.value = "";
    if (fecha) fecha.value = "";
    renderHistorial();
  };

  // ===================== ROTACION =====================
  function renderAvisoRotacion() {
    const aviso=document.getElementById("rotar-aviso");
    const btn=document.getElementById("btn-rotar");
    const hint=document.getElementById("rotar-hint");
    if(!aviso||!btn) return;
    const mDisp=mozos.filter(m=>m.disponible);
    const slots=getSlots();
    const m=mDisp.length, p=slots.length;
    const asigCount=Object.keys(asignaciones).length;
    if(asigCount>0&&asigCount>=p&&p>0){
      aviso.style.display="block";hint.style.display="none";
      aviso.innerHTML=`<div class="aviso warn"><strong>⚠️ Todos los sub sectores están ocupados</strong>Liberá antes de volver a rotar.</div>`;
      btn.disabled=true;btn.style.opacity=".4";return;
    }
    if(m===0||p===0){
      aviso.style.display="block";hint.style.display="none";
      aviso.innerHTML=`<div class="aviso error"><strong>⛔ Sin mozos o sub sectores activos</strong>Activá mozos en Personal y sub sectores en Sectores.</div>`;
      btn.disabled=true;btn.style.opacity=".4";return;
    }
    hint.style.display="block";aviso.style.display="block";
    if(m===p){
      btn.disabled=false;btn.style.opacity="1";
      aviso.innerHTML=`<div class="aviso info"><strong>✅ Listo para rotar</strong>${m} mozo${m>1?"s":""} · ${p} sub sector${p>1?"es":""}.</div>`;
    } else if(m<p){
      btn.disabled=true;btn.style.opacity=".4";
      aviso.innerHTML=`<div class="aviso warn"><strong>⚠️ Menos mozos que sub sectores activos</strong>${m} mozo${m>1?"s":""} para ${p} sub sectores. Ajustá en Personal o Sectores.</div>`;
    } else {
      btn.disabled=true;btn.style.opacity=".4";
      aviso.innerHTML=`<div class="aviso warn"><strong>⚠️ Más mozos que sub sectores activos</strong>${m} mozos para ${p} sub sectores. Ajustá en Personal o Sectores.</div>`;
    }
  }

  window.rotarAutomatico = async function() {
    const mDisp=mozos.filter(m=>m.disponible);
    const slots=getSlots();
    if(mDisp.length===0||slots.length===0||mDisp.length!==slots.length) return;
    const ahora=Date.now();
    const n=mDisp.length;

    // Leer índice circular desde Firestore
    const idxSnap=await getDoc(doc(db,"meta","rotacion"));
    const idx=idxSnap.exists()?(idxSnap.data().idx||0):0;

    // Calcular cuántas veces cada mozo estuvo en cada slot (historial completo)
    // Usamos slotId como clave única para evitar confusión entre subsectores con mismo nombre
    const conteo={};
    mDisp.forEach(m=>{ conteo[m.id]={}; slots.forEach(sl=>{ conteo[m.id][sl.slotId]=0; }); });
    for(const h of historial){
      const mozo=mDisp.find(m=>m.nombre===h.mozo);
      if(!mozo) continue;
      // Buscar el slotId que corresponde a este registro del historial
      const sl=slots.find(s=>s.ssNombre===h.subsector&&s.sectorNombre===h.sector
                           ||(!h.subsector&&s.sectorNombre===h.sector&&!s.ssNombre));
      if(sl&&conteo[mozo.id]) conteo[mozo.id][sl.slotId]=(conteo[mozo.id][sl.slotId]||0)+1;
    }

    const resultado=[], advertencias=[];
    const mozosUsados=new Set();

    for(let pi=0;pi<slots.length;pi++){
      const slot=slots[pi];
      // Sectores con regla "evitar repetir sector"
      const sectoresEvitar=sectores.filter(s=>s.evitarRepetirSector).map(s=>s.id);
      const nSlots=slots.length;

      // ¿Estuvo este mozo en este sector en la última rotación completa?
      const estuvoEnSectorReciente=(mozoId,sectorId)=>{
        if(!sectoresEvitar.includes(sectorId)) return false;
        let count=0;
        for(const h of historial){
          const mo=mDisp.find(m=>m.nombre===h.mozo);
          if(!mo||mo.id!==mozoId) continue;
          count++;
          if(count>nSlots) break;
          const sl=slots.find(s=>s.ssNombre===h.subsector&&s.sectorNombre===h.sector);
          if(sl&&sl.sectorId===sectorId) return true;
        }
        return false;
      };

      // Construir candidatos: excluir usados y restringidos
      // Ordenar: 1) sin penalización de sector, 2) menos veces en este slot, 3) orden circular
      const candidatos=[];
      for(let mi=0;mi<n;mi++){
        const circularPos=(idx+pi+mi)%n;
        const mozo=mDisp[circularPos];
        if(mozosUsados.has(mozo.id)) continue;
        if((mozo.restricciones||[]).includes(slot.slotId)) continue;
        const penSector=estuvoEnSectorReciente(mozo.id,slot.sectorId)?1:0;
        candidatos.push({mozo,veces:(conteo[mozo.id]?.[slot.slotId]||0),penSector,circularPos:mi});
      }
      if(candidatos.length===0){
        advertencias.push(`⛔ ${slot.ssNombre||slot.sectorNombre}: sin mozo (revisar restricciones)`);
        continue;
      }
      candidatos.sort((a,b)=>a.penSector-b.penSector||a.veces-b.veces||a.circularPos-b.circularPos);
      const elegido=candidatos[0].mozo;
      resultado.push({slotId:slot.slotId,mozoId:elegido.id,slot});
      mozosUsados.add(elegido.id);
    }

    if(resultado.length===0){
      alert("⛔ No se pudo rotar:\n\n"+advertencias.join("\n"));
      return;
    }

    const nuevoIdx=(idx+1)%n;
    const batch=writeBatch(db);
    resultado.forEach(({slotId,mozoId})=>batch.set(doc(asigCol,slotId),{mozoId,desde:ahora}));
    batch.set(doc(db,"meta","rotacion"),{idx:nuevoIdx},{merge:true});
    await batch.commit();

    mozoRotIdx=nuevoIdx;
    pendingHistorial=resultado.map(({mozoId,slot},i)=>{
      const mozo=mozos.find(m=>m.id===mozoId);
      return {mozo:mozo.nombre,sector:slot.sectorNombre,subsector:slot.ssNombre||"",ts:ahora-i};
    });
    document.getElementById("btn-confirmar").style.display="block";
    if(advertencias.length>0) setTimeout(()=>alert("Rotación con advertencias:\n\n"+advertencias.join("\n")),300);
  };

  window.confirmarRotacion = async function() {
    if(pendingHistorial.length===0) return;
    const ahora=Date.now();
    const batch=writeBatch(db);
    pendingHistorial.forEach(h=>batch.set(doc(histCol),h));
    // Guardar timestamp de última confirmación
    batch.set(doc(db,"meta","ultimaRotacion"),{ts:ahora},{merge:true});
    await batch.commit();
    pendingHistorial=[];
    document.getElementById("btn-confirmar").style.display="none";
    document.getElementById("btn-pdf").style.display="block";
    document.getElementById("btn-presentacion").style.display="block";
  };

  // ===================== POPUP ASIGNACION =====================
  window.chipClick = function(slotId) {
    if(asignaciones[slotId]) return;
    abrirPopup(slotId);
  };
  window.abrirPopup = function(slotId) {
    popupSlotId=slotId;
    const slot=getSlots().find(s=>s.slotId===slotId);
    const label=slot?(slot.ssNombre?`${slot.sectorNombre} › ${slot.ssNombre}`:slot.sectorNombre):slotId;
    const libres=mozos.filter(m=>m.disponible&&!Object.values(asignaciones).some(a=>a.mozoId===m.id));
    document.getElementById("popup-title").textContent="Asignar mozo";
    document.getElementById("popup-sub").textContent="📍 "+label;
    const opc=document.getElementById("popup-opciones");
    if(libres.length===0) opc.innerHTML=`<div class="empty">No hay mozos libres.</div>`;
    else opc.innerHTML=libres.map(m=>{
      const rest=(m.restricciones||[]).includes(slotId);
      return `<div class="mozo-option ${rest?"restringido":""}" ${rest?"":` onclick="asignarManual('${m.id}')"`}>
        <span class="emoji">${m.emoji}</span><span class="mname">${m.nombre}</span>
        ${rest?`<span class="rest-label">🚫 restringido</span>`:""}
      </div>`;
    }).join("");
    document.getElementById("popup-overlay").classList.add("show");
  };
  window.cerrarPopup = function() { document.getElementById("popup-overlay").classList.remove("show"); popupSlotId=null; };
  window.asignarManual = async function(mozoId) {
    if(!popupSlotId) return;
    const mozo=mozos.find(m=>m.id===mozoId);
    const slot=getSlots().find(s=>s.slotId===popupSlotId);
    const ahora=Date.now();
    const batch=writeBatch(db);
    batch.set(doc(asigCol,popupSlotId),{mozoId,desde:ahora});
    batch.set(doc(histCol),{mozo:mozo.nombre,sector:slot.sectorNombre,subsector:slot.ssNombre||"",ts:ahora});
    await batch.commit();
    cerrarPopup();
  };

  // ===================== LIBERAR =====================
  window.liberarSlot = async function(slotId) { await deleteDoc(doc(asigCol,slotId)); };
  window.liberarTodo = async function() {
    const ocupadas=Object.keys(asignaciones);
    if(ocupadas.length===0) return;
    if(!confirm(`¿Liberar los ${ocupadas.length} slot${ocupadas.length>1?"s":""} asignados (mozos y barra)?`)) return;
    const batch=writeBatch(db);
    ocupadas.forEach(id=>batch.delete(doc(asigCol,id)));
    await batch.commit();
    pendingHistorial=[];
    document.getElementById("btn-confirmar").style.display="none";
    document.getElementById("btn-pdf").style.display="none";
    document.getElementById("btn-presentacion").style.display="none";
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

  // ===================== EDICION =====================
  window.abrirEdicion = function(tipo,id,idx) {
    editCtx={tipo,id,idx};
    let nombre="", desc="", showDesc=false;
    if(tipo==="mozo"){ nombre=mozos.find(m=>m.id===id)?.nombre||""; }
    else if(tipo==="sector"){ const s=sectores.find(s=>s.id===id); nombre=s?.nombre||""; desc=s?.descripcion||""; showDesc=true; }
    else if(tipo==="subsector"){ const ss=sectores.find(s=>s.id===id)?.subsectores?.[idx]; nombre=ss?.nombre||""; desc=ss?.descripcion||""; showDesc=true; }
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
      const nombreViejo=mozos.find(m=>m.id===id)?.nombre;
      await setDoc(doc(mozosCol,id),{nombre},{merge:true});
      if(nombreViejo&&nombreViejo!==nombre){const snap=await getDocs(histCol);const batch=writeBatch(db);snap.docs.forEach(d=>{if(d.data().mozo===nombreViejo)batch.set(d.ref,{mozo:nombre},{merge:true});});await batch.commit();}
    } else if(tipo==="sector"){
      const nombreViejo=sectores.find(s=>s.id===id)?.nombre;
      await setDoc(doc(sectoresCol,id),{nombre,descripcion:desc},{merge:true});
      if(nombreViejo&&nombreViejo!==nombre){const snap=await getDocs(histCol);const batch=writeBatch(db);snap.docs.forEach(d=>{if(d.data().sector===nombreViejo)batch.set(d.ref,{sector:nombre},{merge:true});});await batch.commit();}
    } else if(tipo==="subsector"){
      const s=sectores.find(s=>s.id===id);
      const subs=[...(s.subsectores||[])];
      const nombreViejo=subs[idx]?.nombre;
      subs[idx]={...subs[idx],nombre,descripcion:desc};
      await setDoc(doc(sectoresCol,id),{subsectores:subs},{merge:true});
      if(nombreViejo&&nombreViejo!==nombre){const snap=await getDocs(histCol);const batch=writeBatch(db);snap.docs.forEach(d=>{if(d.data().subsector===nombreViejo)batch.set(d.ref,{subsector:nombre},{merge:true});});await batch.commit();}
    }
    cerrarEdicion();
  };

  // ===================== SECTORES CRUD =====================
  window.agregarSector = async function() {
    const inp=document.getElementById("nuevo-sector");
    const nombre=inp.value.trim();
    if(!nombre) return;
    const maxOrden=sectores.length>0?Math.max(...sectores.map(s=>s.orden??0))+1:0;
    await addDoc(sectoresCol,{nombre,disponible:true,subsectores:[],orden:maxOrden});
    inp.value="";
  };

  // ── SECTORES DE BARRA ──
  window.agregarSectorBar = async function() {
    const inp=document.getElementById("nuevo-sector-bar");
    const nombre=inp.value.trim(); if(!nombre) return;
    const snap=await getDocs(sectoresBarCol);
    const maxOrden=snap.docs.reduce((m,d)=>Math.max(m,d.data().orden||0),0);
    await addDoc(sectoresBarCol,{nombre,disponible:true,orden:maxOrden+1,subsectores:[]});
    inp.value="";
  };

  window.toggleSectorBar = async function(id,disp) {
    await setDoc(doc(sectoresBarCol,id),{disponible:disp},{merge:true});
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
    const subs=[...(s.subsectores||[]),{id:"ss"+Date.now(),nombre,disponible:true,descripcion:""}];
    await setDoc(doc(sectoresBarCol,sectorId),{subsectores:subs},{merge:true});
    inp.value="";
  };

  window.toggleSubsectorBar = async function(sectorId,ssIdx,disp) {
    const s=sectoresBar.find(s=>s.id===sectorId); if(!s) return;
    const subs=[...(s.subsectores||[])];
    subs[ssIdx]={...subs[ssIdx],disponible:disp};
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

  window.toggleSector = async function(id,disp) {
    await setDoc(doc(sectoresCol,id),{disponible:disp},{merge:true});
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
    subs.push({id:"ss"+Date.now(),nombre,disponible:true});
    await setDoc(doc(sectoresCol,sectorId),{subsectores:subs},{merge:true});
    inp.value="";
  };
  window.toggleSubsector = async function(sectorId,idx,disp) {
    const s=sectores.find(s=>s.id===sectorId);
    const subs=[...(s.subsectores||[])];
    const ssId=subs[idx].id;
    subs[idx]={...subs[idx],disponible:disp};
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
    await setDoc(doc(mozosCol,id),{disponible:disp},{merge:true});
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
    await addDoc(mozosCol,{nombre,emoji:emojis[mozos.length%3],disponible:true,restricciones:[]});
    inp.value="";
  };

  window.agregarMozoBar = async function() {
    const inp=document.getElementById("nuevo-mozo-bar");
    const nombre=inp.value.trim();
    if(!nombre) return;
    await addDoc(barraCol,{nombre,disponible:true});
    inp.value="";
  };

  window.toggleMozoBar = async function(id,disp) {
    await setDoc(doc(barraCol,id),{disponible:disp},{merge:true});
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

  // Popup de asignación para sectores de barra
  window.chipBarClick = function(slotId) {
    if(asignaciones[slotId]) return;
    const barDisp=mozosBar.filter(m=>m.disponible);
    if(barDisp.length===0){alert("No hay mozos de barra activos. Agregalos en la pestaña 🍸 Barra.");return;}
    const sl=getSlotsBar().find(s=>s.slotId===slotId);
    const label=sl?`${sl.sectorNombre} › ${sl.ssNombre}`:slotId;
    document.getElementById("popup-title").textContent="Asignar mozo de barra";
    document.getElementById("popup-sub").textContent="📍 "+label;
    const asignados=new Set(Object.entries(asignaciones)
      .filter(([k])=>k.startsWith("bar_"))
      .map(([,v])=>v.mozoId));
    document.getElementById("popup-opciones").innerHTML=barDisp.map(m=>{
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

  // ===================== HISTORIAL =====================
  window.limpiarHistorial = async function() {
    if(!confirm("¿Limpiar todo el historial?")) return;
    const snap=await getDocs(histCol);
    const batch=writeBatch(db);
    snap.docs.forEach(d=>batch.delete(d.ref));
    await batch.commit();
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

    titulo.textContent = "🌅 Rotación — " + new Date().toLocaleDateString("es-AR", {weekday:"long",day:"2-digit",month:"long"});
    fecha.textContent = new Date().toLocaleTimeString("es-AR", {hour:"2-digit",minute:"2-digit"}) + " hs";

    let html = "";

    // Sectores normales — mismo orden que la grilla de operación, agrupados por sector
    sectores.filter(s=>s.disponible).forEach(s=>{
      const subs = (s.subsectores||[]).filter(ss=>ss.disponible);

      // Sector label
      html += `<div class="pres-sector-label">${s.nombre}</div>`;
      html += `<div class="pres-sector-row">`;

      if(subs.length > 0){
        subs.forEach(ss=>{
          const slotId = s.id+"___"+ss.id;
          const asig = asignaciones[slotId];
          const mozo = asig ? mozos.find(m=>m.id===asig.mozoId) : null;
          html += presCard(ss.nombre, mozo);
        });
      } else {
        const asig = asignaciones[s.id];
        const mozo = asig ? mozos.find(m=>m.id===asig.mozoId) : null;
        html += presCard(s.nombre, mozo);
      }

      html += `</div>`;
    });

    // Sectores de barra
    const hasBarSlotsAsig = sectoresBar.filter(s=>s.disponible).some(s=>(s.subsectores||[]).filter(ss=>ss.disponible).length>0);
    if(hasBarSlotsAsig){
      html += `<div class="pres-sector-label" style="color:#5a8fa0;border-color:#5a8fa0">🍸 Barra</div>`;
      sectoresBar.filter(s=>s.disponible).forEach(s=>{
        const subs = (s.subsectores||[]).filter(ss=>ss.disponible);
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
    }

    grid.innerHTML = html;
    overlay.style.display = "block";
    if(overlay.requestFullscreen) overlay.requestFullscreen().catch(()=>{});
  };

  function presCard(nombre, mozo, esBarra=false) {
    const borderColor = esBarra ? "#5a8fa0" : "var(--gold)";
    const mozoColor = esBarra ? "#90cfe0" : "#a8d878";
    return `<div class="pres-card" style="border-color:${borderColor}">
      <div class="pres-card-nombre">${nombre}</div>
      ${mozo
        ? `<div class="pres-card-mozo" style="color:${mozoColor}">${mozo.emoji||"🍸"} ${mozo.nombre}</div>`
        : `<div class="pres-card-libre">libre</div>`
      }
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

  // ===================== TABS =====================
  window.switchTab = function(id,el) {
    document.querySelectorAll(".tab-content").forEach(t=>t.classList.remove("active"));
    document.querySelectorAll(".tab-btn").forEach(b=>b.classList.remove("active"));
    document.getElementById("tab-"+id).classList.add("active");
    el.classList.add("active");
  };

  document.getElementById("nuevo-mozo").addEventListener("keydown",  e=>e.key==="Enter"&&window.agregarMozo());
  document.getElementById("nuevo-sector").addEventListener("keydown",e=>e.key==="Enter"&&window.agregarSector());
  document.getElementById("edit-nombre").addEventListener("keydown", e=>e.key==="Enter"&&window.guardarEdicion());