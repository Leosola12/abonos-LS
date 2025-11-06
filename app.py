import streamlit as st
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
import shutil
import csv
import io
from decimal import Decimal, InvalidOperation
import pandas as pd

# ======= Config =======
DB_FILE = "abonos.db"
BACKUP_DIR = "backups"
LOG_FILE = "abonos.log"

# ======= DB helpers =======
@st.cache_resource
def get_conn():
    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con

def init_db():
    created = not Path(DB_FILE).exists()
    con = get_conn()
    cur = con.cursor()
    cur.executescript("""
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        cuit TEXT,
        contacto TEXT,
        email TEXT,
        telefono TEXT,
        direccion TEXT,
        activo INTEGER DEFAULT 1,
        notas TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS planes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER NOT NULL,
        descripcion TEXT,
        importe REAL NOT NULL,
        fecha_inicio TEXT NOT NULL,
        fecha_fin TEXT,
        periodicidad TEXT DEFAULT 'mensual',
        activo INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS devengamientos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER NOT NULL,
        plan_id INTEGER,
        periodo_anyo INTEGER NOT NULL,
        periodo_mes INTEGER NOT NULL,
        importe REAL NOT NULL,
        fecha_devengada TEXT NOT NULL,
        notas TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE,
        FOREIGN KEY (plan_id) REFERENCES planes(id) ON DELETE SET NULL,
        UNIQUE(cliente_id, plan_id, periodo_anyo, periodo_mes)
    );

    CREATE TABLE IF NOT EXISTS cobros (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER NOT NULL,
        fecha TEXT NOT NULL,
        importe REAL NOT NULL,
        medio TEXT,
        referencia TEXT,
        observacion TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS devengamientos_cobros (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        devengamiento_id INTEGER NOT NULL,
        cobro_id INTEGER NOT NULL,
        monto REAL NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (devengamiento_id) REFERENCES devengamientos(id) ON DELETE CASCADE,
        FOREIGN KEY (cobro_id) REFERENCES cobros(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS ajustes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER NOT NULL,
        fecha TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        monto REAL NOT NULL,
        tipo TEXT,
        referencia_devengamiento_id INTEGER,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE,
        FOREIGN KEY (referencia_devengamiento_id) REFERENCES devengamientos(id) ON DELETE SET NULL
    );

    CREATE INDEX IF NOT EXISTS idx_devengamientos_cliente ON devengamientos(cliente_id);
    CREATE INDEX IF NOT EXISTS idx_devengamientos_periodo ON devengamientos(periodo_anyo, periodo_mes);
    CREATE INDEX IF NOT EXISTS idx_cobros_cliente ON cobros(cliente_id);
    CREATE INDEX IF NOT EXISTS idx_cobros_fecha ON cobros(fecha);
    """)
    con.commit()
    return created

# ======= Utilities =======

def parse_date(s):
    if isinstance(s, date):
        return s
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    raise ValueError("Formato de fecha inválido. Use YYYY-MM-DD o DD/MM/YYYY")

def parse_decimal(s: str):
    if s is None or s == "":
        raise ValueError("Valor vacío")
    try:
        return Decimal(s.replace(',', '.'))
    except InvalidOperation:
        raise ValueError(f"Número inválido: {s}")

def ultimo_dia_mes(anyo: int, mes: int) -> date:
    """Retorna el último día del mes dado"""
    if mes == 12:
        return date(anyo, 12, 31)
    else:
        return date(anyo, mes + 1, 1) - timedelta(days=1)

def backup_database():
    if not Path(DB_FILE).exists():
        return None
    Path(BACKUP_DIR).mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = Path(BACKUP_DIR)/f"abonos_{ts}.db"
    shutil.copy2(DB_FILE, dest)
    return str(dest)

# ======= Business logic helpers =======

def cliente_exists(cliente_id):
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT COUNT(1) as cnt FROM clientes WHERE id=?", (cliente_id,))
    r = cur.fetchone()
    return r[0] > 0

def devengamiento_exists(devengamiento_id):
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT COUNT(1) as cnt FROM devengamientos WHERE id=?", (devengamiento_id,))
    r = cur.fetchone()
    return r[0] > 0

def devengamiento_saldo(deveng_id: int) -> float:
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT importe FROM devengamientos WHERE id=?", (deveng_id,))
    row = cur.fetchone()
    if not row:
        return 0.0
    importe = float(row['importe'])
    
    cur.execute("SELECT COALESCE(SUM(monto),0) as aplicado FROM devengamientos_cobros WHERE devengamiento_id=?", (deveng_id,))
    aplicado = float(cur.fetchone()['aplicado'])
    
    cur.execute("SELECT COALESCE(SUM(monto),0) as ajustes FROM ajustes WHERE referencia_devengamiento_id=?", (deveng_id,))
    ajustes = float(cur.fetchone()['ajustes'])
    
    saldo = importe + ajustes - aplicado
    return max(0.0, saldo)

def imputar_automatico_db(cobro_id: int, cliente_id: int, importe: float):
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT d.* FROM devengamientos d WHERE d.cliente_id=? ORDER BY d.periodo_anyo, d.periodo_mes, d.id", (cliente_id,))
    devs = cur.fetchall()
    restante = importe
    for d in devs:
        if restante <= 0.01:
            break
        saldo = devengamiento_saldo(d['id'])
        if saldo <= 0.01:
            continue
        monto = min(restante, saldo)
        cur.execute("INSERT INTO devengamientos_cobros (devengamiento_id, cobro_id, monto) VALUES (?, ?, ?)", (d['id'], cobro_id, monto))
        restante -= monto
    con.commit()
    return restante

# ======= Streamlit UI =======

st.set_page_config(page_title="Abonos - LS", layout="wide")
st.title("Sistema de Gestión de Abonos — LS")

created = init_db()
if created:
    st.success("Base de datos inicializada")

menu = st.sidebar.selectbox("Sección", ["Dashboard", "Clientes", "Planes", "Devengamientos", "Cobros", "Ajustes", "Reportes", "Backup"]) 

# ---------- Dashboard ----------
if menu == "Dashboard":
    st.header("Dashboard")
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM clientes WHERE activo=1")
    clientes_activos = cur.fetchone()['cnt']
    cur.execute("SELECT COUNT(*) as cnt FROM planes WHERE activo=1")
    planes_activos = cur.fetchone()['cnt']
    hoy = date.today()
    cur.execute("SELECT COALESCE(SUM(importe),0) as total FROM devengamientos WHERE periodo_anyo=? AND periodo_mes=?", (hoy.year, hoy.month))
    devengado_mes = cur.fetchone()['total']
    primer_dia = date(hoy.year, hoy.month, 1).isoformat()
    cur.execute("SELECT COALESCE(SUM(importe),0) as total FROM cobros WHERE fecha >= ?", (primer_dia,))
    cobrado_mes = cur.fetchone()['total']

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Clientes activos", clientes_activos)
    with col2:
        st.metric("Planes activos", planes_activos)
    with col3:
        st.metric(f"Devengado {hoy.month}/{hoy.year}", f"${devengado_mes:.2f}")
    with col4:
        st.metric(f"Cobrado desde {primer_dia}", f"${cobrado_mes:.2f}")

# ---------- Clientes ----------
elif menu == "Clientes":
    st.header("Clientes")
    col1, col2 = st.columns([2,3])
    with col1:
        st.subheader("Agregar cliente")
        with st.form("form_add_cliente"):
            nombre = st.text_input("Nombre/Razón social")
            cuit = st.text_input("CUIT/DNI")
            contacto = st.text_input("Persona de Contacto")
            email = st.text_input("Email")
            telefono = st.text_input("Teléfono")
            direccion = st.text_input("Dirección")
            notas = st.text_area("Notas")
            submit = st.form_submit_button("Agregar")
        if submit:
            if not nombre.strip():
                st.error("El nombre es obligatorio")
            else:
                con = get_conn()
                cur = con.cursor()
                cur.execute("INSERT INTO clientes (nombre, cuit, contacto, email, telefono, direccion, notas) VALUES (?, ?, ?, ?, ?, ?, ?)", (nombre.strip(), cuit or None, contacto or None, email or None, telefono or None, direccion or None, notas or None))
                con.commit()
                st.success("Cliente agregado")
                st.rerun()
    
    with col2:
        st.subheader("Listado de clientes")
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT * FROM clientes ORDER BY nombre")
        rows = cur.fetchall()
        
        if rows:
            df = pd.DataFrame(rows)
            if not df.empty and all(col in df.columns for col in ['id','nombre','cuit','email','telefono','activo']):
                df_display = df[['id','nombre','cuit','email','telefono','activo']]
                st.dataframe(df_display)
            else:
                st.info("No hay clientes para mostrar")
        else:
            st.info("No hay clientes registrados")

        st.subheader("Editar / Eliminar cliente")
        sel = st.number_input("ID cliente para editar/activar/desactivar/eliminar (0=ninguno)", min_value=0, value=0, step=1)
        if sel:
            cur.execute("SELECT * FROM clientes WHERE id=?", (sel,))
            cli = cur.fetchone()
            if not cli:
                st.warning("Cliente no encontrado")
            else:
                st.write(f"**{cli['nombre']}** (ID: {cli['id']}, CUIT: {cli['cuit'] or 'N/A'})")
                
                col_edit, col_delete = st.columns([3, 1])
                
                with col_edit:
                    with st.form("form_edit_cliente"):
                        nombre2 = st.text_input("Nombre", value=cli['nombre'])
                        activo2 = st.selectbox("Activo", [1,0], index=0 if cli['activo'] else 1)
                        email2 = st.text_input("Email", value=cli['email'] or '')
                        tel2 = st.text_input("Teléfono", value=cli['telefono'] or '')
                        save = st.form_submit_button("Guardar cambios")
                    if save:
                        cur.execute("UPDATE clientes SET nombre=?, email=?, telefono=?, activo=?, updated_at=datetime('now') WHERE id=?", (nombre2, email2 or None, tel2 or None, activo2, sel))
                        con.commit()
                        st.success("Cliente actualizado")
                        st.rerun()
                
                with col_delete:
                    st.write("")
                    st.write("")
                    if st.button("🗑️ Eliminar", type="secondary"):
                        cur.execute("SELECT COUNT(*) as cnt FROM planes WHERE cliente_id=?", (sel,))
                        planes_count = cur.fetchone()['cnt']
                        cur.execute("SELECT COUNT(*) as cnt FROM devengamientos WHERE cliente_id=?", (sel,))
                        dev_count = cur.fetchone()['cnt']
                        cur.execute("SELECT COUNT(*) as cnt FROM cobros WHERE cliente_id=?", (sel,))
                        cobros_count = cur.fetchone()['cnt']
                        
                        if planes_count > 0 or dev_count > 0 or cobros_count > 0:
                            st.error(f"No se puede eliminar: tiene {planes_count} planes, {dev_count} devengamientos y {cobros_count} cobros asociados. Desactívelo en su lugar.")
                        else:
                            cur.execute("DELETE FROM clientes WHERE id=?", (sel,))
                            con.commit()
                            st.success("Cliente eliminado")
                            st.rerun()

# ---------- Planes ----------
elif menu == "Planes":
    st.header("Planes")
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT id, nombre, cuit FROM clientes ORDER BY nombre")
    clientes_rows = cur.fetchall()
    cliente_map = {r['id']: f"{r['nombre']} (CUIT: {r['cuit'] or 'N/A'}, ID: {r['id']})" for r in clientes_rows}
    
    with st.form("form_add_plan"):
        st.subheader("Agregar plan")
        cliente_id = st.selectbox("Cliente", options=[0]+list(cliente_map.keys()), format_func=lambda x: "- Seleccione cliente -" if x==0 else cliente_map[x])
        descripcion = st.text_input("Descripción")
        importe = st.text_input("Importe mensual")
        fecha_inicio = st.date_input("Fecha inicio", value=date.today())
        fecha_fin = st.date_input("Fecha fin (opcional)", value=None)
        periodicidad = st.selectbox("Periodicidad", ['mensual'])
        submit = st.form_submit_button("Agregar plan")
    
    if submit:
        try:
            if cliente_id == 0:
                st.error("Seleccione un cliente")
            else:
                imp = float(parse_decimal(importe))
                cur.execute("INSERT INTO planes (cliente_id, descripcion, importe, fecha_inicio, fecha_fin, periodicidad) VALUES (?, ?, ?, ?, ?, ?)", (cliente_id, descripcion or None, imp, fecha_inicio.isoformat(), fecha_fin.isoformat() if fecha_fin else None, periodicidad))
                con.commit()
                st.success("Plan agregado")
                st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")
    
    st.subheader("Listado de planes")
    cur.execute("SELECT p.*, c.nombre as cliente_nombre FROM planes p JOIN clientes c ON p.cliente_id=c.id ORDER BY p.activo DESC, c.nombre")
    dfp = cur.fetchall()
    
    if dfp:
        df_planes = pd.DataFrame(dfp)
        st.dataframe(df_planes)
    else:
        st.info("No hay planes registrados")
    
    st.subheader("Editar / Eliminar plan")
    sel_plan = st.number_input("ID plan para editar/eliminar (0=ninguno)", min_value=0, value=0, step=1)
    if sel_plan:
        cur.execute("SELECT p.*, c.nombre as cliente_nombre FROM planes p JOIN clientes c ON p.cliente_id=c.id WHERE p.id=?", (sel_plan,))
        plan = cur.fetchone()
        if not plan:
            st.warning("Plan no encontrado")
        else:
            st.write(f"**{plan['descripcion'] or 'Sin descripción'}** - Cliente: {plan['cliente_nombre']} (ID: {plan['id']})")
            
            col_edit, col_delete = st.columns([3, 1])
            
            with col_edit:
                with st.form("form_edit_plan"):
                    desc_edit = st.text_input("Descripción", value=plan['descripcion'] or '')
                    imp_edit = st.text_input("Importe", value=str(plan['importe']))
                    activo_edit = st.selectbox("Activo", [1,0], index=0 if plan['activo'] else 1)
                    save_plan = st.form_submit_button("Guardar cambios")
                if save_plan:
                    try:
                        imp_val = float(parse_decimal(imp_edit))
                        cur.execute("UPDATE planes SET descripcion=?, importe=?, activo=?, updated_at=datetime('now') WHERE id=?", (desc_edit or None, imp_val, activo_edit, sel_plan))
                        con.commit()
                        st.success("Plan actualizado")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
            
            with col_delete:
                st.write("")
                st.write("")
                if st.button("🗑️ Eliminar plan", type="secondary"):
                    cur.execute("SELECT COUNT(*) as cnt FROM devengamientos WHERE plan_id=?", (sel_plan,))
                    dev_count = cur.fetchone()['cnt']
                    
                    if dev_count > 0:
                        st.error(f"No se puede eliminar: tiene {dev_count} devengamientos asociados. Desactívelo en su lugar.")
                    else:
                        cur.execute("DELETE FROM planes WHERE id=?", (sel_plan,))
                        con.commit()
                        st.success("Plan eliminado")
                        st.rerun()

# ---------- Devengamientos ----------
elif menu == "Devengamientos":
    st.header("Devengamientos")
    con = get_conn()
    cur = con.cursor()
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Generar devengamientos para período")
        mes = st.number_input("Mes", min_value=1, max_value=12, value=date.today().month)
        anyo = st.number_input("Año", min_value=2000, max_value=2100, value=date.today().year)
        
        st.info(f"""
        **Lógica de generación:**
        - Puede generar devengamientos en cualquier momento (hoy: {date.today().strftime('%d/%m/%Y')})
        - La fecha contable será: {ultimo_dia_mes(anyo, mes).strftime('%d/%m/%Y')} (último día del mes)
        - Solo se generan para planes activos con clientes activos
        - Se omiten planes que no estén vigentes en el período seleccionado
        """)
        
        if st.button("Generar"):
            periodo_start = date(anyo, mes, 1)
            periodo_end = ultimo_dia_mes(anyo, mes)
            
            cur.execute("SELECT p.*, c.nombre as cliente_nombre FROM planes p JOIN clientes c ON p.cliente_id = c.id WHERE p.activo = 1 AND c.activo = 1")
            planes = cur.fetchall()
            created = 0
            skipped = 0
            for p in planes:
                try:
                    fecha_inicio = parse_date(p['fecha_inicio'])
                    fecha_fin = parse_date(p['fecha_fin']) if p['fecha_fin'] else None
                    
                    if fecha_inicio and fecha_inicio > periodo_end:
                        skipped += 1
                        continue
                    
                    if fecha_fin and fecha_fin < periodo_start:
                        skipped += 1
                        continue
                    
                    cur.execute("SELECT COUNT(1) as cnt FROM devengamientos WHERE cliente_id=? AND plan_id=? AND periodo_anyo=? AND periodo_mes=?", (p['cliente_id'], p['id'], anyo, mes))
                    if cur.fetchone()['cnt'] > 0:
                        skipped += 1
                        continue
                    
                    cur.execute("INSERT INTO devengamientos (cliente_id, plan_id, periodo_anyo, periodo_mes, importe, fecha_devengada) VALUES (?, ?, ?, ?, ?, ?)", (p['cliente_id'], p['id'], anyo, mes, p['importe'], periodo_end.isoformat()))
                    created += 1
                except Exception as e:
                    st.error(f"Error plan {p['id']}: {e}")
            con.commit()
            st.success(f"Creados: {created}  Omitidos: {skipped}")
            st.rerun()
    
    with col2:
        st.subheader("Listar devengamientos")
        only_pending = st.checkbox("Solo pendientes")
        cur.execute("SELECT d.*, c.nombre as cliente_nombre FROM devengamientos d JOIN clientes c ON d.cliente_id=c.id ORDER BY d.periodo_anyo DESC, d.periodo_mes DESC")
        rows = cur.fetchall()
        out = []
        for r in rows:
            saldo = devengamiento_saldo(r['id'])
            if only_pending and saldo <= 0.01:
                continue
            out.append({
                'id': r['id'], 
                'cliente': r['cliente_nombre'], 
                'periodo': f"{r['periodo_anyo']}/{r['periodo_mes']:02d}", 
                'fecha': r['fecha_devengada'],
                'importe': r['importe'], 
                'saldo': saldo
            })
        if out:
            st.dataframe(pd.DataFrame(out))
        else:
            st.info("No hay devengamientos a mostrar")
    
    st.subheader("Eliminar devengamiento")
    sel_dev = st.number_input("ID devengamiento para eliminar (0=ninguno)", min_value=0, value=0, step=1)
    if sel_dev:
        cur.execute("SELECT d.*, c.nombre as cliente_nombre FROM devengamientos d JOIN clientes c ON d.cliente_id=c.id WHERE d.id=?", (sel_dev,))
        dev = cur.fetchone()
        if not dev:
            st.warning("Devengamiento no encontrado")
        else:
            st.write(f"**Devengamiento {dev['periodo_anyo']}/{dev['periodo_mes']:02d}** - Cliente: {dev['cliente_nombre']} - Importe: ${dev['importe']:.2f}")
            if st.button("🗑️ Eliminar devengamiento", type="secondary"):
                cur.execute("SELECT COUNT(*) as cnt FROM devengamientos_cobros WHERE devengamiento_id=?", (sel_dev,))
                cobros_count = cur.fetchone()['cnt']
                cur.execute("SELECT COUNT(*) as cnt FROM ajustes WHERE referencia_devengamiento_id=?", (sel_dev,))
                ajustes_count = cur.fetchone()['cnt']
                
                if cobros_count > 0 or ajustes_count > 0:
                    st.error(f"No se puede eliminar: tiene {cobros_count} cobros aplicados y {ajustes_count} ajustes referenciados.")
                else:
                    cur.execute("DELETE FROM devengamientos WHERE id=?", (sel_dev,))
                    con.commit()
                    st.success("Devengamiento eliminado")
                    st.rerun()

# ---------- Cobros ----------
elif menu == "Cobros":
    st.header("Cobros")
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT id, nombre, cuit FROM clientes WHERE activo=1 ORDER BY nombre")
    clients = cur.fetchall()
    client_map = {c['id']: f"{c['nombre']} (CUIT: {c['cuit'] or 'N/A'}, ID: {c['id']})" for c in clients}
    
    with st.form("form_cobro"):
        cliente_id = st.selectbox("Cliente", options=[0]+list(client_map.keys()), format_func=lambda x: "- Seleccione cliente -" if x==0 else client_map[x])
        fecha = st.date_input("Fecha", value=date.today())
        importe = st.text_input("Importe")
        medio = st.text_input("Medio")
        referencia = st.text_input("Referencia")
        observacion = st.text_area("Observación")
        submit = st.form_submit_button("Registrar cobro")
    
    if submit:
        try:
            if cliente_id == 0:
                st.error("Seleccione cliente")
            else:
                imp = float(parse_decimal(importe))
                cur.execute("INSERT INTO cobros (cliente_id, fecha, importe, medio, referencia, observacion) VALUES (?, ?, ?, ?, ?, ?)", (cliente_id, fecha.isoformat(), imp, medio or None, referencia or None, observacion or None))
                cobro_id = cur.lastrowid
                con.commit()
                st.success(f"Cobro registrado ID {cobro_id}")
                
                restante = imputar_automatico_db(cobro_id, cliente_id, imp)
                if restante > 0.01:
                    st.info(f"Quedó sin imputar: ${restante:.2f}")
                st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

    st.subheader("Ver cobros recientes")
    cur.execute("SELECT c.*, cl.nombre as cliente_nombre FROM cobros c JOIN clientes cl ON c.cliente_id=cl.id ORDER BY c.fecha DESC LIMIT 50")
    rows = cur.fetchall()
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
        total = sum(float(r['importe']) for r in rows)
        st.metric("Total cobrado en el período", f"${total:.2f}")
    else:
        st.info("No hay cobros registrados")
    
    st.subheader("Eliminar cobro")
    sel_cobro = st.number_input("ID cobro para eliminar (0=ninguno)", min_value=0, value=0, step=1)
    if sel_cobro:
        cur.execute("SELECT c.*, cl.nombre as cliente_nombre FROM cobros c JOIN clientes cl ON c.cliente_id=cl.id WHERE c.id=?", (sel_cobro,))
        cobro = cur.fetchone()
        if not cobro:
            st.warning("Cobro no encontrado")
        else:
            st.write(f"**Cobro** - Cliente: {cobro['cliente_nombre']} - Fecha: {cobro['fecha']} - Importe: ${cobro['importe']:.2f}")
            if st.button("🗑️ Eliminar cobro", type="secondary"):
                cur.execute("SELECT COUNT(*) as cnt FROM devengamientos_cobros WHERE cobro_id=?", (sel_cobro,))
                imputaciones = cur.fetchone()['cnt']
                
                if imputaciones > 0:
                    st.error(f"No se puede eliminar: tiene {imputaciones} imputaciones a devengamientos. Elimine primero las imputaciones.")
                else:
                    cur.execute("DELETE FROM cobros WHERE id=?", (sel_cobro,))
                    con.commit()
                    st.success("Cobro eliminado")
                    st.rerun()

# ---------- Ajustes ----------
elif menu == "Ajustes":
    st.header("Ajustes")
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT id, nombre, cuit FROM clientes ORDER BY nombre")
    clients = cur.fetchall()
    cm = {c['id']: f"{c['nombre']} (CUIT: {c['cuit'] or 'N/A'}, ID: {c['id']})" for c in clients}
    
    with st.form("form_ajuste"):
        cliente_id = st.selectbox("Cliente", options=[0]+list(cm.keys()), format_func=lambda x: "- Seleccione cliente -" if x==0 else cm[x])
        fecha = st.date_input("Fecha", value=date.today())
        descripcion = st.text_input("Descripción")
        monto = st.text_input("Monto (positivo si aumenta la deuda, negativo si disminuye la deuda)")
        tipo = st.selectbox("Tipo", ["Bonificacion","Recargo","Adicional","Nota_credito","Nota_debito","Otro"]) 
        ref = st.text_input("ID devengamiento referencia (opcional)")
        submit = st.form_submit_button("Registrar ajuste")
    
    if submit:
        try:
            if cliente_id == 0:
                st.error("Seleccione cliente")
            else:
                m = float(parse_decimal(monto))
                ref_id = int(ref) if ref else None
                if ref_id and not devengamiento_exists(ref_id):
                    st.warning("Devengamiento no existe; se guardará sin referencia")
                    ref_id = None
                cur.execute("INSERT INTO ajustes (cliente_id, fecha, descripcion, monto, tipo, referencia_devengamiento_id) VALUES (?, ?, ?, ?, ?, ?)", (cliente_id, fecha.isoformat(), descripcion or None, m, tipo, ref_id))
                con.commit()
                st.success("Ajuste registrado")
                st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")
    
    st.subheader("Ver ajustes recientes")
    cur.execute("SELECT a.*, c.nombre as cliente_nombre FROM ajustes a JOIN clientes c ON a.cliente_id=c.id ORDER BY a.fecha DESC LIMIT 50")
    rows = cur.fetchall()
    if rows:
        st.dataframe(pd.DataFrame(rows))
    else:
        st.info("No hay ajustes registrados")
    
    st.subheader("Eliminar ajuste")
    sel_ajuste = st.number_input("ID ajuste para eliminar (0=ninguno)", min_value=0, value=0, step=1)
    if sel_ajuste:
        cur.execute("SELECT a.*, c.nombre as cliente_nombre FROM ajustes a JOIN clientes c ON a.cliente_id=c.id WHERE a.id=?", (sel_ajuste,))
        ajuste = cur.fetchone()
        if not ajuste:
            st.warning("Ajuste no encontrado")
        else:
            st.write(f"**Ajuste {ajuste['ipo']}** - Cliente: {ajuste['cliente_nombre']} - Monto: ${ajuste['monto']:.2f} - Descripción: {ajuste['descripcion'] or 'N/A'}")
            if st.button("🗑️ Eliminar ajuste", type="secondary"):
                cur.execute("DELETE FROM ajustes WHERE id=?", (sel_ajuste,))
                con.commit()
                st.success("Ajuste eliminado")
                st.rerun()

# ---------- Reportes ----------
elif menu == "Reportes":
    st.header("Reportes & Export")
    con = get_conn()
    cur = con.cursor()
    rpt = st.selectbox("Reporte", ["Estado de cuenta (cliente)", "Morosos", "Cobranzas mes", "Exportar tablas CSV"]) 
    
    if rpt == "Estado de cuenta (cliente)":
        cur.execute("SELECT id, nombre, cuit FROM clientes ORDER BY nombre")
        clients = cur.fetchall()
        cm = {c['id']: f"{c['nombre']} (CUIT: {c['cuit'] or 'N/A'}, ID: {c['id']})" for c in clients}
        if not cm:
            st.info("No hay clientes registrados")
        else:
            sel = st.selectbox("Cliente", options=list(cm.keys()), format_func=lambda x: cm[x])
            if st.button("Generar estado de cuenta"):
                events = []
                
                cur.execute("SELECT id, periodo_anyo, periodo_mes, importe, fecha_devengada FROM devengamientos WHERE cliente_id=? ORDER BY fecha_devengada, id", (sel,))
                for d in cur.fetchall():
                    events.append({
                        'fecha': d['fecha_devengada'], 
                        'concepto': f"Devengamiento {d['periodo_anyo']}/{d['periodo_mes']:02d} (ID: {d['id']})", 
                        'debito': float(d['importe']), 
                        'credito': 0.0
                    })
                
                cur.execute("SELECT fecha, tipo, descripcion, monto FROM ajustes WHERE cliente_id=? ORDER BY fecha, id", (sel,))
                for a in cur.fetchall():
                    m = float(a['monto'])
                    events.append({
                        'fecha': a['fecha'], 
                        'concepto': f"Ajuste {a['tipo']}: {a['descripcion'] or 'Sin descripción'}", 
                        'debito': m if m > 0 else 0.0, 
                        'credito': abs(m) if m < 0 else 0.0
                    })
                
                cur.execute("SELECT id, fecha, importe, medio, referencia FROM cobros WHERE cliente_id=? ORDER BY fecha, id", (sel,))
                for c_ in cur.fetchall():
                    events.append({
                        'fecha': c_['fecha'], 
                        'concepto': f"Cobro {c_['medio'] or 'Sin medio'} (Ref: {c_['referencia'] or 'N/A'})", 
                        'debito': 0.0, 
                        'credito': float(c_['importe'])
                    })
                
                events.sort(key=lambda x: (x['fecha'], x['concepto']))
                if events:
                    df = pd.DataFrame(events)
                    df['saldo'] = (df['debito'] - df['credito']).cumsum()
                    df['debito'] = df['debito'].apply(lambda x: f"${x:.2f}" if x > 0 else "-")
                    df['credito'] = df['credito'].apply(lambda x: f"${x:.2f}" if x > 0 else "-")
                    df['saldo'] = df['saldo'].apply(lambda x: f"${x:.2f}")
                    st.dataframe(df, use_container_width=True)
                    
                    cur.execute("SELECT COALESCE(SUM(importe), 0) as total_dev FROM devengamientos WHERE cliente_id=?", (sel,))
                    total_dev = float(cur.fetchone()['total_dev'])
                    cur.execute("SELECT COALESCE(SUM(monto), 0) as total_ajustes FROM ajustes WHERE cliente_id=?", (sel,))
                    total_ajustes = float(cur.fetchone()['total_ajustes'])
                    cur.execute("SELECT COALESCE(SUM(importe), 0) as total_cobros FROM cobros WHERE cliente_id=?", (sel,))
                    total_cobros = float(cur.fetchone()['total_cobros'])
                    
                    saldo_final = total_dev + total_ajustes - total_cobros
                    
                    st.markdown("---")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Devengado", f"${total_dev:.2f}")
                    with col2:
                        st.metric("Total Ajustes", f"${total_ajustes:.2f}")
                    with col3:
                        st.metric("Total Cobrado", f"${total_cobros:.2f}")
                    with col4:
                        st.metric("Saldo Final", f"${saldo_final:.2f}", delta=None, delta_color="inverse" if saldo_final > 0 else "normal")
                else:
                    st.info("Sin movimientos para este cliente")
    
    elif rpt == "Morosos":
        dias = st.number_input("Días de atraso mínimo", min_value=1, value=30)
        if st.button("Generar reporte"):
            fecha_lim = (date.today() - timedelta(days=dias)).isoformat()
            cur.execute("SELECT DISTINCT c.id, c.nombre, c.email, c.telefono FROM clientes c JOIN devengamientos d ON c.id = d.cliente_id WHERE d.fecha_devengada <= ? AND c.activo = 1 ORDER BY c.nombre", (fecha_lim,))
            rows = cur.fetchall()
            if rows:
                st.dataframe(pd.DataFrame(rows))
            else:
                st.success("No hay clientes morosos")
    
    elif rpt == "Cobranzas mes":
        mes = st.number_input("Mes", min_value=1, max_value=12, value=date.today().month)
        anyo = st.number_input("Año", min_value=2000, max_value=2100, value=date.today().year)
        if st.button("Generar"):
            primer = date(anyo, mes, 1).isoformat()
            ultimo = ultimo_dia_mes(anyo, mes).isoformat()
            cur.execute("SELECT c.*, cl.nombre as cliente_nombre FROM cobros c JOIN clientes cl ON c.cliente_id=cl.id WHERE c.fecha >= ? AND c.fecha <= ? ORDER BY c.fecha", (primer, ultimo))
            rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True)
                total = sum(float(r['importe']) for r in rows)
                st.metric("Total cobrado en el período", f"${total:.2f}")
            else:
                st.info("No hay cobros en ese período")
    
    elif rpt == "Exportar tablas CSV":
        tbl = st.selectbox("Tabla a exportar", ["clientes","planes","devengamientos","cobros","ajustes"]) 
        if st.button("Exportar"):
            cur.execute(f"SELECT * FROM {tbl} ORDER BY id")
            rows = cur.fetchall()
            if not rows:
                st.info("No hay datos")
            else:
                df = pd.DataFrame(rows)
                csv_buf = df.to_csv(index=False).encode('utf-8')
                st.download_button(label="Descargar CSV", data=csv_buf, file_name=f"{tbl}.csv", mime='text/csv')

# ---------- Backup ----------
elif menu == "Backup":
    st.header("Backup")
    if st.button("Crear backup ahora"):
        r = backup_database()
        if r:
            st.success(f"Backup creado: {r}")
        else:
            st.error("No se pudo crear backup (¿DB inexistente?)")

# Footer
st.sidebar.markdown("---")
st.sidebar.write("Sistema de Gestión de Abonos v1.1")
