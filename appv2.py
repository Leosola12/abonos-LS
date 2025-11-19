import streamlit as st
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
import shutil
from decimal import Decimal, InvalidOperation
import pandas as pd
from io import BytesIO
from reportlab.lib.pagesizes import A4, letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_RIGHT

# ======= Config =======
DB_FILE = "abonos.db"
BACKUP_DIR = "backups"

# ======= DB helpers =======
@st.cache_resource
def get_conn():
    """Conexión singleton a la base de datos"""
    try:
        con = sqlite3.connect(DB_FILE, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys = ON")
        return con
    except sqlite3.Error as e:
        st.error(f"Error al conectar con la base de datos: {e}")
        return None

def init_db():
    """Inicializa la base de datos con todas las tablas necesarias"""
    try:
        created = not Path(DB_FILE).exists()
        con = get_conn()
        if not con:
            return False
        
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
    except sqlite3.Error as e:
        st.error(f"Error al inicializar la base de datos: {e}")
        return False
    except Exception as e:
        st.error(f"Error inesperado: {e}")
        return False

# ======= Utilities =======

def format_currency_ar(valor):
    """Formatea un número al estilo argentino: punto para miles, coma para decimales"""
    try:
        num = float(valor)
        # Formatear con punto para miles y coma para decimales
        formatted = f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"${formatted}"
    except (ValueError, TypeError):
        return "$0,00"

def parse_date(s):
    """Parsea una fecha en múltiples formatos"""
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

def parse_input_ar(s: str):
    """Parsea un input que puede venir en formato argentino (1.234,56) o internacional"""
    if s is None or s == "":
        raise ValueError("Valor vacío")
    try:
        # Eliminar espacios y símbolo $
        s_clean = str(s).strip().replace('$', '').replace(' ', '')
        # Si tiene punto antes que coma, formato argentino: eliminar puntos, cambiar coma por punto
        if '.' in s_clean and ',' in s_clean:
            if s_clean.rfind('.') < s_clean.rfind(','):
                s_clean = s_clean.replace('.', '').replace(',', '.')
            else:  # formato internacional
                s_clean = s_clean.replace(',', '')
        elif ',' in s_clean:
            # Solo coma, asumir decimal argentino
            s_clean = s_clean.replace(',', '.')
        return Decimal(s_clean)
    except (InvalidOperation, ValueError):
        raise ValueError(f"Formato de número inválido: {s}")


def parse_decimal(s: str):
    """Parsea un número decimal de forma segura"""
    if s is None or s == "":
        raise ValueError("Valor vacío")
    try:
        # Eliminar espacios y reemplazar coma por punto
        s_clean = str(s).strip().replace(',', '.').replace(' ', '')
        return Decimal(s_clean)
    except (InvalidOperation, ValueError):
        raise ValueError(f"Número inválido: {s}")

def ultimo_dia_mes(anyo: int, mes: int) -> date:
    """Retorna el último día del mes dado"""
    try:
        if mes == 12:
            return date(anyo, 12, 31)
        else:
            return date(anyo, mes + 1, 1) - timedelta(days=1)
    except Exception as e:
        st.error(f"Error al calcular último día del mes: {e}")
        return date(anyo, mes, 28)

def backup_database():
    """Crea un backup de la base de datos"""
    try:
        if not Path(DB_FILE).exists():
            return None
        Path(BACKUP_DIR).mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = Path(BACKUP_DIR)/f"abonos_{ts}.db"
        shutil.copy2(DB_FILE, dest)
        return str(dest)
    except Exception as e:
        st.error(f"Error al crear backup: {e}")
        return None

def safe_float(value, default=0.0):
    """Convierte un valor a float de forma segura"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

# ======= Business logic helpers =======

def devengamiento_saldo(deveng_id: int) -> float:
    """Calcula el saldo pendiente de un devengamiento"""
    try:
        con = get_conn()
        if not con:
            return 0.0
        
        cur = con.cursor()
        cur.execute("SELECT importe FROM devengamientos WHERE id=?", (deveng_id,))
        row = cur.fetchone()
        if not row:
            return 0.0
        importe = safe_float(row['importe'])
        
        cur.execute("SELECT COALESCE(SUM(monto),0) as aplicado FROM devengamientos_cobros WHERE devengamiento_id=?", (deveng_id,))
        aplicado = safe_float(cur.fetchone()['aplicado'])
        
        cur.execute("SELECT COALESCE(SUM(monto),0) as ajustes FROM ajustes WHERE referencia_devengamiento_id=?", (deveng_id,))
        ajustes = safe_float(cur.fetchone()['ajustes'])
        
        saldo = importe + ajustes - aplicado
        return max(0.0, saldo)
    except Exception as e:
        st.error(f"Error al calcular saldo del devengamiento {deveng_id}: {e}")
        return 0.0

def imputar_automatico_db(cobro_id: int, cliente_id: int, importe: float):
    """Imputa automáticamente un cobro a los devengamientos pendientes más antiguos"""
    try:
        con = get_conn()
        if not con:
            return importe
        
        cur = con.cursor()
        cur.execute(
            "SELECT d.* FROM devengamientos d WHERE d.cliente_id=? ORDER BY d.periodo_anyo, d.periodo_mes, d.id",
            (cliente_id,)
        )
        devs = cur.fetchall()
        restante = importe
        
        for d in devs:
            if restante <= 0.01:
                break
            saldo = devengamiento_saldo(d['id'])
            if saldo <= 0.01:
                continue
            monto = min(restante, saldo)
            cur.execute(
                "INSERT INTO devengamientos_cobros (devengamiento_id, cobro_id, monto) VALUES (?, ?, ?)",
                (d['id'], cobro_id, monto)
            )
            restante -= monto
        
        con.commit()
        return restante
    except Exception as e:
        st.error(f"Error en imputación automática: {e}")
        return importe

def get_dashboard_metrics():
    """Obtiene las métricas para el dashboard"""
    try:
        con = get_conn()
        if not con:
            return None
        
        cur = con.cursor()
        hoy = date.today()
        
        # Clientes activos
        cur.execute("SELECT COUNT(*) as cnt FROM clientes WHERE activo=1")
        clientes_activos = cur.fetchone()['cnt']
        
        # Planes activos
        cur.execute("SELECT COUNT(*) as cnt FROM planes WHERE activo=1")
        planes_activos = cur.fetchone()['cnt']
        
        # Devengado este mes
        cur.execute(
            "SELECT COALESCE(SUM(importe),0) as total FROM devengamientos WHERE periodo_anyo=? AND periodo_mes=?",
            (hoy.year, hoy.month)
        )
        devengado_mes = safe_float(cur.fetchone()['total'])
        
        # Cobrado este mes
        primer_dia = date(hoy.year, hoy.month, 1).isoformat()
        cur.execute("SELECT COALESCE(SUM(importe),0) as total FROM cobros WHERE fecha >= ?", (primer_dia,))
        cobrado_mes = safe_float(cur.fetchone()['total'])
        
        # Saldo total pendiente
        cur.execute("SELECT COALESCE(SUM(importe),0) as total_dev FROM devengamientos")
        total_dev = safe_float(cur.fetchone()['total_dev'])
        
        cur.execute("SELECT COALESCE(SUM(monto),0) as total_cobros FROM devengamientos_cobros")
        total_cobros = safe_float(cur.fetchone()['total_cobros'])
        
        cur.execute("SELECT COALESCE(SUM(monto),0) as total_ajustes FROM ajustes")
        total_ajustes = safe_float(cur.fetchone()['total_ajustes'])
        
        saldo_pendiente = total_dev + total_ajustes - total_cobros
        
        # Clientes con saldo
        cur.execute("""
            SELECT COUNT(DISTINCT cliente_id) as cnt 
            FROM devengamientos 
            WHERE id IN (
                SELECT d.id FROM devengamientos d
                LEFT JOIN devengamientos_cobros dc ON d.id = dc.devengamiento_id
                GROUP BY d.id
                HAVING COALESCE(SUM(dc.monto), 0) < d.importe
            )
        """)
        clientes_con_saldo = cur.fetchone()['cnt']
        
        return {
            'clientes_activos': clientes_activos,
            'planes_activos': planes_activos,
            'devengado_mes': devengado_mes,
            'cobrado_mes': cobrado_mes,
            'saldo_pendiente': saldo_pendiente,
            'clientes_con_saldo': clientes_con_saldo
        }
    except Exception as e:
        st.error(f"Error al obtener métricas: {e}")
        return None

# ======= PDF Export Functions =======

def generar_pdf_estado_cuenta(cliente_id: int, cliente_nombre: str, events: list):
    """Genera un PDF con el estado de cuenta de un cliente"""
    try:
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4)
        elements = []
        styles = getSampleStyleSheet()
        
        # Título
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=colors.HexColor('#1f77b4'),
            spaceAfter=30,
            alignment=TA_CENTER
        )
        elements.append(Paragraph(f"Estado de Cuenta - {cliente_nombre}", title_style))
        elements.append(Paragraph(f"Fecha: {date.today().strftime('%d/%m/%Y')}", styles['Normal']))
        elements.append(Spacer(1, 0.3*inch))
        
        # Tabla de movimientos
        if events:
            data = [['Fecha', 'Concepto', 'Débito', 'Crédito', 'Saldo']]
            saldo = 0.0
            
            for e in events:
                debito = safe_float(e.get('debito', 0))
                credito = safe_float(e.get('credito', 0))
                saldo += debito - credito
                
                data.append([
                    e.get('fecha', ''),
                    e.get('concepto', '')[:50],
                    format_currency_ar(debito) if debito > 0 else "-",
                    format_currency_ar(credito) if credito > 0 else "-",
                    format_currency_ar(saldo)
                ])
            
            table = Table(data, colWidths=[1*inch, 3*inch, 1*inch, 1*inch, 1*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f77b4')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(table)
        
        doc.build(elements)
        buffer.seek(0)
        return buffer
    except Exception as e:
        st.error(f"Error al generar PDF: {e}")
        return None

def generar_pdf_reporte_cobranzas(rows, mes, anyo, total):
    """Genera un PDF con el reporte de cobranzas"""
    try:
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4)
        elements = []
        styles = getSampleStyleSheet()
        
        # Título
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=colors.HexColor('#2ca02c'),
            spaceAfter=30,
            alignment=TA_CENTER
        )
        elements.append(Paragraph(f"Reporte de Cobranzas - {mes}/{anyo}", title_style))
        elements.append(Paragraph(f"Generado: {date.today().strftime('%d/%m/%Y')}", styles['Normal']))
        elements.append(Spacer(1, 0.3*inch))
        
        # Tabla
        if rows:
            data = [['ID', 'Fecha', 'Cliente', 'Medio', 'Importe']]
            for r in rows:
                data.append([
                    str(r['id']),
                    r['fecha'],
                    r['cliente_nombre'][:30],
                    (r['medio'] or 'N/A')[:20],
                    f"${safe_float(r['importe']):.2f}"
                ])
            
            # Agregar total
            data.append(['', '', '', 'TOTAL:', f"${total:.2f}"])
            
            table = Table(data, colWidths=[0.5*inch, 1*inch, 2.5*inch, 1.5*inch, 1*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2ca02c')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -2), colors.beige),
                ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
                ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(table)
        
        doc.build(elements)
        buffer.seek(0)
        return buffer
    except Exception as e:
        st.error(f"Error al generar PDF de cobranzas: {e}")
        return None

# ======= UI Helper Functions =======

def show_help(section):
    """Muestra ayuda contextual según la sección"""
    help_texts = {
        "Dashboard": """
        **Dashboard** muestra un resumen general del sistema:
        - Cantidad de clientes y planes activos
        - Monto devengado y cobrado en el mes actual
        - Saldo total pendiente de cobro
        - Cantidad de clientes con deuda
        """,
        "Clientes": """
        **Gestión de Clientes:**
        1. Complete el formulario con los datos del cliente
        2. El nombre es obligatorio, los demás campos son opcionales
        3. Use el campo "Activo" para desactivar clientes sin eliminarlos
        4. Los clientes desactivados no generarán nuevos devengamientos
        """,
        "Planes": """
        **Gestión de Planes de Abono:**
        1. Seleccione el cliente para el cual crear el plan
        2. Ingrese el importe mensual del abono
        3. Defina la fecha de inicio (requerida) y fin (opcional)
        4. Los planes inactivos no generarán devengamientos
        """,
        "Devengamientos": """
        **Devengamientos:**
        - Son los cargos mensuales generados automáticamente según los planes
        - Use "Generar devengamientos" para crear los del período seleccionado
        - Solo se generan para planes activos en el período
        - No se pueden duplicar devengamientos para el mismo cliente/plan/período
        """,
        "Cobros": """
        **Registro de Cobros:**
        1. Seleccione el cliente que realizó el pago
        2. Ingrese fecha, importe y medio de pago
        3. El sistema imputará automáticamente a los devengamientos más antiguos
        4. Si queda saldo sin imputar, se le informará
        """,
        "Ajustes": """
        **Ajustes Contables:**
        - Use ajustes para bonificaciones, recargos o correcciones
        - Monto positivo: aumenta la deuda del cliente
        - Monto negativo: disminuye la deuda del cliente
        - Puede referenciar un devengamiento específico (opcional)
        """,
        "Reportes": """
        **Reportes y Exportaciones:**
        - **Estado de cuenta**: Ver todos los movimientos de un cliente
        - **Morosos**: Clientes con devengamientos vencidos
        - **Cobranzas**: Resumen de pagos en un período
        - **Export CSV/PDF**: Descargue los datos para análisis externo
        """
    }
    
    help_text = help_texts.get(section, "Ayuda no disponible para esta sección")
    with st.expander("ℹ️ Ayuda - " + section):
        st.markdown(help_text)

def show_quick_stats():
    """Muestra estadísticas rápidas en la barra lateral"""
    metrics = get_dashboard_metrics()
    if metrics:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📊 Resumen Rápido")
        st.sidebar.metric("Clientes activos", metrics['clientes_activos'])
        st.sidebar.metric("Saldo pendiente", format_currency_ar(metrics['saldo_pendiente']))
        st.sidebar.metric("Cobrado este mes", format_currency_ar(metrics['cobrado_mes']))

# ======= Streamlit UI =======

st.set_page_config(
    page_title="Sistema de Abonos - LS",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    /* Estilos que funcionan en modo claro y oscuro */
    .stMetric {
        background-color: rgba(28, 131, 225, 0.1);
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    
    /* Tema oscuro */
    @media (prefers-color-scheme: dark) {
        .stMetric {
            background-color: rgba(28, 131, 225, 0.15);
            border-left: 5px solid #4dabf7;
        }
    }
    
    .stAlert {
        border-radius: 10px;
    }
    
    .stButton>button {
        border-radius: 20px;
    }
    
    /* Mejorar legibilidad en tablas */
    .stDataFrame {
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

st.title("💰 Sistema de Gestión de Abonos — LS")
st.caption("Sistema profesional para gestión de abonos, cobros y cuentas corrientes")

# Inicializar DB
created = init_db()
if created:
    st.success("✅ Base de datos inicializada correctamente")

# Sidebar menu
st.sidebar.title("📋 Menú Principal")
menu = st.sidebar.selectbox(
    "Seleccione una sección",
    ["Dashboard", "Clientes", "Planes", "Devengamientos", "Cobros", "Ajustes", "Reportes", "Backup"],
    help="Navegue por las diferentes secciones del sistema"
)

# Mostrar ayuda contextual
show_help(menu)

# Mostrar stats rápidas
show_quick_stats()

# ---------- Dashboard ----------
if menu == "Dashboard":
    st.header("📊 Dashboard")
    
    metrics = get_dashboard_metrics()
    
    if metrics:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "👥 Clientes Activos",
                metrics['clientes_activos'],
                help="Cantidad de clientes activos en el sistema"
            )
            st.metric(
                "📋 Planes Activos",
                metrics['planes_activos'],
                help="Cantidad de planes de abono activos"
            )
        
        with col2:
            hoy = date.today()
            st.metric(
                f"📅 Devengado {hoy.month}/{hoy.year}",
                format_currency_ar(metrics['devengado_mes']),
                help="Total devengado en el mes actual"
            )
            st.metric(
                f"💵 Cobrado {hoy.month}/{hoy.year}",
                format_currency_ar(metrics['cobrado_mes']),
                delta=f"{(metrics['cobrado_mes'] / max(metrics['devengado_mes'], 1) * 100):.1f}% del devengado",
                help="Total cobrado en el mes actual"
            )
        
        with col3:
            st.metric(
                "💰 Saldo Total Pendiente",
                format_currency_ar(metrics['saldo_pendiente']),
                help="Suma de todos los saldos pendientes de cobro"
            )
            st.metric(
                "⚠️ Clientes con Saldo",
                metrics['clientes_con_saldo'],
                help="Cantidad de clientes que tienen saldo pendiente"
            )
        
        st.markdown("---")
        
        # Información adicional
        col_info1, col_info2 = st.columns(2)
        
        with col_info1:
            st.info("""
            **🎯 Acciones Rápidas:**
            - Vaya a **Devengamientos** para generar los cargos del mes
            - Vaya a **Cobros** para registrar pagos recibidos
            - Vaya a **Reportes** para ver estados de cuenta
            """)
        
        with col_info2:
            st.success("""
            **✨ Consejos:**
            - Genere los devengamientos al inicio de cada mes
            - Revise regularmente el reporte de morosos
            - Haga backups periódicos de la base de datos
            """)

# ---------- Clientes ----------
elif menu == "Clientes":
    st.header("👥 Gestión de Clientes")
    
    col1, col2 = st.columns([2, 3])
    
    with col1:
        st.subheader("➕ Agregar Nuevo Cliente")
        
        with st.form("form_add_cliente", clear_on_submit=True):
            nombre = st.text_input("Nombre / Razón Social *", help="Campo obligatorio")
            cuit = st.text_input("CUIT / DNI", help="Número de identificación fiscal")
            contacto = st.text_input("Persona de Contacto", help="Nombre del contacto principal")
            
            col_email, col_tel = st.columns(2)
            with col_email:
                email = st.text_input("Email", help="Correo electrónico de contacto")
            with col_tel:
                telefono = st.text_input("Teléfono", help="Número de teléfono")
            
            direccion = st.text_input("Dirección", help="Dirección física del cliente")
            notas = st.text_area("Notas", help="Información adicional sobre el cliente")
            
            submit = st.form_submit_button("➕ Agregar Cliente", use_container_width=True)
        
        if submit:
            try:
                if not nombre.strip():
                    st.error("❌ El nombre es obligatorio")
                else:
                    con = get_conn()
                    if con:
                        cur = con.cursor()
                        cur.execute(
                            "INSERT INTO clientes (nombre, cuit, contacto, email, telefono, direccion, notas) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (nombre.strip(), cuit or None, contacto or None, email or None, telefono or None, direccion or None, notas or None)
                        )
                        con.commit()
                        st.success(f"✅ Cliente '{nombre}' agregado correctamente (ID: {cur.lastrowid})")
                        st.rerun()
            except sqlite3.IntegrityError as e:
                st.error(f"❌ Error de integridad: {e}")
            except Exception as e:
                st.error(f"❌ Error al agregar cliente: {e}")
    
    with col2:
        st.subheader("📋 Listado de Clientes")
        
        try:
            con = get_conn()
            if con:
                cur = con.cursor()
                
                # Filtro de búsqueda
                buscar = st.text_input("🔍 Buscar cliente", placeholder="Ingrese nombre, CUIT o email...")
                
                if buscar:
                    cur.execute(
                        "SELECT * FROM clientes WHERE nombre LIKE ? OR cuit LIKE ? OR email LIKE ? ORDER BY nombre",
                        (f"%{buscar}%", f"%{buscar}%", f"%{buscar}%")
                    )
                else:
                    cur.execute("SELECT * FROM clientes ORDER BY activo DESC, nombre")
                
                rows = cur.fetchall()
                
                if rows:
                    df = pd.DataFrame(rows)
                    # Formatear columna activo
                    df['activo'] = df['activo'].apply(lambda x: '✅ Activo' if x else '❌ Inactivo')
                    
                    if not df.empty:
                        cols_display = ['id', 'nombre', 'cuit', 'email', 'telefono', 'activo']
                        cols_display = [c for c in cols_display if c in df.columns]
                        st.dataframe(df[cols_display], use_container_width=True, height=400)
                        st.caption(f"Total: {len(rows)} cliente(s)")
                else:
                    st.info("ℹ️ No hay clientes registrados")
        except Exception as e:
            st.error(f"❌ Error al cargar clientes: {e}")

    st.markdown("---")
    st.subheader("✏️ Editar / Eliminar Cliente")
    
    try:
        con = get_conn()
        if con:
            cur = con.cursor()
            sel = st.number_input(
                "ID del cliente para editar/eliminar (0 = ninguno)",
                min_value=0,
                value=0,
                step=1,
                help="Ingrese el ID del cliente que desea modificar"
            )
            
            if sel > 0:
                cur.execute("SELECT * FROM clientes WHERE id=?", (sel,))
                cli = cur.fetchone()
                
                if not cli:
                    st.warning("⚠️ Cliente no encontrado")
                else:
                    st.info(f"**Editando:** {cli['nombre']} (ID: {cli['id']}, CUIT: {cli['cuit'] or 'N/A'})")
                    
                    col_edit, col_delete = st.columns([3, 1])
                    
                    with col_edit:
                        with st.form("form_edit_cliente"):
                            nombre2 = st.text_input("Nombre", value=cli['nombre'])
                            activo2 = st.selectbox("Estado", [1, 0], index=0 if cli['activo'] else 1, format_func=lambda x: '✅ Activo' if x else '❌ Inactivo')
                            
                            col_e1, col_e2 = st.columns(2)
                            with col_e1:
                                email2 = st.text_input("Email", value=cli['email'] or '')
                            with col_e2:
                                tel2 = st.text_input("Teléfono", value=cli['telefono'] or '')
                            
                            save = st.form_submit_button("💾 Guardar Cambios", use_container_width=True)
                        
                        if save:
                            try:
                                if not nombre2.strip():
                                    st.error("❌ El nombre no puede estar vacío")
                                else:
                                    cur.execute(
                                        "UPDATE clientes SET nombre=?, email=?, telefono=?, activo=?, updated_at=datetime('now') WHERE id=?",
                                        (nombre2.strip(), email2 or None, tel2 or None, activo2, sel)
                                    )
                                    con.commit()
                                    st.success("✅ Cliente actualizado correctamente")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error al actualizar: {e}")
                    
                    with col_delete:
                        st.write("")
                        st.write("")
                        if st.button("🗑️ Eliminar", type="secondary", use_container_width=True):
                            try:
                                # Verificar dependencias
                                cur.execute("SELECT COUNT(*) as cnt FROM planes WHERE cliente_id=?", (sel,))
                                planes_count = cur.fetchone()['cnt']
                                cur.execute("SELECT COUNT(*) as cnt FROM devengamientos WHERE cliente_id=?", (sel,))
                                dev_count = cur.fetchone()['cnt']
                                cur.execute("SELECT COUNT(*) as cnt FROM cobros WHERE cliente_id=?", (sel,))
                                cobros_count = cur.fetchone()['cnt']
                                
                                if planes_count > 0 or dev_count > 0 or cobros_count > 0:
                                    st.error(f"❌ No se puede eliminar: tiene {planes_count} planes, {dev_count} devengamientos y {cobros_count} cobros asociados. Desactívelo en su lugar.")
                                else:
                                    cur.execute("DELETE FROM clientes WHERE id=?", (sel,))
                                    con.commit()
                                    st.success("✅ Cliente eliminado correctamente")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error al eliminar: {e}")
    except Exception as e:
        st.error(f"❌ Error: {e}")

# ---------- Planes ----------
elif menu == "Planes":
    st.header("📋 Gestión de Planes de Abono")
    
    try:
        con = get_conn()
        if not con:
            st.error("❌ No se pudo conectar a la base de datos")
        else:
            cur = con.cursor()
            cur.execute("SELECT id, nombre, cuit, activo FROM clientes ORDER BY activo DESC, nombre")
            clientes_rows = cur.fetchall()
            cliente_map = {
                r['id']: f"{r['nombre']} (CUIT: {r['cuit'] or 'N/A'}, ID: {r['id']}) {'✅' if r['activo'] else '❌'}"
                for r in clientes_rows
            }
            
            if not cliente_map:
                st.warning("⚠️ No hay clientes registrados. Agregue clientes primero.")
            else:
                with st.form("form_add_plan"):
                    st.subheader("➕ Agregar Nuevo Plan")
                    
                    cliente_id = st.selectbox(
                        "Cliente *",
                        options=[0] + list(cliente_map.keys()),
                        format_func=lambda x: "- Seleccione un cliente -" if x == 0 else cliente_map[x],
                        help="Seleccione el cliente para este plan"
                    )
                    
                    descripcion = st.text_input("Descripción", help="Descripción del plan de abono")
                    importe = st.text_input("Importe Mensual *", help="Monto a cobrar mensualmente")
                    
                    col_f1, col_f2 = st.columns(2)
                    with col_f1:
                        fecha_inicio = st.date_input("Fecha Inicio *", value=date.today(), help="Fecha de inicio del plan")
                    with col_f2:
                        fecha_fin = st.date_input("Fecha Fin (opcional)", value=None, help="Dejar vacío para plan sin fecha de fin")
                    
                    periodicidad = st.selectbox("Periodicidad", ['mensual'], help="Frecuencia de facturación")
                    
                    submit = st.form_submit_button("➕ Agregar Plan", use_container_width=True)
                
                if submit:
                    try:
                        if cliente_id == 0:
                            st.error("❌ Debe seleccionar un cliente")
                        elif not importe.strip():
                            st.error("❌ El importe es obligatorio")
                        else:
                            imp = float(parse_decimal(importe))
                            if imp <= 0:
                                st.error("❌ El importe debe ser mayor a cero")
                            else:
                                cur.execute(
                                    "INSERT INTO planes (cliente_id, descripcion, importe, fecha_inicio, fecha_fin, periodicidad) VALUES (?, ?, ?, ?, ?, ?)",
                                    (cliente_id, descripcion or None, imp, fecha_inicio.isoformat(), fecha_fin.isoformat() if fecha_fin else None, periodicidad)
                                )
                                con.commit()
                                st.success(f"✅ Plan agregado correctamente (ID: {cur.lastrowid})")
                                st.rerun()
                    except ValueError as e:
                        st.error(f"❌ Error en formato de importe: {e}")
                    except Exception as e:
                        st.error(f"❌ Error al agregar plan: {e}")
                
                st.markdown("---")
                st.subheader("📋 Listado de Planes")
                
                # Filtros
                col_f1, col_f2 = st.columns(2)
                with col_f1:
                    filtro_activo = st.selectbox("Filtrar por estado", ["Todos", "Solo activos", "Solo inactivos"])
                with col_f2:
                    buscar_plan = st.text_input("🔍 Buscar plan", placeholder="Buscar por descripción...")
                
                query = """
                    SELECT p.*, c.nombre as cliente_nombre, c.activo as cliente_activo
                    FROM planes p
                    JOIN clientes c ON p.cliente_id = c.id
                """
                
                conditions = []
                params = []
                
                if filtro_activo == "Solo activos":
                    conditions.append("p.activo = 1")
                elif filtro_activo == "Solo inactivos":
                    conditions.append("p.activo = 0")
                
                if buscar_plan:
                    conditions.append("(p.descripcion LIKE ? OR c.nombre LIKE ?)")
                    params.extend([f"%{buscar_plan}%", f"%{buscar_plan}%"])
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                
                query += " ORDER BY p.activo DESC, c.nombre"
                
                cur.execute(query, params)
                dfp = cur.fetchall()
                
                if dfp:
                    df_planes = pd.DataFrame(dfp)
                    df_planes['activo'] = df_planes['activo'].apply(lambda x: '✅' if x else '❌')
                    df_planes['estado_cliente'] = df_planes['cliente_activo'].apply(lambda x: '✅' if x else '❌')
                    
                    cols_display = ['id', 'cliente_nombre', 'descripcion', 'importe', 'fecha_inicio', 'fecha_fin', 'activo']
                    cols_display = [c for c in cols_display if c in df_planes.columns]
                    
                    st.dataframe(df_planes[cols_display], use_container_width=True, height=400)
                    st.caption(f"Total: {len(dfp)} plan(es)")
                else:
                    st.info("ℹ️ No hay planes registrados")
                
                st.markdown("---")
                st.subheader("✏️ Editar / Eliminar Plan")
                
                sel_plan = st.number_input(
                    "ID del plan para editar/eliminar (0 = ninguno)",
                    min_value=0,
                    value=0,
                    step=1,
                    help="Ingrese el ID del plan que desea modificar"
                )
                
                if sel_plan > 0:
                    cur.execute(
                        "SELECT p.*, c.nombre as cliente_nombre FROM planes p JOIN clientes c ON p.cliente_id = c.id WHERE p.id=?",
                        (sel_plan,)
                    )
                    plan = cur.fetchone()
                    
                    if not plan:
                        st.warning("⚠️ Plan no encontrado")
                    else:
                        st.info(f"**Editando:** {plan['descripcion'] or 'Sin descripción'} - Cliente: {plan['cliente_nombre']} (ID: {plan['id']})")
                        
                        col_edit, col_delete = st.columns([3, 1])
                        
                        with col_edit:
                            with st.form("form_edit_plan"):
                                desc_edit = st.text_input("Descripción", value=plan['descripcion'] or '')
                                imp_edit = st.text_input("Importe", value=str(plan['importe']))
                                activo_edit = st.selectbox(
                                    "Estado",
                                    [1, 0],
                                    index=0 if plan['activo'] else 1,
                                    format_func=lambda x: '✅ Activo' if x else '❌ Inactivo'
                                )
                                save_plan = st.form_submit_button("💾 Guardar Cambios", use_container_width=True)
                            
                            if save_plan:
                                try:
                                    imp_val = float(parse_decimal(imp_edit))
                                    if imp_val <= 0:
                                        st.error("❌ El importe debe ser mayor a cero")
                                    else:
                                        cur.execute(
                                            "UPDATE planes SET descripcion=?, importe=?, activo=?, updated_at=datetime('now') WHERE id=?",
                                            (desc_edit or None, imp_val, activo_edit, sel_plan)
                                        )
                                        con.commit()
                                        st.success("✅ Plan actualizado correctamente")
                                        st.rerun()
                                except ValueError as e:
                                    st.error(f"❌ Error en formato de importe: {e}")
                                except Exception as e:
                                    st.error(f"❌ Error al actualizar: {e}")
                        
                        with col_delete:
                            st.write("")
                            st.write("")
                            if st.button("🗑️ Eliminar", type="secondary", use_container_width=True):
                                try:
                                    cur.execute("SELECT COUNT(*) as cnt FROM devengamientos WHERE plan_id=?", (sel_plan,))
                                    dev_count = cur.fetchone()['cnt']
                                    
                                    if dev_count > 0:
                                        st.error(f"❌ No se puede eliminar: tiene {dev_count} devengamientos asociados. Desactívelo en su lugar.")
                                    else:
                                        cur.execute("DELETE FROM planes WHERE id=?", (sel_plan,))
                                        con.commit()
                                        st.success("✅ Plan eliminado correctamente")
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error al eliminar: {e}")
    except Exception as e:
        st.error(f"❌ Error: {e}")

# ---------- Devengamientos ----------
elif menu == "Devengamientos":
    st.header("📅 Gestión de Devengamientos")
    
    try:
        con = get_conn()
        if not con:
            st.error("❌ No se pudo conectar a la base de datos")
        else:
            cur = con.cursor()
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("➕ Generar Devengamientos")
                
                hoy = date.today()
                
                col_m, col_a = st.columns(2)
                with col_m:
                    mes = st.number_input("Mes", min_value=1, max_value=12, value=hoy.month)
                with col_a:
                    anyo = st.number_input("Año", min_value=2000, max_value=2100, value=hoy.year)
                
                periodo_end = ultimo_dia_mes(anyo, mes)
                
                st.info(f"""
                **ℹ️ Información:**
                - Fecha contable: {periodo_end.strftime('%d/%m/%Y')}
                - Solo se procesan planes activos
                - Se omiten planes fuera del período
                - No se generan duplicados
                """)
                
                if st.button("🔄 Generar Devengamientos", type="primary", use_container_width=True):
                    try:
                        periodo_start = date(anyo, mes, 1)
                        
                        with st.spinner("Generando devengamientos..."):
                            cur.execute("""
                                SELECT p.*, c.nombre as cliente_nombre
                                FROM planes p
                                JOIN clientes c ON p.cliente_id = c.id
                                WHERE p.activo = 1 AND c.activo = 1
                            """)
                            planes = cur.fetchall()
                            
                            created = 0
                            skipped = 0
                            errors = []
                            
                            for p in planes:
                                try:
                                    fecha_inicio = parse_date(p['fecha_inicio'])
                                    fecha_fin = parse_date(p['fecha_fin']) if p['fecha_fin'] else None
                                    
                                    # Verificar si el plan está vigente en el período
                                    if fecha_inicio and fecha_inicio > periodo_end:
                                        skipped += 1
                                        continue
                                    
                                    if fecha_fin and fecha_fin < periodo_start:
                                        skipped += 1
                                        continue
                                    
                                    # Verificar si ya existe
                                    cur.execute("""
                                        SELECT COUNT(1) as cnt
                                        FROM devengamientos
                                        WHERE cliente_id=? AND plan_id=? AND periodo_anyo=? AND periodo_mes=?
                                    """, (p['cliente_id'], p['id'], anyo, mes))
                                    
                                    if cur.fetchone()['cnt'] > 0:
                                        skipped += 1
                                        continue
                                    
                                    # Insertar devengamiento
                                    cur.execute("""
                                        INSERT INTO devengamientos
                                        (cliente_id, plan_id, periodo_anyo, periodo_mes, importe, fecha_devengada)
                                        VALUES (?, ?, ?, ?, ?, ?)
                                    """, (p['cliente_id'], p['id'], anyo, mes, p['importe'], periodo_end.isoformat()))
                                    
                                    created += 1
                                
                                except Exception as e:
                                    errors.append(f"Plan {p['id']} ({p['cliente_nombre']}): {str(e)}")
                            
                            con.commit()
                            
                            st.success(f"✅ Generados: {created} | Omitidos: {skipped}")
                            
                            if errors:
                                with st.expander(f"⚠️ Ver {len(errors)} error(es)"):
                                    for err in errors:
                                        st.warning(err)
                            
                            st.rerun()
                    
                    except Exception as e:
                        st.error(f"❌ Error al generar devengamientos: {e}")
            
            with col2:
                st.subheader("📋 Listado de Devengamientos")
                
                col_filtro = st.columns([1, 1, 1])
                with col_filtro[0]:
                    only_pending = st.checkbox("Solo pendientes", value=True, help="Mostrar solo devengamientos con saldo pendiente")
                with col_filtro[1]:
                    filtro_mes = st.selectbox("Mes", ["Todos"] + list(range(1, 13)), format_func=lambda x: "Todos" if x == "Todos" else f"{x:02d}")
                with col_filtro[2]:
                    filtro_anyo = st.selectbox("Año", ["Todos"] + list(range(2020, 2030)), format_func=lambda x: x)
                
                query = """
                    SELECT d.*, c.nombre as cliente_nombre
                    FROM devengamientos d
                    JOIN clientes c ON d.cliente_id = c.id
                """
                
                conditions = []
                params = []
                
                if filtro_mes != "Todos":
                    conditions.append("d.periodo_mes = ?")
                    params.append(filtro_mes)
                
                if filtro_anyo != "Todos":
                    conditions.append("d.periodo_anyo = ?")
                    params.append(filtro_anyo)
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                
                query += " ORDER BY d.periodo_anyo DESC, d.periodo_mes DESC, c.nombre"
                
                cur.execute(query, params)
                rows = cur.fetchall()
                
                out = []
                total_importe = 0.0
                total_saldo = 0.0
                
                for r in rows:
                    saldo = devengamiento_saldo(r['id'])
                    if only_pending and saldo <= 0.01:
                        continue
                    
                    total_importe += safe_float(r['importe'])
                    total_saldo += saldo
                    
                    out.append({
                        'ID': r['id'],
                        'Cliente': r['cliente_nombre'],
                        'Período': f"{r['periodo_anyo']}/{r['periodo_mes']:02d}",
                        'Fecha': r['fecha_devengada'],
                        'Importe': f"${safe_float(r['importe']):.2f}",
                        'Saldo': f"${saldo:.2f}",
                        'Estado': '⚠️ Pendiente' if saldo > 0.01 else '✅ Cobrado'
                    })
                
                if out:
                    df_out = pd.DataFrame(out)
                    st.dataframe(df_out, use_container_width=True, height=400)
                    
                    col_sum1, col_sum2 = st.columns(2)
                    with col_sum1:
                        st.metric("Total Importe", f"${total_importe:.2f}")
                    with col_sum2:
                        st.metric("Total Saldo Pendiente", f"${total_saldo:.2f}")
                    
                    st.caption(f"Mostrando {len(out)} devengamiento(s)")
                else:
                    st.info("ℹ️ No hay devengamientos para mostrar con los filtros seleccionados")
            
            st.markdown("---")
            st.subheader("🗑️ Eliminar Devengamiento")
            
            sel_dev = st.number_input(
                "ID del devengamiento para eliminar (0 = ninguno)",
                min_value=0,
                value=0,
                step=1,
                help="Use con precaución. Solo se pueden eliminar devengamientos sin cobros aplicados."
            )
            
            if sel_dev > 0:
                cur.execute("""
                    SELECT d.*, c.nombre as cliente_nombre
                    FROM devengamientos d
                    JOIN clientes c ON d.cliente_id = c.id
                    WHERE d.id=?
                """, (sel_dev,))
                dev = cur.fetchone()
                
                if not dev:
                    st.warning("⚠️ Devengamiento no encontrado")
                else:
                    saldo = devengamiento_saldo(sel_dev)
                    st.info(f"""
                    **Devengamiento {dev['periodo_anyo']}/{dev['periodo_mes']:02d}**
                    - Cliente: {dev['cliente_nombre']}
                    - Importe: ${safe_float(dev['importe']):.2f}
                    - Saldo pendiente: ${saldo:.2f}
                    """)
                    
                    if st.button("🗑️ Eliminar Devengamiento", type="secondary"):
                        try:
                            cur.execute("SELECT COUNT(*) as cnt FROM devengamientos_cobros WHERE devengamiento_id=?", (sel_dev,))
                            cobros_count = cur.fetchone()['cnt']
                            cur.execute("SELECT COUNT(*) as cnt FROM ajustes WHERE referencia_devengamiento_id=?", (sel_dev,))
                            ajustes_count = cur.fetchone()['cnt']
                            
                            if cobros_count > 0 or ajustes_count > 0:
                                st.error(f"❌ No se puede eliminar: tiene {cobros_count} cobros aplicados y {ajustes_count} ajustes referenciados.")
                            else:
                                cur.execute("DELETE FROM devengamientos WHERE id=?", (sel_dev,))
                                con.commit()
                                st.success("✅ Devengamiento eliminado correctamente")
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al eliminar: {e}")
    
    except Exception as e:
        st.error(f"❌ Error: {e}")

# ---------- Cobros ----------
elif menu == "Cobros":
    st.header("💵 Gestión de Cobros")
    
    try:
        con = get_conn()
        if not con:
            st.error("❌ No se pudo conectar a la base de datos")
        else:
            cur = con.cursor()
            cur.execute("SELECT id, nombre, cuit FROM clientes WHERE activo=1 ORDER BY nombre")
            clients = cur.fetchall()
            client_map = {
                c['id']: f"{c['nombre']} (CUIT: {c['cuit'] or 'N/A'}, ID: {c['id']})"
                for c in clients
            }
            
            if not client_map:
                st.warning("⚠️ No hay clientes activos. Active o agregue clientes primero.")
            else:
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    with st.form("form_cobro"):
                        st.subheader("➕ Registrar Cobro")
                        
                        cliente_id = st.selectbox(
                            "Cliente *",
                            options=[0] + list(client_map.keys()),
                            format_func=lambda x: "- Seleccione un cliente -" if x == 0 else client_map[x],
                            help="Cliente que realizó el pago"
                        )
                        
                        fecha = st.date_input("Fecha *", value=date.today(), help="Fecha en que se recibió el pago")
                        importe = st.text_input("Importe *", help="Monto recibido")
                        medio = st.selectbox(
                            "Medio de Pago",
                            ["Efectivo", "Transferencia", "Cheque", "Tarjeta", "Otro"],
                            help="Método de pago utilizado"
                        )
                        referencia = st.text_input("Referencia", help="Número de comprobante, transferencia, etc.")
                        observacion = st.text_area("Observación", help="Información adicional sobre el cobro")
                        
                        submit = st.form_submit_button("💰 Registrar Cobro", use_container_width=True)
                    
                    if submit:
                        try:
                            if cliente_id == 0:
                                st.error("❌ Debe seleccionar un cliente")
                            elif not importe.strip():
                                st.error("❌ El importe es obligatorio")
                            else:
                                imp = float(parse_decimal(importe))
                                if imp <= 0:
                                    st.error("❌ El importe debe ser mayor a cero")
                                else:
                                    cur.execute("""
                                        INSERT INTO cobros (cliente_id, fecha, importe, medio, referencia, observacion)
                                        VALUES (?, ?, ?, ?, ?, ?)
                                    """, (cliente_id, fecha.isoformat(), imp, medio or None, referencia or None, observacion or None))
                                    
                                    cobro_id = cur.lastrowid
                                    con.commit()
                                    
                                    st.success(f"✅ Cobro registrado (ID: {cobro_id})")
                                    
                                    # Imputación automática
                                    with st.spinner("Imputando cobro a devengamientos..."):
                                        restante = imputar_automatico_db(cobro_id, cliente_id, imp)
                                        
                                        if restante > 0.01:
                                            st.warning(f"⚠️ Quedó sin imputar: ${restante:.2f}")
                                        else:
                                            st.success("✅ Cobro imputado completamente")
                                    
                                    st.rerun()
                        except ValueError as e:
                            st.error(f"❌ Error en formato de importe: {e}")
                        except Exception as e:
                            st.error(f"❌ Error al registrar cobro: {e}")
                
                with col2:
                    st.subheader("📋 Cobros Recientes")
                    
                    # Filtros
                    col_f = st.columns(3)
                    with col_f[0]:
                        limite = st.selectbox("Mostrar", [10, 25, 50, 100], index=2)
                    with col_f[1]:
                        filtro_cliente = st.selectbox("Cliente", ["Todos"] + list(client_map.values()))
                    with col_f[2]:
                        filtro_medio = st.selectbox("Medio", ["Todos", "Efectivo", "Transferencia", "Cheque", "Tarjeta", "Otro"])
                    
                    query = """
                        SELECT c.*, cl.nombre as cliente_nombre
                        FROM cobros c
                        JOIN clientes cl ON c.cliente_id = cl.id
                    """
                    
                    conditions = []
                    params = []
                    
                    if filtro_cliente != "Todos":
                        cliente_filtro_id = [k for k, v in client_map.items() if v == filtro_cliente]
                        if cliente_filtro_id:
                            conditions.append("c.cliente_id = ?")
                            params.append(cliente_filtro_id[0])
                    
                    if filtro_medio != "Todos":
                        conditions.append("c.medio = ?")
                        params.append(filtro_medio)
                    
                    if conditions:
                        query += " WHERE " + " AND ".join(conditions)
                    
                    query += f" ORDER BY c.fecha DESC, c.id DESC LIMIT {limite}"
                    
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    
                    if rows:
                        data = []
                        total = 0.0
                        
                        for r in rows:
                            imp_val = safe_float(r['importe'])
                            total += imp_val
                            data.append({
                                'ID': r['id'],
                                'Fecha': r['fecha'],
                                'Cliente': r['cliente_nombre'],
                                'Importe': f"${imp_val:.2f}",
                                'Medio': r['medio'] or 'N/A',
                                'Referencia': r['referencia'] or '-'
                            })
                        
                        df = pd.DataFrame(data)
                        st.dataframe(df, use_container_width=True, height=400)
                        st.metric("💰 Total Cobrado", f"${total:.2f}")
                        st.caption(f"Mostrando {len(rows)} cobro(s)")
                    else:
                        st.info("ℹ️ No hay cobros registrados")
                
                st.markdown("---")
                st.subheader("🗑️ Eliminar Cobro")
                
                sel_cobro = st.number_input(
                    "ID del cobro para eliminar (0 = ninguno)",
                    min_value=0,
                    value=0,
                    step=1,
                    help="Use con precaución. Primero deben eliminarse las imputaciones."
                )
                
                if sel_cobro > 0:
                    cur.execute("""
                        SELECT c.*, cl.nombre as cliente_nombre
                        FROM cobros c
                        JOIN clientes cl ON c.cliente_id = cl.id
                        WHERE c.id=?
                    """, (sel_cobro,))
                    cobro = cur.fetchone()
                    
                    if not cobro:
                        st.warning("⚠️ Cobro no encontrado")
                    else:
                        st.info(f"""
                        **Cobro ID: {cobro['id']}**
                        - Cliente: {cobro['cliente_nombre']}
                        - Fecha: {cobro['fecha']}
                        - Importe: ${safe_float(cobro['importe']):.2f}
                        - Medio: {cobro['medio'] or 'N/A'}
                        """)
                        
                        if st.button("🗑️ Eliminar Cobro", type="secondary"):
                            try:
                                cur.execute("SELECT COUNT(*) as cnt FROM devengamientos_cobros WHERE cobro_id=?", (sel_cobro,))
                                imputaciones = cur.fetchone()['cnt']
                                
                                if imputaciones > 0:
                                    st.error(f"❌ No se puede eliminar: tiene {imputaciones} imputaciones a devengamientos. Elimine primero las imputaciones.")
                                else:
                                    cur.execute("DELETE FROM cobros WHERE id=?", (sel_cobro,))
                                    con.commit()
                                    st.success("✅ Cobro eliminado correctamente")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error al eliminar: {e}")
    
    except Exception as e:
        st.error(f"❌ Error: {e}")

# ---------- Ajustes ----------
elif menu == "Ajustes":
    st.header("⚖️ Gestión de Ajustes Contables")
    
    try:
        con = get_conn()
        if not con:
            st.error("❌ No se pudo conectar a la base de datos")
        else:
            cur = con.cursor()
            cur.execute("SELECT id, nombre, cuit FROM clientes ORDER BY nombre")
            clients = cur.fetchall()
            cm = {c['id']: f"{c['nombre']} (CUIT: {c['cuit'] or 'N/A'}, ID: {c['id']})" for c in clients}
            
            if not cm:
                st.warning("⚠️ No hay clientes registrados")
            else:
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    with st.form("form_ajuste"):
                        st.subheader("➕ Registrar Ajuste")
                        
                        cliente_id = st.selectbox(
                            "Cliente *",
                            options=[0] + list(cm.keys()),
                            format_func=lambda x: "- Seleccione un cliente -" if x == 0 else cm[x],
                            help="Cliente al que se aplicará el ajuste"
                        )
                        
                        fecha = st.date_input("Fecha *", value=date.today(), help="Fecha del ajuste contable")
                        descripcion = st.text_input("Descripción *", help="Motivo del ajuste")
                        
                        st.info("""
                        **💡 Monto:**
                        - Positivo (+): Aumenta la deuda del cliente
                        - Negativo (-): Disminuye la deuda del cliente
                        """)
                        
                        monto = st.text_input("Monto *", placeholder="Ej: 100 o -50", help="Ingrese el monto con signo")
                        
                        tipo = st.selectbox(
                            "Tipo *",
                            ["Bonificacion", "Recargo", "Adicional", "Nota_credito", "Nota_debito", "Otro"],
                            help="Categoría del ajuste"
                        )
                        
                        ref = st.text_input("ID Devengamiento (opcional)", help="ID del devengamiento al que se referencia")
                        
                        submit = st.form_submit_button("➕ Registrar Ajuste", use_container_width=True)
                    
                    if submit:
                        try:
                            if cliente_id == 0:
                                st.error("❌ Debe seleccionar un cliente")
                            elif not descripcion.strip():
                                st.error("❌ La descripción es obligatoria")
                            elif not monto.strip():
                                st.error("❌ El monto es obligatorio")
                            else:
                                m = float(parse_decimal(monto))
                                ref_id = int(ref) if ref and ref.strip() else None
                                
                                if ref_id:
                                    cur.execute("SELECT COUNT(1) as cnt FROM devengamientos WHERE id=?", (ref_id,))
                                    if cur.fetchone()['cnt'] == 0:
                                        st.warning("⚠️ Devengamiento no existe; se guardará sin referencia")
                                        ref_id = None
                                
                                cur.execute("""
                                    INSERT INTO ajustes (cliente_id, fecha, descripcion, monto, tipo, referencia_devengamiento_id)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """, (cliente_id, fecha.isoformat(), descripcion.strip(), m, tipo, ref_id))
                                
                                con.commit()
                                st.success(f"✅ Ajuste registrado (ID: {cur.lastrowid})")
                                st.rerun()
                        except ValueError as e:
                            st.error(f"❌ Error en formato de monto: {e}")
                        except Exception as e:
                            st.error(f"❌ Error al registrar ajuste: {e}")
                
                with col2:
                    st.subheader("📋 Ajustes Recientes")
                    
                    limite = st.selectbox("Mostrar últimos", [10, 25, 50, 100], index=1)
                    
                    cur.execute("""
                        SELECT a.*, c.nombre as cliente_nombre
                        FROM ajustes a
                        JOIN clientes c ON a.cliente_id = c.id
                        ORDER BY a.fecha DESC, a.id DESC
                        LIMIT ?
                    """, (limite,))
                    rows = cur.fetchall()
                    
                    if rows:
                        data = []
                        for r in rows:
                            monto_val = safe_float(r['monto'])
                            data.append({
                                'ID': r['id'],
                                'Fecha': r['fecha'],
                                'Cliente': r['cliente_nombre'],
                                'Tipo': r['tipo'],
                                'Descripción': r['descripcion'][:40] + '...' if len(r['descripcion'] or '') > 40 else (r['descripcion'] or ''),
                                'Monto': f"${monto_val:+.2f}",
                                'Efecto': '📈 Aumenta deuda' if monto_val > 0 else '📉 Disminuye deuda'
                            })
                        
                        df = pd.DataFrame(data)
                        st.dataframe(df, use_container_width=True, height=400)
                        st.caption(f"Mostrando {len(rows)} ajuste(s)")
                    else:
                        st.info("ℹ️ No hay ajustes registrados")
                
                st.markdown("---")
                st.subheader("🗑️ Eliminar Ajuste")
                
                sel_ajuste = st.number_input(
                    "ID del ajuste para eliminar (0 = ninguno)",
                    min_value=0,
                    value=0,
                    step=1,
                    help="Eliminar un ajuste contable"
                )
                
                if sel_ajuste > 0:
                    cur.execute("""
                        SELECT a.*, c.nombre as cliente_nombre
                        FROM ajustes a
                        JOIN clientes c ON a.cliente_id = c.id
                        WHERE a.id=?
                    """, (sel_ajuste,))
                    ajuste = cur.fetchone()
                    
                    if not ajuste:
                        st.warning("⚠️ Ajuste no encontrado")
                    else:
                        monto_val = safe_float(ajuste['monto'])
                        st.info(f"""
                        **Ajuste {ajuste['tipo']}**
                        - Cliente: {ajuste['cliente_nombre']}
                        - Monto: ${monto_val:+.2f}
                        - Descripción: {ajuste['descripcion'] or 'N/A'}
                        - Fecha: {ajuste['fecha']}
                        """)
                        
                        if st.button("🗑️ Eliminar Ajuste", type="secondary"):
                            try:
                                cur.execute("DELETE FROM ajustes WHERE id=?", (sel_ajuste,))
                                con.commit()
                                st.success("✅ Ajuste eliminado correctamente")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error al eliminar: {e}")
    
    except Exception as e:
        st.error(f"❌ Error: {e}")

# ---------- Reportes ----------
elif menu == "Reportes":
    st.header("📊 Reportes y Exportaciones")
    
    try:
        con = get_conn()
        if not con:
            st.error("❌ No se pudo conectar a la base de datos")
        else:
            cur = con.cursor()
            
            rpt = st.selectbox(
                "Seleccione el tipo de reporte",
                ["Estado de Cuenta (Cliente)", "Reporte de Morosos", "Cobranzas por Mes", "Exportar Datos CSV"],
                help="Elija el reporte que desea generar"
            )
            
            st.markdown("---")
            
            # Estado de cuenta
            if rpt == "Estado de Cuenta (Cliente)":
                st.subheader("📄 Estado de Cuenta por Cliente")
                
                cur.execute("SELECT id, nombre, cuit FROM clientes ORDER BY nombre")
                clients = cur.fetchall()
                cm = {c['id']: f"{c['nombre']} (CUIT: {c['cuit'] or 'N/A'}, ID: {c['id']})" for c in clients}
                
                if not cm:
                    st.info("ℹ️ No hay clientes registrados")
                else:
                    sel = st.selectbox("Cliente", options=list(cm.keys()), format_func=lambda x: cm[x])
                    
                    col_btn = st.columns([1, 1, 3])
                    with col_btn[0]:
                        btn_generar = st.button("📊 Generar Estado", use_container_width=True, type="primary")
                    with col_btn[1]:
                        btn_pdf = st.button("📄 Generar PDF", use_container_width=True, type="secondary")
                    
                    if btn_generar or btn_pdf:
                        try:
                            with st.spinner("Generando estado de cuenta..."):
                                events = []
                                
                                # Devengamientos
                                cur.execute("""
                                    SELECT id, periodo_anyo, periodo_mes, importe, fecha_devengada
                                    FROM devengamientos
                                    WHERE cliente_id=?
                                    ORDER BY fecha_devengada, id
                                """, (sel,))
                                
                                for d in cur.fetchall():
                                    events.append({
                                        'fecha': d['fecha_devengada'],
                                        'concepto': f"Devengamiento {d['periodo_anyo']}/{d['periodo_mes']:02d} (ID: {d['id']})",
                                        'debito': safe_float(d['importe']),
                                        'credito': 0.0
                                    })
                                
                                # Ajustes
                                cur.execute("""
                                    SELECT fecha, tipo, descripcion, monto
                                    FROM ajustes
                                    WHERE cliente_id=?
                                    ORDER BY fecha, id
                                """, (sel,))
                                
                                for a in cur.fetchall():
                                    m = safe_float(a['monto'])
                                    events.append({
                                        'fecha': a['fecha'],
                                        'concepto': f"Ajuste {a['tipo']}: {a['descripcion'] or 'Sin descripción'}",
                                        'debito': m if m > 0 else 0.0,
                                        'credito': abs(m) if m < 0 else 0.0
                                    })
                                
                                # Cobros
                                cur.execute("""
                                    SELECT id, fecha, importe, medio, referencia
                                    FROM cobros
                                    WHERE cliente_id=?
                                    ORDER BY fecha, id
                                """, (sel,))
                                
                                for c_ in cur.fetchall():
                                    events.append({
                                        'fecha': c_['fecha'],
                                        'concepto': f"Cobro {c_['medio'] or 'Sin medio'} (Ref: {c_['referencia'] or 'N/A'})",
                                        'debito': 0.0,
                                        'credito': safe_float(c_['importe'])
                                    })
                                
                                # Ordenar eventos
                                events.sort(key=lambda x: (x['fecha'], x['concepto']))
                                
                                if btn_pdf and events:
                                    # Generar PDF
                                    pdf_buffer = generar_pdf_estado_cuenta(sel, cm[sel].split('(')[0].strip(), events)
                                    if pdf_buffer:
                                        st.download_button(
                                            label="⬇️ Descargar PDF",
                                            data=pdf_buffer,
                                            file_name=f"estado_cuenta_{sel}_{date.today().strftime('%Y%m%d')}.pdf",
                                            mime="application/pdf",
                                            use_container_width=True
                                        )
                                
                                if events:
                                    # Mostrar tabla
                                    df_events = pd.DataFrame(events)
                                    df_events['saldo'] = (df_events['debito'] - df_events['credito']).cumsum()
                                    
                                    # Formatear para display
                                    df_display = df_events.copy()
                                    df_display['debito'] = df_display['debito'].apply(lambda x: format_currency_ar(x) if x > 0 else "-")
                                    df_display['credito'] = df_display['credito'].apply(lambda x: format_currency_ar(x) if x > 0 else "-")
                                    df_display['saldo'] = df_display['saldo'].apply(lambda x: format_currency_ar(x))
                                    
                                    st.dataframe(df_display, use_container_width=True, height=500)
                                    
                                    # Totales
                                    st.markdown("---")
                                    
                                    cur.execute("SELECT COALESCE(SUM(importe), 0) as total FROM devengamientos WHERE cliente_id=?", (sel,))
                                    total_dev = safe_float(cur.fetchone()['total'])
                                    
                                    cur.execute("SELECT COALESCE(SUM(monto), 0) as total FROM ajustes WHERE cliente_id=?", (sel,))
                                    total_ajustes = safe_float(cur.fetchone()['total'])
                                    
                                    cur.execute("SELECT COALESCE(SUM(importe), 0) as total FROM cobros WHERE cliente_id=?", (sel,))
                                    total_cobros = safe_float(cur.fetchone()['total'])
                                    
                                    saldo_final = total_dev + total_ajustes - total_cobros
                                    
                                    col_res = st.columns(4)
                                    with col_res[0]:
                                        st.metric("📊 Total Devengado", format_currency_ar(total_dev))
                                    with col_res[1]:
                                        st.metric("⚖️ Total Ajustes", format_currency_ar(total_ajustes))
                                    with col_res[2]:
                                        st.metric("💵 Total Cobrado", format_currency_ar(total_cobros))
                                    with col_res[3]:
                                        delta_color = "inverse" if saldo_final > 0 else "normal"
                                        st.metric("💰 Saldo Final", format_currency_ar(saldo_final), delta=None)
                                        if saldo_final > 0:
                                            st.error(f"⚠️ Cliente debe: {format_currency_ar(saldo_final)}")
                                        elif saldo_final < 0:
                                            st.info(f"✅ Saldo a favor: {format_currency_ar(abs(saldo_final))}")
                                        else:
                                            st.success("✅ Cuenta saldada")
                                else:
                                    st.info("ℹ️ Sin movimientos para este cliente")
                        
                        except Exception as e:
                            st.error(f"❌ Error al generar estado de cuenta: {e}")
            
            # Reporte de morosos
            elif rpt == "Reporte de Morosos":
                st.subheader("⚠️ Reporte de Clientes Morosos")
                
                dias = st.number_input(
                    "Días de atraso mínimo",
                    min_value=1,
                    max_value=365,
                    value=30,
                    help="Clientes con devengamientos vencidos hace más de estos días"
                )
                
                if st.button("📊 Generar Reporte", type="primary"):
                    try:
                        with st.spinner("Analizando clientes morosos..."):
                            fecha_lim = (date.today() - timedelta(days=dias)).isoformat()
                            
                            cur.execute("""
                                SELECT DISTINCT
                                    c.id,
                                    c.nombre,
                                    c.email,
                                    c.telefono,
                                    COUNT(DISTINCT d.id) as devengamientos_vencidos
                                FROM clientes c
                                JOIN devengamientos d ON c.id = d.cliente_id
                                WHERE d.fecha_devengada <= ?
                                    AND c.activo = 1
                                GROUP BY c.id, c.nombre, c.email, c.telefono
                                ORDER BY devengamientos_vencidos DESC, c.nombre
                            """, (fecha_lim,))
                            
                            rows = cur.fetchall()
                            
                            if rows:
                                # Calcular saldo para cada cliente moroso
                                data = []
                                total_deuda = 0.0
                                
                                for r in rows:
                                    # Calcular saldo del cliente
                                    cur.execute("""
                                        SELECT COALESCE(SUM(importe), 0) as total_dev
                                        FROM devengamientos
                                        WHERE cliente_id=?
                                    """, (r['id'],))
                                    dev = safe_float(cur.fetchone()['total_dev'])
                                    
                                    cur.execute("""
                                        SELECT COALESCE(SUM(monto), 0) as total_cob
                                        FROM devengamientos_cobros dc
                                        JOIN devengamientos d ON dc.devengamiento_id = d.id
                                        WHERE d.cliente_id=?
                                    """, (r['id'],))
                                    cob = safe_float(cur.fetchone()['total_cob'])
                                    
                                    cur.execute("""
                                        SELECT COALESCE(SUM(monto), 0) as total_aj
                                        FROM ajustes
                                        WHERE cliente_id=?
                                    """, (r['id'],))
                                    aj = safe_float(cur.fetchone()['total_aj'])
                                    
                                    saldo = dev + aj - cob
                                    
                                    if saldo > 0.01:  # Solo mostrar si realmente debe
                                        total_deuda += saldo
                                        data.append({
                                            'ID': r['id'],
                                            'Cliente': r['nombre'],
                                            'Email': r['email'] or '-',
                                            'Teléfono': r['telefono'] or '-',
                                            'Dev. Vencidos': r['devengamientos_vencidos'],
                                            'Saldo Pendiente': f"${saldo:.2f}"
                                        })
                                
                                if data:
                                    df = pd.DataFrame(data)
                                    st.dataframe(df, use_container_width=True, height=400)
                                    
                                    col_metric = st.columns(3)
                                    with col_metric[0]:
                                        st.metric("⚠️ Clientes Morosos", len(data))
                                    with col_metric[1]:
                                        st.metric("💰 Deuda Total", f"${total_deuda:.2f}")
                                    with col_metric[2]:
                                        promedio = total_deuda / len(data) if len(data) > 0 else 0
                                        st.metric("📊 Deuda Promedio", f"${promedio:.2f}")
                                    
                                    # Export CSV
                                    csv_buf = df.to_csv(index=False).encode('utf-8')
                                    st.download_button(
                                        label="⬇️ Descargar CSV",
                                        data=csv_buf,
                                        file_name=f"morosos_{date.today().strftime('%Y%m%d')}.csv",
                                        mime='text/csv'
                                    )
                                else:
                                    st.success("✅ No hay clientes morosos con saldo pendiente")
                            else:
                                st.success("✅ No hay clientes con devengamientos vencidos")
                    
                    except Exception as e:
                        st.error(f"❌ Error al generar reporte de morosos: {e}")
            
            # Cobranzas por mes
            elif rpt == "Cobranzas por Mes":
                st.subheader("💵 Reporte de Cobranzas Mensuales")
                
                col_fecha = st.columns(2)
                with col_fecha[0]:
                    mes = st.number_input("Mes", min_value=1, max_value=12, value=date.today().month)
                with col_fecha[1]:
                    anyo = st.number_input("Año", min_value=2000, max_value=2100, value=date.today().year)
                
                col_btn = st.columns([1, 1, 3])
                with col_btn[0]:
                    btn_gen = st.button("📊 Generar Reporte", use_container_width=True, type="primary")
                with col_btn[1]:
                    btn_pdf_cob = st.button("📄 Generar PDF", use_container_width=True, type="secondary")
                
                if btn_gen or btn_pdf_cob:
                    try:
                        with st.spinner("Generando reporte de cobranzas..."):
                            primer = date(anyo, mes, 1).isoformat()
                            ultimo = ultimo_dia_mes(anyo, mes).isoformat()
                            
                            cur.execute("""
                                SELECT c.*, cl.nombre as cliente_nombre
                                FROM cobros c
                                JOIN clientes cl ON c.cliente_id = cl.id
                                WHERE c.fecha >= ? AND c.fecha <= ?
                                ORDER BY c.fecha, c.id
                            """, (primer, ultimo))
                            
                            rows = cur.fetchall()
                            
                            if rows:
                                total = sum(safe_float(r['importe']) for r in rows)
                                
                                if btn_pdf_cob:
                                    pdf_buffer = generar_pdf_reporte_cobranzas(rows, mes, anyo, total)
                                    if pdf_buffer:
                                        st.download_button(
                                            label="⬇️ Descargar PDF",
                                            data=pdf_buffer,
                                            file_name=f"cobranzas_{anyo}_{mes:02d}.pdf",
                                            mime="application/pdf",
                                            use_container_width=True
                                        )
                                
                                data = []
                                for r in rows:
                                    data.append({
                                        'ID': r['id'],
                                        'Fecha': r['fecha'],
                                        'Cliente': r['cliente_nombre'],
                                        'Importe': f"${safe_float(r['importe']):.2f}",
                                        'Medio': r['medio'] or 'N/A',
                                        'Referencia': r['referencia'] or '-',
                                        'Observación': r['observacion'] or '-'
                                    })
                                
                                df = pd.DataFrame(data)
                                st.dataframe(df, use_container_width=True, height=400)
                                
                                col_sum = st.columns(3)
                                with col_sum[0]:
                                    st.metric("💰 Total Cobrado", f"${total:.2f}")
                                with col_sum[1]:
                                    st.metric("📊 Cantidad de Cobros", len(rows))
                                with col_sum[2]:
                                    promedio = total / len(rows) if len(rows) > 0 else 0
                                    st.metric("📈 Cobro Promedio", f"${promedio:.2f}")
                                
                                # Export CSV
                                csv_buf = df.to_csv(index=False).encode('utf-8')
                                st.download_button(
                                    label="⬇️ Descargar CSV",
                                    data=csv_buf,
                                    file_name=f"cobranzas_{anyo}_{mes:02d}.csv",
                                    mime='text/csv'
                                )
                            else:
                                st.info(f"ℹ️ No hay cobros registrados en {mes:02d}/{anyo}")
                    
                    except Exception as e:
                        st.error(f"❌ Error al generar reporte: {e}")
            
            # Export CSV
            elif rpt == "Exportar Datos CSV":
                st.subheader("📥 Exportar Tablas a CSV")
                
                tbl = st.selectbox(
                    "Tabla a exportar",
                    ["clientes", "planes", "devengamientos", "cobros", "ajustes"],
                    help="Seleccione la tabla que desea exportar"
                )
                
                if st.button("📥 Generar Exportación", type="primary"):
                    try:
                        with st.spinner(f"Exportando tabla {tbl}..."):
                            cur.execute(f"SELECT * FROM {tbl} ORDER BY id")
                            rows = cur.fetchall()
                            
                            if not rows:
                                st.info(f"ℹ️ No hay datos en la tabla {tbl}")
                            else:
                                df = pd.DataFrame(rows)
                                
                                st.success(f"✅ {len(rows)} registro(s) encontrado(s)")
                                st.dataframe(df.head(20), use_container_width=True)
                                
                                if len(rows) > 20:
                                    st.caption(f"Mostrando primeros 20 de {len(rows)} registros")
                                
                                csv_buf = df.to_csv(index=False).encode('utf-8')
                                st.download_button(
                                    label=f"⬇️ Descargar {tbl}.csv",
                                    data=csv_buf,
                                    file_name=f"{tbl}_{date.today().strftime('%Y%m%d')}.csv",
                                    mime='text/csv',
                                    use_container_width=True
                                )
                    
                    except Exception as e:
                        st.error(f"❌ Error al exportar: {e}")
    
    except Exception as e:
        st.error(f"❌ Error: {e}")

# ---------- Backup ----------
elif menu == "Backup":
    st.header("💾 Backup de Base de Datos")
    
    st.info("""
    **ℹ️ Información sobre Backups:**
    - Los backups son copias de seguridad completas de su base de datos
    - Se almacenan en la carpeta `backups/`
    - Recomendamos realizar backups periódicamente
    - Puede restaurar manualmente reemplazando el archivo abonos.db
    """)
    
    col_backup = st.columns([1, 2])
    
    with col_backup[0]:
        if st.button("💾 Crear Backup Ahora", type="primary", use_container_width=True):
            try:
                with st.spinner("Creando backup..."):
                    r = backup_database()
                    if r:
                        st.success(f"✅ Backup creado exitosamente:\n`{r}`")
                    else:
                        st.error("❌ No se pudo crear el backup")
            except Exception as e:
                st.error(f"❌ Error al crear backup: {e}")
    
    with col_backup[1]:
        st.markdown("### 📂 Backups Existentes")
        
        try:
            backup_dir = Path(BACKUP_DIR)
            if backup_dir.exists():
                backups = sorted(backup_dir.glob("abonos_*.db"), reverse=True)
                
                if backups:
                    data = []
                    for bk in backups[:10]:  # Mostrar últimos 10
                        stat = bk.stat()
                        data.append({
                            'Archivo': bk.name,
                            'Tamaño': f"{stat.st_size / 1024:.2f} KB",
                            'Fecha Creación': datetime.fromtimestamp(stat.st_mtime).strftime('%d/%m/%Y %H:%M')
                        })
                    
                    df_backups = pd.DataFrame(data)
                    st.dataframe(df_backups, use_container_width=True)
                    st.caption(f"Mostrando últimos {min(len(backups), 10)} de {len(backups)} backup(s)")
                else:
                    st.info("ℹ️ No hay backups disponibles")
            else:
                st.info("ℹ️ La carpeta de backups no existe aún")
        except Exception as e:
            st.error(f"❌ Error al listar backups: {e}")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("""
<div style='text-align: center; color: #666;'>
    <p><strong>Sistema de Gestión de Abonos v2.1</strong></p>
    <p style='font-size: 0.8em;'>Desarrollado con ❤️ usando Streamlit</p>
    <p style='font-size: 0.7em;'>© 2025 LS - Todos los derechos reservados</p>
</div>
""", unsafe_allow_html=True)
