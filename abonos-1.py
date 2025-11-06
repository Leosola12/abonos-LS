#!/usr/bin/env python3
"""
abonos.py - Sistema robusto de gestión de abonos
Versión mejorada con validaciones, backups, reportes y manejo de errores completo.
Requiere Python 3.8+
"""

import sqlite3
from datetime import datetime, date, timedelta
from pathlib import Path
import os
import sys
import shutil
from typing import Optional, List, Tuple
from decimal import Decimal, InvalidOperation

# ============================= CONFIGURACIÓN =============================

DB_FILE = "abonos.db"
BACKUP_DIR = "backups"
LOG_FILE = "abonos.log"

# Colores para terminal (compatible con Windows y Unix)
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    
    @classmethod
    def disable(cls):
        cls.HEADER = ''
        cls.BLUE = ''
        cls.CYAN = ''
        cls.GREEN = ''
        cls.WARNING = ''
        cls.FAIL = ''
        cls.ENDC = ''
        cls.BOLD = ''
        cls.UNDERLINE = ''

# Detectar si estamos en Windows sin soporte ANSI
if os.name == 'nt':
    try:
        import colorama
        colorama.init()
    except ImportError:
        Colors.disable()

# ============================= LOGGING =============================

def log(message: str, level: str = "INFO"):
    """Registra eventos en archivo de log"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}\n"
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_entry)
    except Exception as e:
        print(f"{Colors.WARNING}Warning: No se pudo escribir en log: {e}{Colors.ENDC}")

# ============================= HELPERS =============================

def fmt_date(d) -> str:
    """Formatea una fecha para mostrar"""
    if not d:
        return ""
    if isinstance(d, str):
        return d
    return d.strftime("%Y-%m-%d")

def parse_date(s: str) -> date:
    """Parsea una fecha desde string con múltiples formatos"""
    if not s:
        raise ValueError("Fecha vacía")
    
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    raise ValueError("Formato de fecha inválido. Use YYYY-MM-DD o DD/MM/YYYY")

def parse_decimal(s: str) -> Decimal:
    """Parsea un número decimal con validación"""
    if not s:
        raise ValueError("Valor vacío")
    
    s = s.strip().replace(',', '.')
    try:
        return Decimal(s)
    except InvalidOperation:
        raise ValueError(f"Número inválido: {s}")

def clear_screen():
    """Limpia la pantalla de la terminal"""
    os.system('cls' if os.name == 'nt' else 'clear')

def pause():
    """Pausa la ejecución hasta que el usuario presione Enter"""
    input(f"\n{Colors.CYAN}Presione Enter para continuar...{Colors.ENDC}")

def confirm(message: str) -> bool:
    """Solicita confirmación al usuario"""
    resp = input(f"{Colors.WARNING}{message} (s/n): {Colors.ENDC}").strip().lower()
    return resp in ('s', 'si', 'sí', 'y', 'yes')

def print_header(title: str):
    """Imprime un encabezado formateado"""
    clear_screen()
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{title.center(70)}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.ENDC}\n")

def print_success(message: str):
    """Imprime un mensaje de éxito"""
    print(f"{Colors.GREEN}✓ {message}{Colors.ENDC}")

def print_error(message: str):
    """Imprime un mensaje de error"""
    print(f"{Colors.FAIL}✗ {message}{Colors.ENDC}")

def print_warning(message: str):
    """Imprime un mensaje de advertencia"""
    print(f"{Colors.WARNING}⚠ {message}{Colors.ENDC}")

# ============================= DATABASE =============================

def get_conn():
    """Obtiene conexión a la base de datos"""
    try:
        con = sqlite3.connect(DB_FILE)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys = ON")
        return con
    except sqlite3.Error as e:
        print_error(f"Error al conectar a la base de datos: {e}")
        log(f"Error de conexión DB: {e}", "ERROR")
        sys.exit(1)

def init_db():
    """Inicializa la base de datos con todas las tablas"""
    created = not os.path.exists(DB_FILE)
    
    try:
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
            FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE,
            CHECK (importe >= 0)
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
            UNIQUE(cliente_id, plan_id, periodo_anyo, periodo_mes),
            CHECK (importe >= 0),
            CHECK (periodo_mes BETWEEN 1 AND 12)
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
            FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE,
            CHECK (importe > 0)
        );

        CREATE TABLE IF NOT EXISTS devengamientos_cobros (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            devengamiento_id INTEGER NOT NULL,
            cobro_id INTEGER NOT NULL,
            monto REAL NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (devengamiento_id) REFERENCES devengamientos(id) ON DELETE CASCADE,
            FOREIGN KEY (cobro_id) REFERENCES cobros(id) ON DELETE CASCADE,
            CHECK (monto > 0)
        );

        CREATE TABLE IF NOT EXISTS ajustes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id INTEGER NOT NULL,
            fecha TEXT NOT NULL,
            descripcion TEXT NOT NULL,
            monto REAL NOT NULL,
            tipo TEXT CHECK(tipo IN ('bonificacion', 'recargo', 'adicional', 'nota_credito', 'nota_debito', 'otro')),
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
        con.close()
        
        if created:
            print_success(f"Base de datos creada: {DB_FILE}")
            log("Base de datos inicializada", "INFO")
        
    except sqlite3.Error as e:
        print_error(f"Error al inicializar la base de datos: {e}")
        log(f"Error init_db: {e}", "ERROR")
        sys.exit(1)

def backup_database():
    """Crea backup de la base de datos"""
    if not os.path.exists(DB_FILE):
        print_warning("No hay base de datos para respaldar")
        return False
    
    try:
        Path(BACKUP_DIR).mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(BACKUP_DIR, f"abonos_{timestamp}.db")
        shutil.copy2(DB_FILE, backup_file)
        
        # Mantener solo los últimos 30 backups
        backups = sorted(Path(BACKUP_DIR).glob("abonos_*.db"))
        if len(backups) > 30:
            for old_backup in backups[:-30]:
                old_backup.unlink()
        
        print_success(f"Backup creado: {backup_file}")
        log(f"Backup creado: {backup_file}", "INFO")
        return True
    except Exception as e:
        print_error(f"Error al crear backup: {e}")
        log(f"Error backup: {e}", "ERROR")
        return False

def auto_backup():
    """Crea backup automático si han pasado más de 24 horas desde el último"""
    try:
        backup_dir = Path(BACKUP_DIR)
        if not backup_dir.exists():
            backup_database()
            return
        
        backups = sorted(backup_dir.glob("abonos_*.db"))
        if not backups:
            backup_database()
            return
        
        last_backup = backups[-1]
        last_backup_time = datetime.fromtimestamp(last_backup.stat().st_mtime)
        
        if datetime.now() - last_backup_time > timedelta(hours=24):
            backup_database()
    except Exception as e:
        log(f"Error en auto-backup: {e}", "ERROR")

# ============================= VALIDACIONES =============================

def cliente_exists(cliente_id: int) -> bool:
    """Verifica si existe un cliente"""
    try:
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) as cnt FROM clientes WHERE id=?", (cliente_id,))
        result = cur.fetchone()['cnt'] > 0
        con.close()
        return result
    except Exception as e:
        print_error(f"Error al verificar cliente: {e}")
        return False

def plan_exists(plan_id: int) -> bool:
    """Verifica si existe un plan"""
    try:
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) as cnt FROM planes WHERE id=?", (plan_id,))
        result = cur.fetchone()['cnt'] > 0
        con.close()
        return result
    except Exception as e:
        print_error(f"Error al verificar plan: {e}")
        return False

def devengamiento_exists(deveng_id: int) -> bool:
    """Verifica si existe un devengamiento"""
    try:
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) as cnt FROM devengamientos WHERE id=?", (deveng_id,))
        result = cur.fetchone()['cnt'] > 0
        con.close()
        return result
    except Exception as e:
        print_error(f"Error al verificar devengamiento: {e}")
        return False

# ============================= CLIENTES =============================

def add_cliente():
    """Agrega un nuevo cliente con validaciones"""
    print_header("AGREGAR NUEVO CLIENTE")
    
    try:
        nombre = input("Nombre/Razón Social (*): ").strip()
        if not nombre:
            print_error("El nombre es obligatorio")
            return
        
        cuit = input("CUIT/DNI: ").strip() or None
        contacto = input("Persona de contacto: ").strip() or None
        email = input("Email: ").strip() or None
        telefono = input("Teléfono: ").strip() or None
        direccion = input("Dirección: ").strip() or None
        notas = input("Notas adicionales: ").strip() or None
        
        con = get_conn()
        cur = con.cursor()
        cur.execute(
            """INSERT INTO clientes (nombre, cuit, contacto, email, telefono, direccion, notas) 
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (nombre, cuit, contacto, email, telefono, direccion, notas)
        )
        cliente_id = cur.lastrowid
        con.commit()
        con.close()
        
        print_success(f"Cliente agregado con ID: {cliente_id}")
        log(f"Cliente agregado: {nombre} (ID: {cliente_id})", "INFO")
        
    except Exception as e:
        print_error(f"Error al agregar cliente: {e}")
        log(f"Error add_cliente: {e}", "ERROR")

def list_clients(pause_after: bool = True):
    """Lista todos los clientes"""
    try:
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT * FROM clientes ORDER BY nombre")
        rows = cur.fetchall()
        con.close()
        
        if not rows:
            print_warning("No hay clientes registrados")
            return
        
        print(f"\n{Colors.BOLD}{'ID':<5} {'Nombre':<30} {'CUIT':<15} {'Contacto':<25} {'Estado':<10}{Colors.ENDC}")
        print("-" * 90)
        
        for r in rows:
            estado = f"{Colors.GREEN}Activo{Colors.ENDC}" if r['activo'] else f"{Colors.FAIL}Inactivo{Colors.ENDC}"
            nombre = r['nombre'][:29]
            cuit = (r['cuit'] or '-')[:14]
            contacto = (r['email'] or r['telefono'] or r['contacto'] or '-')[:24]
            
            print(f"{r['id']:<5} {nombre:<30} {cuit:<15} {contacto:<25} {estado}")
        
        print(f"\n{Colors.CYAN}Total: {len(rows)} cliente(s){Colors.ENDC}")
        
        if pause_after:
            pause()
            
    except Exception as e:
        print_error(f"Error al listar clientes: {e}")
        log(f"Error list_clients: {e}", "ERROR")

def edit_cliente():
    """Edita un cliente existente"""
    print_header("EDITAR CLIENTE")
    list_clients(pause_after=False)
    
    try:
        cliente_id = int(input("\nID del cliente a editar: ").strip())
        
        if not cliente_exists(cliente_id):
            print_error("Cliente no encontrado")
            return
        
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT * FROM clientes WHERE id=?", (cliente_id,))
        cliente = cur.fetchone()
        
        print(f"\n{Colors.CYAN}Datos actuales (presione Enter para mantener):{Colors.ENDC}")
        
        nombre = input(f"Nombre [{cliente['nombre']}]: ").strip() or cliente['nombre']
        cuit = input(f"CUIT [{cliente['cuit'] or '-'}]: ").strip() or cliente['cuit']
        contacto = input(f"Contacto [{cliente['contacto'] or '-'}]: ").strip() or cliente['contacto']
        email = input(f"Email [{cliente['email'] or '-'}]: ").strip() or cliente['email']
        telefono = input(f"Teléfono [{cliente['telefono'] or '-'}]: ").strip() or cliente['telefono']
        direccion = input(f"Dirección [{cliente['direccion'] or '-'}]: ").strip() or cliente['direccion']
        notas = input(f"Notas [{cliente['notas'] or '-'}]: ").strip() or cliente['notas']
        
        cur.execute(
            """UPDATE clientes 
               SET nombre=?, cuit=?, contacto=?, email=?, telefono=?, direccion=?, notas=?, updated_at=datetime('now')
               WHERE id=?""",
            (nombre, cuit, contacto, email, telefono, direccion, notas, cliente_id)
        )
        con.commit()
        con.close()
        
        print_success("Cliente actualizado correctamente")
        log(f"Cliente editado: ID {cliente_id}", "INFO")
        
    except ValueError:
        print_error("ID inválido")
    except Exception as e:
        print_error(f"Error al editar cliente: {e}")
        log(f"Error edit_cliente: {e}", "ERROR")

def toggle_cliente_estado():
    """Activa o desactiva un cliente"""
    print_header("ACTIVAR/DESACTIVAR CLIENTE")
    list_clients(pause_after=False)
    
    try:
        cliente_id = int(input("\nID del cliente: ").strip())
        
        if not cliente_exists(cliente_id):
            print_error("Cliente no encontrado")
            return
        
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT activo, nombre FROM clientes WHERE id=?", (cliente_id,))
        r = cur.fetchone()
        
        nuevo_estado = 0 if r['activo'] else 1
        accion = "activar" if nuevo_estado else "desactivar"
        
        if confirm(f"¿Confirma {accion} al cliente '{r['nombre']}'?"):
            cur.execute("UPDATE clientes SET activo=?, updated_at=datetime('now') WHERE id=?", (nuevo_estado, cliente_id))
            con.commit()
            print_success(f"Cliente {accion}do correctamente")
            log(f"Cliente {accion}do: ID {cliente_id}", "INFO")
        
        con.close()
        
    except ValueError:
        print_error("ID inválido")
    except Exception as e:
        print_error(f"Error al cambiar estado: {e}")
        log(f"Error toggle_cliente_estado: {e}", "ERROR")

# ============================= PLANES =============================

def add_plan():
    """Agrega un nuevo plan de abono"""
    print_header("AGREGAR NUEVO PLAN")
    list_clients(pause_after=False)
    
    try:
        cliente_id = int(input("\nID del cliente (*): ").strip())
        
        if not cliente_exists(cliente_id):
            print_error("Cliente no encontrado")
            return
        
        descripcion = input("Descripción del plan: ").strip() or None
        
        importe_str = input("Importe mensual (*): ").strip()
        importe = float(parse_decimal(importe_str))
        
        if importe < 0:
            print_error("El importe no puede ser negativo")
            return
        
        fecha_inicio_str = input("Fecha de inicio (YYYY-MM-DD) [hoy]: ").strip()
        if not fecha_inicio_str:
            fecha_inicio = date.today()
        else:
            fecha_inicio = parse_date(fecha_inicio_str)
        
        fecha_fin_str = input("Fecha de fin (YYYY-MM-DD) [indefinido]: ").strip()
        fecha_fin = parse_date(fecha_fin_str) if fecha_fin_str else None
        
        periodicidad = input("Periodicidad [mensual]: ").strip() or "mensual"
        
        con = get_conn()
        cur = con.cursor()
        cur.execute(
            """INSERT INTO planes (cliente_id, descripcion, importe, fecha_inicio, fecha_fin, periodicidad) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            (cliente_id, descripcion, importe, fecha_inicio.isoformat(), 
             fecha_fin.isoformat() if fecha_fin else None, periodicidad)
        )
        plan_id = cur.lastrowid
        con.commit()
        con.close()
        
        print_success(f"Plan agregado con ID: {plan_id}")
        log(f"Plan agregado: cliente {cliente_id}, importe {importe} (ID: {plan_id})", "INFO")
        
    except ValueError as e:
        print_error(str(e))
    except Exception as e:
        print_error(f"Error al agregar plan: {e}")
        log(f"Error add_plan: {e}", "ERROR")

def list_plans(cliente_id: Optional[int] = None, pause_after: bool = True):
    """Lista planes de abono"""
    try:
        con = get_conn()
        cur = con.cursor()
        
        if cliente_id:
            cur.execute(
                """SELECT p.*, c.nombre as cliente_nombre 
                   FROM planes p 
                   JOIN clientes c ON p.cliente_id=c.id 
                   WHERE p.cliente_id=? 
                   ORDER BY p.activo DESC, p.fecha_inicio DESC""",
                (cliente_id,)
            )
        else:
            cur.execute(
                """SELECT p.*, c.nombre as cliente_nombre 
                   FROM planes p 
                   JOIN clientes c ON p.cliente_id=c.id 
                   ORDER BY p.activo DESC, c.nombre"""
            )
        
        rows = cur.fetchall()
        con.close()
        
        if not rows:
            print_warning("No hay planes registrados")
            return
        
        print(f"\n{Colors.BOLD}{'ID':<5} {'Cliente':<25} {'Descripción':<25} {'Importe':<12} {'Inicio':<12} {'Estado':<10}{Colors.ENDC}")
        print("-" * 95)
        
        for r in rows:
            estado = f"{Colors.GREEN}Activo{Colors.ENDC}" if r['activo'] else f"{Colors.FAIL}Inactivo{Colors.ENDC}"
            cliente = r['cliente_nombre'][:24]
            desc = (r['descripcion'] or '-')[:24]
            
            print(f"{r['id']:<5} {cliente:<25} {desc:<25} ${r['importe']:>10.2f} {r['fecha_inicio']:<12} {estado}")
        
        print(f"\n{Colors.CYAN}Total: {len(rows)} plan(es){Colors.ENDC}")
        
        if pause_after:
            pause()
            
    except Exception as e:
        print_error(f"Error al listar planes: {e}")
        log(f"Error list_plans: {e}", "ERROR")

def edit_plan():
    """Edita un plan existente"""
    print_header("EDITAR PLAN")
    list_plans(pause_after=False)
    
    try:
        plan_id = int(input("\nID del plan a editar: ").strip())
        
        if not plan_exists(plan_id):
            print_error("Plan no encontrado")
            return
        
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT * FROM planes WHERE id=?", (plan_id,))
        plan = cur.fetchone()
        
        print(f"\n{Colors.CYAN}Datos actuales (presione Enter para mantener):{Colors.ENDC}")
        
        descripcion = input(f"Descripción [{plan['descripcion'] or '-'}]: ").strip() or plan['descripcion']
        
        importe_str = input(f"Importe [{plan['importe']}]: ").strip()
        importe = float(parse_decimal(importe_str)) if importe_str else plan['importe']
        
        fecha_inicio_str = input(f"Fecha inicio [{plan['fecha_inicio']}]: ").strip()
        fecha_inicio = parse_date(fecha_inicio_str) if fecha_inicio_str else parse_date(plan['fecha_inicio'])
        
        fecha_fin_str = input(f"Fecha fin [{plan['fecha_fin'] or 'indefinido'}]: ").strip()
        if fecha_fin_str:
            fecha_fin = parse_date(fecha_fin_str)
        else:
            fecha_fin = parse_date(plan['fecha_fin']) if plan['fecha_fin'] else None
        
        periodicidad = input(f"Periodicidad [{plan['periodicidad']}]: ").strip() or plan['periodicidad']
        
        activo_str = input(f"Activo (1=sí, 0=no) [{plan['activo']}]: ").strip()
        activo = int(activo_str) if activo_str else plan['activo']
        
        cur.execute(
            """UPDATE planes 
               SET descripcion=?, importe=?, fecha_inicio=?, fecha_fin=?, periodicidad=?, activo=?, updated_at=datetime('now')
               WHERE id=?""",
            (descripcion, importe, fecha_inicio.isoformat(), 
             fecha_fin.isoformat() if fecha_fin else None, periodicidad, activo, plan_id)
        )
        con.commit()
        con.close()
        
        print_success("Plan actualizado correctamente")
        log(f"Plan editado: ID {plan_id}", "INFO")
        
    except ValueError as e:
        print_error(str(e))
    except Exception as e:
        print_error(f"Error al editar plan: {e}")
        log(f"Error edit_plan: {e}", "ERROR")

# ============================= DEVENGAMIENTOS =============================

def devengamiento_saldo(deveng_id: int) -> float:
    """Calcula el saldo pendiente de un devengamiento"""
    try:
        con = get_conn()
        cur = con.cursor()
        
        cur.execute("SELECT importe FROM devengamientos WHERE id=?", (deveng_id,))
        r = cur.fetchone()
        if not r:
            con.close()
            return 0.0
        
        importe = float(r['importe'])
        
        cur.execute(
            "SELECT COALESCE(SUM(monto),0) as aplicado FROM devengamientos_cobros WHERE devengamiento_id=?",
            (deveng_id,)
        )
        aplicado = float(cur.fetchone()['aplicado'])
        
        cur.execute(
            "SELECT COALESCE(SUM(monto),0) as ajustes FROM ajustes WHERE referencia_devengamiento_id=?",
            (deveng_id,)
        )
        ajustes = float(cur.fetchone()['ajustes'])
        
        con.close()
        
        saldo = importe + ajustes - aplicado
        return max(0.0, saldo)  # No devolver saldos negativos
        
    except Exception as e:
        log(f"Error calculando saldo devengamiento {deveng_id}: {e}", "ERROR")
        return 0.0

def generate_devengamientos_for(month: Optional[int] = None, year: Optional[int] = None):
    """Genera devengamientos para un período"""
    hoy = date.today()
    if not month:
        month = hoy.month
    if not year:
        year = hoy.year
    
    if not (1 <= month <= 12):
        print_error("Mes inválido (debe ser 1-12)")
        return
    
    print_header(f"GENERAR DEVENGAMIENTOS - {month:02d}/{year}")
    
    try:
        con = get_conn()
        cur = con.cursor()
        
        periodo_start = date(year, month, 1)
        
        # Obtener planes activos vigentes en el período
        cur.execute(
            """SELECT p.*, c.nombre as cliente_nombre 
               FROM planes p
               JOIN clientes c ON p.cliente_id = c.id
               WHERE p.activo = 1 AND c.activo = 1"""
        )
        planes = cur.fetchall()
        
        created = 0
        skipped = 0
        errors = 0
        
        for p in planes:
            try:
                fecha_inicio = parse_date(p['fecha_inicio'])
                fecha_fin = parse_date(p['fecha_fin']) if p['fecha_fin'] else None
                
                # Verificar vigencia del plan en el período
                if fecha_inicio > periodo_start:
                    skipped += 1
                    continue
                
                if fecha_fin:
                    # Si hay fecha fin, verificar que el período esté dentro
                    periodo_end = date(year, month, 28)  # Usamos día 28 como referencia
                    if fecha_fin < periodo_end:
                        skipped += 1
                        continue
                
                # Verificar si ya existe devengamiento
                cur.execute(
                    """SELECT COUNT(1) as cnt 
                       FROM devengamientos 
                       WHERE cliente_id=? AND plan_id=? AND periodo_anyo=? AND periodo_mes=?""",
                    (p['cliente_id'], p['id'], year, month)
                )
                
                if cur.fetchone()['cnt'] > 0:
                    skipped += 1
                    continue
                
                # Crear devengamiento
                fecha_dev = periodo_start.isoformat()
                cur.execute(
                    """INSERT INTO devengamientos 
                       (cliente_id, plan_id, periodo_anyo, periodo_mes, importe, fecha_devengada) 
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (p['cliente_id'], p['id'], year, month, p['importe'], fecha_dev)
                )
                created += 1
                
            except Exception as e:
                print_error(f"Error al procesar plan {p['id']}: {e}")
                log(f"Error en devengamiento plan {p['id']}: {e}", "ERROR")
                errors += 1
        
        con.commit()
        con.close()
        
        print(f"\n{Colors.GREEN}✓ Creados: {created}{Colors.ENDC}")
        print(f"{Colors.WARNING}⊘ Omitidos: {skipped}{Colors.ENDC}")
        if errors > 0:
            print(f"{Colors.FAIL}✗ Errores: {errors}{Colors.ENDC}")
        
        log(f"Devengamientos generados {month}/{year}: {created} creados, {skipped} omitidos", "INFO")
        
    except Exception as e:
        print_error(f"Error al generar devengamientos: {e}")
        log(f"Error generate_devengamientos: {e}", "ERROR")

def list_devengamientos(cliente_id: Optional[int] = None, only_pending: bool = False, pause_after: bool = True):
    """Lista devengamientos con saldos"""
    try:
        con = get_conn()
        cur = con.cursor()
        
        q = """SELECT d.*, c.nombre as cliente_nombre 
               FROM devengamientos d 
               JOIN clientes c ON d.cliente_id=c.id"""
        
        cond = []
        params = []
        
        if cliente_id:
            cond.append("d.cliente_id=?")
            params.append(cliente_id)
        
        if cond:
            q += " WHERE " + " AND ".join(cond)
        
        q += " ORDER BY d.periodo_anyo DESC, d.periodo_mes DESC, c.nombre"
        
        cur.execute(q, tuple(params))
        rows = cur.fetchall()
        
        if not rows:
            print_warning("No hay devengamientos")
            con.close()
            return
        
        # Filtrar pendientes si se solicita
        if only_pending:
            rows_filtered = []
            for r in rows:
                if devengamiento_saldo(r['id']) > 0.01:
                    rows_filtered.append(r)
            rows = rows_filtered
        
        if not rows:
            print_warning("No hay devengamientos pendientes")
            con.close()
            return
        
        print(f"\n{Colors.BOLD}{'ID':<5} {'Cliente':<25} {'Período':<10} {'Importe':<12} {'Cobrado':<12} {'Saldo':<12}{Colors.ENDC}")
        print("-" * 80)
        
        total_importe = 0.0
        total_saldo = 0.0
        
        for r in rows:
            saldo = devengamiento_saldo(r['id'])
            cobrado = r['importe'] - saldo
            
            color_saldo = Colors.GREEN if saldo < 0.01 else Colors.WARNING if saldo < r['importe'] else Colors.FAIL
            
            cliente = r['cliente_nombre'][:24]
            periodo = f"{r['periodo_anyo']}/{r['periodo_mes']:02d}"
            
            print(f"{r['id']:<5} {cliente:<25} {periodo:<10} ${r['importe']:>10.2f} ${cobrado:>10.2f} {color_saldo}${saldo:>10.2f}{Colors.ENDC}")
            
            total_importe += r['importe']
            total_saldo += saldo
        
        print("-" * 80)
        print(f"{'TOTALES':<42} ${total_importe:>10.2f} {' '*12} ${total_saldo:>10.2f}")
        print(f"\n{Colors.CYAN}Total: {len(rows)} devengamiento(s){Colors.ENDC}")
        
        con.close()
        
        if pause_after:
            pause()
            
    except Exception as e:
        print_error(f"Error al listar devengamientos: {e}")
        log(f"Error list_devengamientos: {e}", "ERROR")

# ============================= COBROS =============================

def record_cobro():
    """Registra un nuevo cobro con imputación automática o manual"""
    print_header("REGISTRAR COBRO")
    list_clients(pause_after=False)
    
    try:
        cliente_id = int(input("\nID del cliente que paga (*): ").strip())
        
        if not cliente_exists(cliente_id):
            print_error("Cliente no encontrado")
            return
        
        fecha_str = input("Fecha del cobro (YYYY-MM-DD) [hoy]: ").strip()
        if not fecha_str:
            fecha = date.today()
        else:
            fecha = parse_date(fecha_str)
        
        importe_str = input("Importe cobrado (*): ").strip()
        importe = float(parse_decimal(importe_str))
        
        if importe <= 0:
            print_error("El importe debe ser mayor a cero")
            return
        
        medio = input("Medio de pago (transferencia/efectivo/cheque/otro): ").strip() or "sin especificar"
        referencia = input("Referencia/Nº de operación (opcional): ").strip()
        observacion = input("Observación (opcional): ").strip()
        
        con = get_conn()
        cur = con.cursor()
        cur.execute(
            """INSERT INTO cobros (cliente_id, fecha, importe, medio, referencia, observacion) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            (cliente_id, fecha.isoformat(), importe, medio, referencia or None, observacion or None)
        )
        cobro_id = cur.lastrowid
        con.commit()
        
        print_success(f"Cobro registrado con ID: {cobro_id}")
        
        # Preguntar por imputación
        print(f"\n{Colors.CYAN}¿Cómo desea imputar este cobro?{Colors.ENDC}")
        print("1) Automático (desde el devengamiento más antiguo)")
        print("2) Manual (elegir devengamientos específicos)")
        print("3) No imputar ahora")
        
        opcion = input("Opción: ").strip()
        
        if opcion == '1':
            imputar_automatico(cur, cobro_id, cliente_id, importe)
            con.commit()
        elif opcion == '2':
            imputar_manual(cur, cobro_id, cliente_id, importe)
            con.commit()
        else:
            print_warning("Cobro no imputado. Puede imputarlo después desde el menú.")
        
        con.close()
        log(f"Cobro registrado: cliente {cliente_id}, importe {importe} (ID: {cobro_id})", "INFO")
        
    except ValueError as e:
        print_error(str(e))
    except Exception as e:
        print_error(f"Error al registrar cobro: {e}")
        log(f"Error record_cobro: {e}", "ERROR")

def imputar_automatico(cur, cobro_id: int, cliente_id: int, importe: float):
    """Imputa un cobro automáticamente desde los devengamientos más antiguos"""
    try:
        cur.execute(
            """SELECT d.* FROM devengamientos d 
               WHERE d.cliente_id=? 
               ORDER BY d.periodo_anyo, d.periodo_mes, d.id""",
            (cliente_id,)
        )
        devs = cur.fetchall()
        
        restante = importe
        imputado_total = 0.0
        
        for d in devs:
            if restante <= 0.01:
                break
            
            saldo = devengamiento_saldo(d['id'])
            if saldo <= 0.01:
                continue
            
            monto_a_imputar = min(restante, saldo)
            
            cur.execute(
                """INSERT INTO devengamientos_cobros (devengamiento_id, cobro_id, monto) 
                   VALUES (?, ?, ?)""",
                (d['id'], cobro_id, monto_a_imputar)
            )
            
            restante -= monto_a_imputar
            imputado_total += monto_a_imputar
            
            print(f"  → Imputado ${monto_a_imputar:.2f} al devengamiento {d['periodo_anyo']}/{d['periodo_mes']:02d} (ID: {d['id']})")
        
        print_success(f"Total imputado: ${imputado_total:.2f}")
        
        if restante > 0.01:
            print_warning(f"Saldo sin imputar: ${restante:.2f} (el cliente tiene crédito a favor)")
        
    except Exception as e:
        print_error(f"Error en imputación automática: {e}")
        log(f"Error imputar_automatico: {e}", "ERROR")

def imputar_manual(cur, cobro_id: int, cliente_id: int, importe: float):
    """Imputa un cobro manualmente eligiendo devengamientos"""
    try:
        cur.execute(
            """SELECT d.* FROM devengamientos d 
               WHERE d.cliente_id=? 
               ORDER BY d.periodo_anyo DESC, d.periodo_mes DESC""",
            (cliente_id,)
        )
        devs = cur.fetchall()
        
        if not devs:
            print_warning("No hay devengamientos para este cliente")
            return
        
        print(f"\n{Colors.CYAN}Devengamientos disponibles:{Colors.ENDC}")
        print(f"{'ID':<5} {'Período':<10} {'Importe':<12} {'Saldo':<12}")
        print("-" * 42)
        
        for d in devs:
            saldo = devengamiento_saldo(d['id'])
            if saldo > 0.01:
                print(f"{d['id']:<5} {d['periodo_anyo']}/{d['periodo_mes']:02d}   ${d['importe']:>10.2f} ${saldo:>10.2f}")
        
        print(f"\n{Colors.CYAN}Ingrese imputaciones como: ID:monto,ID:monto{Colors.ENDC}")
        print(f"Ejemplo: 5:1000,7:500")
        print(f"Monto disponible: ${importe:.2f}")
        
        line = input("\nImputaciones: ").strip()
        
        if not line:
            print_warning("No se realizaron imputaciones")
            return
        
        pairs = [p.strip() for p in line.split(',') if ':' in p]
        restante = importe
        
        for p in pairs:
            try:
                did_s, m_s = p.split(':')
                did = int(did_s)
                monto = float(parse_decimal(m_s))
                
                if monto <= 0:
                    print_error(f"Monto inválido: {monto}")
                    continue
                
                if not devengamiento_exists(did):
                    print_error(f"Devengamiento {did} no existe")
                    continue
                
                saldo = devengamiento_saldo(did)
                
                if monto > saldo + 0.01:
                    print_warning(f"Monto {monto:.2f} mayor que saldo {saldo:.2f}. Se ajusta al saldo.")
                    monto = saldo
                
                if monto > restante + 0.01:
                    print_warning(f"Monto {monto:.2f} mayor que disponible {restante:.2f}. Se ajusta.")
                    monto = restante
                
                if monto <= 0:
                    continue
                
                cur.execute(
                    """INSERT INTO devengamientos_cobros (devengamiento_id, cobro_id, monto) 
                       VALUES (?, ?, ?)""",
                    (did, cobro_id, monto)
                )
                
                restante -= monto
                print(f"  ✓ Imputado ${monto:.2f} al devengamiento {did}")
                
                if restante <= 0.01:
                    break
                    
            except ValueError as e:
                print_error(f"Error en '{p}': {e}")
        
        if restante > 0.01:
            print_warning(f"Saldo sin imputar: ${restante:.2f}")
        
    except Exception as e:
        print_error(f"Error en imputación manual: {e}")
        log(f"Error imputar_manual: {e}", "ERROR")

def list_cobros(cliente_id: Optional[int] = None, pause_after: bool = True):
    """Lista cobros registrados"""
    try:
        con = get_conn()
        cur = con.cursor()
        
        if cliente_id:
            cur.execute(
                """SELECT c.*, cl.nombre as cliente_nombre 
                   FROM cobros c 
                   JOIN clientes cl ON c.cliente_id=cl.id 
                   WHERE c.cliente_id=? 
                   ORDER BY c.fecha DESC""",
                (cliente_id,)
            )
        else:
            cur.execute(
                """SELECT c.*, cl.nombre as cliente_nombre 
                   FROM cobros c 
                   JOIN clientes cl ON c.cliente_id=cl.id 
                   ORDER BY c.fecha DESC"""
            )
        
        rows = cur.fetchall()
        con.close()
        
        if not rows:
            print_warning("No hay cobros registrados")
            return
        
        print(f"\n{Colors.BOLD}{'ID':<5} {'Fecha':<12} {'Cliente':<25} {'Importe':<12} {'Medio':<15} {'Ref':<15}{Colors.ENDC}")
        print("-" * 90)
        
        total = 0.0
        
        for r in rows:
            cliente = r['cliente_nombre'][:24]
            medio = (r['medio'] or 'N/A')[:14]
            ref = (r['referencia'] or '-')[:14]
            
            print(f"{r['id']:<5} {r['fecha']:<12} {cliente:<25} ${r['importe']:>10.2f} {medio:<15} {ref:<15}")
            total += r['importe']
        
        print("-" * 90)
        print(f"{'TOTAL':<42} ${total:>10.2f}")
        print(f"\n{Colors.CYAN}Total: {len(rows)} cobro(s){Colors.ENDC}")
        
        if pause_after:
            pause()
            
    except Exception as e:
        print_error(f"Error al listar cobros: {e}")
        log(f"Error list_cobros: {e}", "ERROR")

# ============================= AJUSTES =============================

def registrar_ajuste():
    """Registra un ajuste (bonificación, recargo, etc.)"""
    print_header("REGISTRAR AJUSTE")
    list_clients(pause_after=False)
    
    try:
        cliente_id = int(input("\nID del cliente (*): ").strip())
        
        if not cliente_exists(cliente_id):
            print_error("Cliente no encontrado")
            return
        
        fecha_str = input("Fecha del ajuste (YYYY-MM-DD) [hoy]: ").strip()
        if not fecha_str:
            fecha = date.today()
        else:
            fecha = parse_date(fecha_str)
        
        descripcion = input("Descripción (*): ").strip()
        if not descripcion:
            print_error("La descripción es obligatoria")
            return
        
        print(f"\n{Colors.CYAN}Tipo de ajuste:{Colors.ENDC}")
        print("1) Bonificación (descuento, negativo)")
        print("2) Recargo (incremento, positivo)")
        print("3) Servicio adicional (positivo)")
        print("4) Nota de crédito (negativo)")
        print("5) Nota de débito (positivo)")
        print("6) Otro")
        
        tipo_opcion = input("Opción: ").strip()
        tipo_map = {
            '1': ('bonificacion', -1),
            '2': ('recargo', 1),
            '3': ('adicional', 1),
            '4': ('nota_credito', -1),
            '5': ('nota_debito', 1),
            '6': ('otro', 0)
        }
        
        if tipo_opcion not in tipo_map:
            print_error("Opción inválida")
            return
        
        tipo, signo = tipo_map[tipo_opcion]
        
        monto_str = input("Monto (positivo): ").strip()
        monto = float(parse_decimal(monto_str))
        
        if monto <= 0:
            print_error("El monto debe ser mayor a cero")
            return
        
        if signo == -1:
            monto = -abs(monto)
        elif signo == 1:
            monto = abs(monto)
        else:
            if not confirm("¿El monto es negativo (bonificación)?"):
                monto = abs(monto)
            else:
                monto = -abs(monto)
        
        ref_str = input("ID de devengamiento relacionado (opcional): ").strip()
        ref_id = int(ref_str) if ref_str else None
        
        if ref_id and not devengamiento_exists(ref_id):
            print_warning("El devengamiento no existe, se guardará sin referencia")
            ref_id = None
        
        con = get_conn()
        cur = con.cursor()
        cur.execute(
            """INSERT INTO ajustes (cliente_id, fecha, descripcion, monto, tipo, referencia_devengamiento_id) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            (cliente_id, fecha.isoformat(), descripcion, monto, tipo, ref_id)
        )
        ajuste_id = cur.lastrowid
        con.commit()
        con.close()
        
        print_success(f"Ajuste registrado con ID: {ajuste_id}")
        log(f"Ajuste registrado: cliente {cliente_id}, monto {monto}, tipo {tipo}", "INFO")
        
    except ValueError as e:
        print_error(str(e))
    except Exception as e:
        print_error(f"Error al registrar ajuste: {e}")
        log(f"Error registrar_ajuste: {e}", "ERROR")

def list_ajustes(cliente_id: Optional[int] = None, pause_after: bool = True):
    """Lista ajustes registrados"""
    try:
        con = get_conn()
        cur = con.cursor()
        
        if cliente_id:
            cur.execute(
                """SELECT a.*, c.nombre as cliente_nombre 
                   FROM ajustes a 
                   JOIN clientes c ON a.cliente_id=c.id 
                   WHERE a.cliente_id=? 
                   ORDER BY a.fecha DESC""",
                (cliente_id,)
            )
        else:
            cur.execute(
                """SELECT a.*, c.nombre as cliente_nombre 
                   FROM ajustes a 
                   JOIN clientes c ON a.cliente_id=c.id 
                   ORDER BY a.fecha DESC"""
            )
        
        rows = cur.fetchall()
        con.close()
        
        if not rows:
            print_warning("No hay ajustes registrados")
            return
        
        print(f"\n{Colors.BOLD}{'ID':<5} {'Fecha':<12} {'Cliente':<25} {'Tipo':<15} {'Monto':<12} {'Descripción'}{Colors.ENDC}")
        print("-" * 90)
        
        for r in rows:
            cliente = r['cliente_nombre'][:24]
            tipo = (r['tipo'] or 'otro')[:14]
            color = Colors.GREEN if r['monto'] < 0 else Colors.WARNING
            
            print(f"{r['id']:<5} {r['fecha']:<12} {cliente:<25} {tipo:<15} {color}${r['monto']:>10.2f}{Colors.ENDC} {r['descripcion'][:30]}")
        
        print(f"\n{Colors.CYAN}Total: {len(rows)} ajuste(s){Colors.ENDC}")
        
        if pause_after:
            pause()
            
    except Exception as e:
        print_error(f"Error al listar ajustes: {e}")
        log(f"Error list_ajustes: {e}", "ERROR")

# ============================= REPORTES =============================

def dashboard():
    """Muestra un resumen general del sistema"""
    print_header("DASHBOARD")
    
    try:
        con = get_conn()
        cur = con.cursor()
        
        # Clientes activos
        cur.execute("SELECT COUNT(*) as cnt FROM clientes WHERE activo=1")
        clientes_activos = cur.fetchone()['cnt']
        
        # Planes activos
        cur.execute("SELECT COUNT(*) as cnt FROM planes WHERE activo=1")
        planes_activos = cur.fetchone()['cnt']
        
        # Total devengado este mes
        hoy = date.today()
        cur.execute(
            "SELECT COALESCE(SUM(importe),0) as total FROM devengamientos WHERE periodo_anyo=? AND periodo_mes=?",
            (hoy.year, hoy.month)
        )
        devengado_mes = cur.fetchone()['total']
        
        # Total cobrado este mes
        primer_dia = date(hoy.year, hoy.month, 1).isoformat()
        cur.execute(
            "SELECT COALESCE(SUM(importe),0) as total FROM cobros WHERE fecha >= ?",
            (primer_dia,)
        )
        cobrado_mes = cur.fetchone()['total']
        
        # Deuda total pendiente
        cur.execute("SELECT * FROM devengamientos")
        todos_devs = cur.fetchall()
        deuda_total = sum(devengamiento_saldo(d['id']) for d in todos_devs)
        
        # Clientes con deuda
        clientes_con_deuda = set()
        for d in todos_devs:
            if devengamiento_saldo(d['id']) > 0.01:
                clientes_con_deuda.add(d['cliente_id'])
        
        # Clientes morosos (>30 días)
        fecha_limite = (date.today() - timedelta(days=30)).isoformat()
        cur.execute(
            """SELECT COUNT(DISTINCT cliente_id) as cnt FROM devengamientos 
               WHERE fecha_devengada <= ?""",
            (fecha_limite,)
        )
        morosos_potenciales = cur.fetchone()['cnt']
        
        con.close()
        
        # Mostrar dashboard
        print(f"{Colors.BOLD}CLIENTES Y PLANES:{Colors.ENDC}")
        print(f"  Clientes activos: {clientes_activos}")
        print(f"  Planes activos: {planes_activos}")
        
        print(f"\n{Colors.BOLD}FACTURACIÓN DEL MES ({hoy.month}/{hoy.year}):{Colors.ENDC}")
        print(f"  Devengado: ${devengado_mes:.2f}")
        print(f"  Cobrado: ${cobrado_mes:.2f}")
        diferencia = devengado_mes - cobrado_mes
        color_dif = Colors.GREEN if diferencia <= 0 else Colors.WARNING
        print(f"  Diferencia: {color_dif}${diferencia:.2f}{Colors.ENDC}")
        
        print(f"\n{Colors.BOLD}ESTADO GENERAL:{Colors.ENDC}")
        print(f"  Deuda total pendiente: ${deuda_total:.2f}")
        print(f"  Clientes con deuda: {len(clientes_con_deuda)}")
        print(f"  Clientes con deuda >30 días: {morosos_potenciales}")
        
        # Gráfico simple de cobranza
        if devengado_mes > 0:
            porcentaje = (cobrado_mes / devengado_mes) * 100
            barras = int(porcentaje / 5)
            print(f"\n{Colors.BOLD}Efectividad de cobranza:{Colors.ENDC}")
            print(f"  [{'█' * barras}{'░' * (20 - barras)}] {porcentaje:.1f}%")
        
        pause()
        
    except Exception as e:
        print_error(f"Error al generar dashboard: {e}")
        log(f"Error dashboard: {e}", "ERROR")

def account_statement():
    """Muestra el estado de cuenta detallado de un cliente"""
    print_header("ESTADO DE CUENTA")
    list_clients(pause_after=False)
    
    try:
        cliente_id = int(input("\nID del cliente: ").strip())
        
        if not cliente_exists(cliente_id):
            print_error("Cliente no encontrado")
            return
        
        con = get_conn()
        cur = con.cursor()
        
        # Obtener nombre del cliente
        cur.execute("SELECT nombre FROM clientes WHERE id=?", (cliente_id,))
        cliente_nombre = cur.fetchone()['nombre']
        
        print_header(f"ESTADO DE CUENTA - {cliente_nombre}")
        
        # Recolectar todos los movimientos
        events = []
        
        # Devengamientos
        cur.execute("SELECT * FROM devengamientos WHERE cliente_id=? ORDER BY fecha_devengada", (cliente_id,))
        devs = cur.fetchall()
        for d in devs:
            events.append({
                'fecha': d['fecha_devengada'],
                'tipo': 'devengamiento',
                'descripcion': f"Devengamiento {d['periodo_anyo']}/{d['periodo_mes']:02d}",
                'debito': float(d['importe']),
                'credito': 0.0
            })
        
        # Ajustes
        cur.execute("SELECT * FROM ajustes WHERE cliente_id=? ORDER BY fecha", (cliente_id,))
        ajustes = cur.fetchall()
        for a in ajustes:
            monto = float(a['monto'])
            events.append({
                'fecha': a['fecha'],
                'tipo': 'ajuste',
                'descripcion': f"{a['tipo']}: {a['descripcion'][:30]}",
                'debito': monto if monto > 0 else 0.0,
                'credito': -monto if monto < 0 else 0.0
            })
        
        # Cobros
        cur.execute("SELECT * FROM cobros WHERE cliente_id=? ORDER BY fecha", (cliente_id,))
        cobros = cur.fetchall()
        for c in cobros:
            events.append({
                'fecha': c['fecha'],
                'tipo': 'cobro',
                'descripcion': f"Cobro - {c['medio'] or 'N/A'}",
                'debito': 0.0,
                'credito': float(c['importe'])
            })
        
        # Ordenar por fecha
        events.sort(key=lambda x: x['fecha'])
        
        # Mostrar movimientos
        print(f"\n{Colors.BOLD}{'Fecha':<12} {'Concepto':<45} {'Débito':<12} {'Crédito':<12} {'Saldo':<12}{Colors.ENDC}")
        print("-" * 95)
        
        saldo = 0.0
        total_debito = 0.0
        total_credito = 0.0
        
        for e in events:
            saldo += e['debito'] - e['credito']
            total_debito += e['debito']
            total_credito += e['credito']
            
            color_saldo = Colors.GREEN if saldo < 0.01 else Colors.FAIL if saldo > 100 else Colors.WARNING
            
            deb_str = f"${e['debito']:>10.2f}" if e['debito'] > 0 else "-"
            cred_str = f"${e['credito']:>10.2f}" if e['credito'] > 0 else "-"
            
            print(f"{e['fecha']:<12} {e['descripcion']:<45} {deb_str:<12} {cred_str:<12} {color_saldo}${saldo:>10.2f}{Colors.ENDC}")
        
        print("-" * 95)
        print(f"{'TOTALES':<57} ${total_debito:>10.2f} ${total_credito:>10.2f} ${saldo:>10.2f}")
        
        # Resumen
        print(f"\n{Colors.BOLD}RESUMEN:{Colors.ENDC}")
        print(f"Total devengado: ${total_debito:.2f}")
        print(f"Total cobrado: ${total_credito:.2f}")
        
        if saldo < 0.01:
            print(f"{Colors.GREEN}Saldo: $0.00 (al día){Colors.ENDC}")
        elif saldo < 100:
            print(f"{Colors.WARNING}Saldo pendiente: ${saldo:.2f}{Colors.ENDC}")
        else:
            print(f"{Colors.FAIL}Saldo pendiente: ${saldo:.2f} ⚠{Colors.ENDC}")
        
        con.close()
        pause()
        
    except ValueError:
        print_error("ID inválido")
    except Exception as e:
        print_error(f"Error al generar estado de cuenta: {e}")
        log(f"Error account_statement: {e}", "ERROR")

def reporte_morosos():
    """Genera reporte de clientes con deuda vencida"""
    print_header("REPORTE DE MOROSOS")
    
    try:
        dias = int(input("Días de atraso mínimo [30]: ").strip() or "30")
        
        fecha_limite = (date.today() - timedelta(days=dias)).isoformat()
        
        con = get_conn()
        cur = con.cursor()
        
        cur.execute(
            """SELECT DISTINCT c.id, c.nombre, c.email, c.telefono
               FROM clientes c
               JOIN devengamientos d ON c.id = d.cliente_id
               WHERE d.fecha_devengada <= ? AND c.activo = 1
               ORDER BY c.nombre""",
            (fecha_limite,)
        )
        
        clientes = cur.fetchall()
        
        if not clientes:
            print_success("¡No hay clientes morosos!")
            con.close()
            pause()
            return
        
        print(f"\n{Colors.BOLD}{'ID':<5} {'Cliente':<30} {'Deuda':<12} {'Contacto':<30}{Colors.ENDC}")
        print("-" * 80)
        
        total_deuda = 0.0
        
        for c in clientes:
            # Calcular deuda del cliente
            cur.execute("SELECT * FROM devengamientos WHERE cliente_id=?", (c['id'],))
            devs = cur.fetchall()
            
            deuda_cliente = 0.0
            for d in devs:
                if d['fecha_devengada'] <= fecha_limite:
                    deuda_cliente += devengamiento_saldo(d['id'])
            
            if deuda_cliente > 0.01:
                contacto = c['email'] or c['telefono'] or '-'
                print(f"{c['id']:<5} {c['nombre'][:29]:<30} {Colors.FAIL}${deuda_cliente:>10.2f}{Colors.ENDC} {contacto[:29]:<30}")
                total_deuda += deuda_cliente
        
        print("-" * 80)
        print(f"{'TOTAL DEUDA VENCIDA':<42} ${total_deuda:>10.2f}")
        
        con.close()
        pause()
        
    except ValueError:
        print_error("Valor inválido")
    except Exception as e:
        print_error(f"Error al generar reporte: {e}")
        log(f"Error reporte_morosos: {e}", "ERROR")

def reporte_cobranzas_mes():
    """Genera reporte de cobranzas del mes"""
    print_header("REPORTE DE COBRANZAS DEL MES")
    
    try:
        mes_str = input("Mes (1-12) [actual]: ").strip()
        anyo_str = input("Año [actual]: ").strip()
        
        hoy = date.today()
        mes = int(mes_str) if mes_str else hoy.month
        anyo = int(anyo_str) if anyo_str else hoy.year
        
        if not (1 <= mes <= 12):
            print_error("Mes inválido")
            return
        
        primer_dia = date(anyo, mes, 1).isoformat()
        
        # Calcular último día del mes
        if mes == 12:
            ultimo_dia = date(anyo, 12, 31).isoformat()
        else:
            ultimo_dia = (date(anyo, mes + 1, 1) - timedelta(days=1)).isoformat()
        
        con = get_conn()
        cur = con.cursor()
        
        cur.execute(
            """SELECT c.*, cl.nombre as cliente_nombre
               FROM cobros c
               JOIN clientes cl ON c.cliente_id = cl.id
               WHERE c.fecha >= ? AND c.fecha <= ?
               ORDER BY c.fecha""",
            (primer_dia, ultimo_dia)
        )
        
        cobros = cur.fetchall()
        
        if not cobros:
            print_warning(f"No hay cobros registrados en {mes:02d}/{anyo}")
            con.close()
            pause()
            return
        
        print(f"\n{Colors.BOLD}{'Fecha':<12} {'Cliente':<30} {'Importe':<12} {'Medio':<15}{Colors.ENDC}")
        print("-" * 72)
        
        total = 0.0
        medios = {}
        
        for c in cobros:
            print(f"{c['fecha']:<12} {c['cliente_nombre'][:29]:<30} ${c['importe']:>10.2f} {(c['medio'] or 'N/A')[:14]:<15}")
            total += c['importe']
            
            medio = c['medio'] or 'sin especificar'
            medios[medio] = medios.get(medio, 0.0) + c['importe']
        
        print("-" * 72)
        print(f"{'TOTAL':<42} ${total:>10.2f}")
        
        print(f"\n{Colors.BOLD}Desglose por medio de pago:{Colors.ENDC}")
        for medio, monto in sorted(medios.items(), key=lambda x: x[1], reverse=True):
            print(f"  {medio}: ${monto:.2f}")
        
        con.close()
        pause()
        
    except ValueError:
        print_error("Valores inválidos")
    except Exception as e:
        print_error(f"Error al generar reporte: {e}")
        log(f"Error reporte_cobranzas_mes: {e}", "ERROR")

def exportar_datos():
    """Exporta datos a CSV"""
    print_header("EXPORTAR DATOS")
    
    print("1) Exportar clientes")
    print("2) Exportar planes")
    print("3) Exportar devengamientos")
    print("4) Exportar cobros")
    print("5) Exportar estado de cuenta de un cliente")
    print("0) Volver")
    
    opt = input("\nOpción: ").strip()
    
    try:
        import csv
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if opt == '1':
            con = get_conn()
            cur = con.cursor()
            cur.execute("SELECT * FROM clientes ORDER BY id")
            rows = cur.fetchall()
            
            filename = f"clientes_{timestamp}.csv"
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                if rows:
                    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                    writer.writeheader()
                    for row in rows:
                        writer.writerow(dict(row))
            
            con.close()
            print_success(f"Exportado a: {filename}")
            
        elif opt == '2':
            con = get_conn()
            cur = con.cursor()
            cur.execute("SELECT p.*, c.nombre as cliente_nombre FROM planes p JOIN clientes c ON p.cliente_id=c.id ORDER BY p.id")
            rows = cur.fetchall()
            
            filename = f"planes_{timestamp}.csv"
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                if rows:
                    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                    writer.writeheader()
                    for row in rows:
                        writer.writerow(dict(row))
            
            con.close()
            print_success(f"Exportado a: {filename}")
            
        elif opt == '3':
            con = get_conn()
            cur = con.cursor()
            cur.execute("SELECT d.*, c.nombre as cliente_nombre FROM devengamientos d JOIN clientes c ON d.cliente_id=c.id ORDER BY d.periodo_anyo DESC, d.periodo_mes DESC")
            rows = cur.fetchall()
            
            filename = f"devengamientos_{timestamp}.csv"
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['ID', 'Cliente', 'Período', 'Importe', 'Fecha', 'Saldo'])
                for row in rows:
                    saldo = devengamiento_saldo(row['id'])
                    writer.writerow([
                        row['id'],
                        row['cliente_nombre'],
                        f"{row['periodo_anyo']}/{row['periodo_mes']:02d}",
                        row['importe'],
                        row['fecha_devengada'],
                        saldo
                    ])
            
            con.close()
            print_success(f"Exportado a: {filename}")
            
        elif opt == '4':
            con = get_conn()
            cur = con.cursor()
            cur.execute("SELECT c.*, cl.nombre as cliente_nombre FROM cobros c JOIN clientes cl ON c.cliente_id=cl.id ORDER BY c.fecha DESC")
            rows = cur.fetchall()
            
            filename = f"cobros_{timestamp}.csv"
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                if rows:
                    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                    writer.writeheader()
                    for row in rows:
                        writer.writerow(dict(row))
            
            con.close()
            print_success(f"Exportado a: {filename}")
            
        elif opt == '5':
            list_clients(pause_after=False)
            cliente_id = int(input("\nID del cliente: ").strip())
            
            if not cliente_exists(cliente_id):
                print_error("Cliente no encontrado")
                return
            
            con = get_conn()
            cur = con.cursor()
            
            cur.execute("SELECT nombre FROM clientes WHERE id=?", (cliente_id,))
            cliente_nombre = cur.fetchone()['nombre']
            
            filename = f"estado_cuenta_{cliente_id}_{timestamp}.csv"
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Fecha', 'Concepto', 'Débito', 'Crédito', 'Saldo'])
                
                events = []
                
                # Devengamientos
                cur.execute("SELECT * FROM devengamientos WHERE cliente_id=? ORDER BY fecha_devengada", (cliente_id,))
                for d in cur.fetchall():
                    events.append({
                        'fecha': d['fecha_devengada'],
                        'descripcion': f"Devengamiento {d['periodo_anyo']}/{d['periodo_mes']:02d}",
                        'debito': float(d['importe']),
                        'credito': 0.0
                    })
                
                # Ajustes
                cur.execute("SELECT * FROM ajustes WHERE cliente_id=? ORDER BY fecha", (cliente_id,))
                for a in cur.fetchall():
                    monto = float(a['monto'])
                    events.append({
                        'fecha': a['fecha'],
                        'descripcion': f"{a['tipo']}: {a['descripcion']}",
                        'debito': monto if monto > 0 else 0.0,
                        'credito': -monto if monto < 0 else 0.0
                    })
                
                # Cobros
                cur.execute("SELECT * FROM cobros WHERE cliente_id=? ORDER BY fecha", (cliente_id,))
                for c in cur.fetchall():
                    events.append({
                        'fecha': c['fecha'],
                        'descripcion': f"Cobro - {c['medio'] or 'N/A'}",
                        'debito': 0.0,
                        'credito': float(c['importe'])
                    })
                
                events.sort(key=lambda x: x['fecha'])
                
                saldo = 0.0
                for e in events:
                    saldo += e['debito'] - e['credito']
                    writer.writerow([
                        e['fecha'],
                        e['descripcion'],
                        e['debito'] if e['debito'] > 0 else '',
                        e['credito'] if e['credito'] > 0 else '',
                        saldo
                    ])
            
            con.close()
            print_success(f"Exportado a: {filename}")
        
        else:
            print_warning("Opción no válida")
        
        pause()
        
    except ImportError:
        print_error("Módulo csv no disponible")
    except ValueError:
        print_error("Valor inválido")
    except Exception as e:
        print_error(f"Error al exportar: {e}")
        log(f"Error exportar_datos: {e}", "ERROR")

# ============================= MENÚS =============================

def menu_clientes():
    """Submenú de gestión de clientes"""
    while True:
        print_header("GESTIÓN DE CLIENTES")
        print("1) Listar clientes")
        print("2) Agregar cliente")
        print("3) Editar cliente")
        print("4) Activar/Desactivar cliente")
        print("0) Volver")
        
        opt = input("\nOpción: ").strip()
        
        if opt == '1':
            list_clients()
        elif opt == '2':
            add_cliente()
        elif opt == '3':
            edit_cliente()
        elif opt == '4':
            toggle_cliente_estado()
        elif opt == '0':
            break
        else:
            print_error("Opción no válida")

def menu_planes():
    """Submenú de gestión de planes"""
    while True:
        print_header("GESTIÓN DE PLANES")
        print("1) Listar todos los planes")
        print("2) Listar planes de un cliente")
        print("3) Agregar plan")
        print("4) Editar plan")
        print("0) Volver")
        
        opt = input("\nOpción: ").strip()
        
        if opt == '1':
            list_plans()
        elif opt == '2':
            list_clients(pause_after=False)
            try:
                cliente_id = int(input("\nID del cliente: ").strip())
                list_plans(cliente_id)
            except ValueError:
                print_error("ID inválido")
        elif opt == '3':
            add_plan()
        elif opt == '4':
            edit_plan()
        elif opt == '0':
            break
        else:
            print_error("Opción no válida")

def menu_devengamientos():
    """Submenú de devengamientos"""
    while True:
        print_header("DEVENGAMIENTOS")
        print("1) Listar todos los devengamientos")
        print("2) Listar devengamientos pendientes")
        print("3) Listar devengamientos de un cliente")
        print("4) Generar devengamientos del mes")
        print("5) Generar devengamientos de período específico")
        print("0) Volver")
        
        opt = input("\nOpción: ").strip()
        
        if opt == '1':
            list_devengamientos()
        elif opt == '2':
            list_devengamientos(only_pending=True)
        elif opt == '3':
            list_clients(pause_after=False)
            try:
                cliente_id = int(input("\nID del cliente: ").strip())
                list_devengamientos(cliente_id)
            except ValueError:
                print_error("ID inválido")
        elif opt == '4':
            generate_devengamientos_for()
            pause()
        elif opt == '5':
            try:
                mes = int(input("Mes (1-12): ").strip())
                anyo = int(input("Año: ").strip())
                generate_devengamientos_for(mes, anyo)
                pause()
            except ValueError:
                print_error("Valores inválidos")
        elif opt == '0':
            break
        else:
            print_error("Opción no válida")

def menu_cobros():
    """Submenú de cobros"""
    while True:
        print_header("COBROS")
        print("1) Registrar cobro")
        print("2) Listar todos los cobros")
        print("3) Listar cobros de un cliente")
        print("0) Volver")
        
        opt = input("\nOpción: ").strip()
        
        if opt == '1':
            record_cobro()
        elif opt == '2':
            list_cobros()
        elif opt == '3':
            list_clients(pause_after=False)
            try:
                cliente_id = int(input("\nID del cliente: ").strip())
                list_cobros(cliente_id)
            except ValueError:
                print_error("ID inválido")
        elif opt == '0':
            break
        else:
            print_error("Opción no válida")

def menu_ajustes():
    """Submenú de ajustes"""
    while True:
        print_header("AJUSTES")
        print("1) Registrar ajuste")
        print("2) Listar todos los ajustes")
        print("3) Listar ajustes de un cliente")
        print("0) Volver")
        
        opt = input("\nOpción: ").strip()
        
        if opt == '1':
            registrar_ajuste()
        elif opt == '2':
            list_ajustes()
        elif opt == '3':
            list_clients(pause_after=False)
            try:
                cliente_id = int(input("\nID del cliente: ").strip())
                list_ajustes(cliente_id)
            except ValueError:
                print_error("ID inválido")
        elif opt == '0':
            break
        else:
            print_error("Opción no válida")

def menu_reportes():
    """Submenú de reportes"""
    while True:
        print_header("REPORTES")
        print("1) Dashboard general")
        print("2) Estado de cuenta de cliente")
        print("3) Reporte de morosos")
        print("4) Cobranzas del mes")
        print("5) Exportar datos a CSV")
        print("0) Volver")
        
        opt = input("\nOpción: ").strip()
        
        if opt == '1':
            dashboard()
        elif opt == '2':
            account_statement()
        elif opt == '3':
            reporte_morosos()
        elif opt == '4':
            reporte_cobranzas_mes()
        elif opt == '5':
            exportar_datos()
        elif opt == '0':
            break
        else:
            print_error("Opción no válida")

# ============================= MAIN =============================

def show_main_menu():
    """Muestra el menú principal"""
    print_header("SISTEMA DE GESTIÓN DE ABONOS")
    print(f"{Colors.CYAN}1) Gestión de Clientes{Colors.ENDC}")
    print(f"{Colors.CYAN}2) Gestión de Planes{Colors.ENDC}")
    print(f"{Colors.CYAN}3) Devengamientos{Colors.ENDC}")
    print(f"{Colors.CYAN}4) Cobros{Colors.ENDC}")
    print(f"{Colors.CYAN}5) Ajustes{Colors.ENDC}")
    print(f"{Colors.CYAN}6) Reportes{Colors.ENDC}")
    print()
    print(f"{Colors.WARNING}7) Crear backup manual{Colors.ENDC}")
    print(f"{Colors.FAIL}0) Salir{Colors.ENDC}")

def main_loop():
    """Bucle principal del programa"""
    print(f"{Colors.BOLD}{Colors.CYAN}")
    print("=" * 70)
    print("  SISTEMA DE GESTIÓN DE ABONOS - Versión Robusta")
    print("=" * 70)
    print(f"{Colors.ENDC}\n")
    
    init_db()
    auto_backup()
    
    log("Sistema iniciado", "INFO")
    
    while True:
        try:
            show_main_menu()
            opt = input(f"\n{Colors.BOLD}Opción: {Colors.ENDC}").strip()
            
            if opt == '1':
                menu_clientes()
            elif opt == '2':
                menu_planes()
            elif opt == '3':
                menu_devengamientos()
            elif opt == '4':
                menu_cobros()
            elif opt == '5':
                menu_ajustes()
            elif opt == '6':
                menu_reportes()
            elif opt == '7':
                backup_database()
                pause()
            elif opt == '0':
                if confirm("¿Confirma salir del sistema?"):
                    print(f"\n{Colors.GREEN}Gracias por usar el sistema. ¡Hasta pronto!{Colors.ENDC}\n")
                    log("Sistema cerrado", "INFO")
                    break
            else:
                print_error("Opción no válida")
                pause()
                
        except KeyboardInterrupt:
            print(f"\n\n{Colors.WARNING}Interrupción detectada{Colors.ENDC}")
            if confirm("¿Desea salir?"):
                log("Sistema interrumpido por usuario", "INFO")
                break
        except Exception as e:
            print_error(f"Error inesperado: {e}")
            log(f"Error inesperado en main_loop: {e}", "ERROR")
            pause()

# ============================= ENTRY POINT =============================

if __name__ == '__main__':
    try:
        main_loop()
    except KeyboardInterrupt:
        print(f"\n{Colors.WARNING}Saliendo...{Colors.ENDC}\n")
        log("Sistema cerrado abruptamente", "WARNING")
        sys.exit(0)
    except Exception as e:
        print_error(f"Error crítico: {e}")
        log(f"Error crítico: {e}", "CRITICAL")
        sys.exit(1)
