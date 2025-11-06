# abonos-LS
Gestión de abonos, cuentas corrientes, cobranzas y clientes

# Sistema de Gestión de Abonos — LS

Este proyecto contiene dos versiones del mismo sistema de gestión de abonos y cobranzas para clientes, desarrollado en Python con base de datos SQLite.

## 📦 Contenido

- **`abonos-1.py`** → Versión de consola, más completa y robusta, con interacción por terminal.
- **`app.py`** → Versión web basada en Streamlit, con interfaz gráfica moderna.

---

## 🧠 Funcionalidades principales

- Registro de clientes y planes de abono
- Generación de devengamientos mensuales
- Registro de cobros con imputación automática o manual
- Aplicación de ajustes (bonificaciones, recargos, notas de crédito/débito)
- Cálculo de saldos y reportes básicos
- Backups automáticos de la base de datos

---

## ⚙️ Requisitos

- Python 3.8 o superior  
- Librerías: `streamlit`, `pandas`, `decimal`, `sqlite3`, `colorama` (solo para versión consola)

Instalación rápida:

```bash
pip install -r requirements.txt
```

## ▶️ Uso
1. Modo consola
python abonos-1.py

Ideal para testeo y posterior desarrollo.

2. Modo web
streamlit run app.py

Abre el panel visual en tu navegador (http://localhost:8501)
