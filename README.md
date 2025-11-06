# Sistema básico de Gestión de Abonos — LS
Gestión de abonos, cuentas corrientes, cobranzas y clientes
Este proyecto contiene dos versiones del mismo sistema de gestión de abonos y cobranzas para clientes, desarrollado en Python con base de datos SQLite.

## 📦 Contenido

- **`abonos-1.py`** → Versión de consola, con interacción por terminal.
- **`app.py`** → Versión web basada en Streamlit, con una pequeña interfaz gráfica. Aún en desarrollo. Podés editarla a tu gusto y placer.

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


---

### 🧭 Guía de uso básico

El sistema está diseñado para gestionar de forma ordenada el ciclo completo de abonos y cobranzas.  
A continuación se describe el flujo recomendado paso a paso:

1. **📇 Crear un cliente**  
   - Desde la versión de consola o la interfaz web, registrá un nuevo cliente.  
   - Los clientes se crean **activos por defecto**, lo que significa que participarán en los devengamientos mensuales.  
   - Si un cliente deja de tener abonos vigentes, podés **desactivarlo** para excluirlo de futuros procesos.

2. **🧾 Crear un abono (plan o servicio)**  
   - Configurá los datos del abono (nombre, importe, periodicidad, etc.).  
   - Podés asignar el mismo abono a uno o varios clientes, según corresponda.

3. **🌀 Realizar los devengamientos**  
   - Este proceso genera los cargos automáticos (mensuales, por ejemplo) para todos los **clientes activos** con abonos vigentes.  
   - Cada devengamiento se registra con fecha, importe y referencia al cliente.  
   - Es la base para conocer los importes pendientes de cobro.

4. **💰 Registrar pagos**  
   - Cuando un cliente abona, registrá el **pago** indicando el monto, fecha y forma de pago.  
   - Los pagos no se aplican automáticamente: quedan disponibles para imputar.

5. **🔗 Imputar pagos**  
   - En esta etapa, vinculás los pagos registrados con los devengamientos pendientes del cliente.  
   - Esto permite llevar un control preciso del saldo de cada cliente.

6. **📊 Consultar reportes**  
   - Con los pagos y devengamientos actualizados, podés generar reportes que muestren:  
     - Abonos activos  
     - Clientes con saldo a favor o pendiente  
     - Historial de cobranzas y devengamientos


