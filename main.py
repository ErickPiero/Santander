# main.py - Microservicio de Suscripciones de Alta Concurrencia (Santander Consumer Perú)
# pyrefly: ignore [missing-import]
from fastapi import FastAPI, HTTPException, status, Depends
from pydantic import BaseModel, Field
from typing import List
from datetime import date
# pyrefly: ignore [missing-import]
import asyncpg
import re

app = FastAPI(
    title="Santander Consumer - Microservicio de Suscripciones (PoC)",
    description="Servicio asíncrono para la gestión de afiliaciones de recibos públicos con enmascaramiento SBS",
    version="2.0.0",
)

# Configuración de conexión asíncrona a PostgreSQL (Estándar Técnico)
DB_DSN = "postgresql://postgres:secure_password_sbs@localhost:5432/santander_db"


async def get_db_pool():
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=5, max_size=20)
    try:
        yield pool
    finally:
        await pool.close()


# --- MODELOS DE ENTRADA Y SALIDA (Pydantic con Validación) ---
class SuscripcionCreate(BaseModel):
    usuario_id: int = Field(
        ..., example=1024, description="ID único del cliente de Santander"
    )
    servicio_id: int = Field(
        ..., example=1, description="ID del proveedor (1=Luz del Sur, 2=Sedapal, etc.)"
    )
    codigo_suministro: str = Field(
        ..., min_length=4, max_length=20, example="9876543-2"
    )
    saldo_pendiente: float = Field(default=0.0, ge=0.0)
    fecha_vencimiento: date = Field(
        ..., example="2026-07-20"
    )


class SuscripcionResponse(BaseModel):
    id: int
    usuario_id: int
    servicio_id: int
    codigo_suministro_enmascarado: str  # Cumplimiento Ley N° 29733 (SBS)
    saldo_pendiente: float
    fecha_vencimiento: str
    estado: str


# --- FUNCION AUXILIAR DE ENMASCARAMIENTO (Cumplimiento de Privacidad) ---
def enmascarar_suministro(codigo: str) -> str:
    """
    Enmascara el código de suministro para evitar exposición de datos sensibles.
    Ejemplo: '9876543-2' -> '987****-2'
    """
    if len(codigo) <= 4:
        return "****"
    return codigo[:3] + "*" * (len(codigo) - 5) + codigo[-2:]


# --- ENDPOINTS API RESTFUL ---


@app.post(
    "/api/v2/suscripciones",
    status_code=status.HTTP_201_CREATED,
    response_model=dict,
    summary="Registrar una nueva afiliación de servicio",
)
async def crear_suscripcion(suscripcion: SuscripcionCreate, pool=Depends(get_db_pool)):
    """
    Registra un recibo de servicios públicos en la cuenta del cliente para su posterior
    análisis por el asistente de Inteligencia Artificial y pago programado.
    """
    async with pool.acquire() as conn:
        try:
            query = """
                INSERT INTO suscripciones (usuario_id, servicio_id, codigo_suministro, saldo_pendiente, fecha_vencimiento, estado)
                VALUES ($1, $2, $3, $4, $5, 'Activo') RETURNING id;
            """
            suscripcion_id = await conn.fetchval(
                query,
                suscripcion.usuario_id,
                suscripcion.servicio_id,
                suscripcion.codigo_suministro,
                suscripcion.saldo_pendiente,
                suscripcion.fecha_vencimiento,
            )
            return {
                "status": "success",
                "suscripcion_id": suscripcion_id,
                "message": "Suscripción registrada exitosamente y encolada para análisis cognitivo.",
            }
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error transaccional en la persistencia de datos: {str(e)}",
            )


@app.get(
    "/api/v2/suscripciones/usuario/{usuario_id}",
    response_model=List[SuscripcionResponse],
    summary="Obtener suscripciones activas del cliente",
)
async def obtener_suscripciones(usuario_id: int, pool=Depends(get_db_pool)):
    """
    Retorna la lista de suscripciones asociadas a un usuario.
    Los datos sensibles son enmascarados antes de enviarse al canal móvil (App Santander).
    """
    async with pool.acquire() as conn:
        query = """
            SELECT id, usuario_id, servicio_id, codigo_suministro, saldo_pendiente, 
                   TO_CHAR(fecha_vencimiento, 'YYYY-MM-DD') as fecha_venc_str, estado 
            FROM suscripciones 
            WHERE usuario_id = $1 AND estado = 'Activo';
        """
        rows = await conn.fetch(query, usuario_id)

        if not rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No se encontraron suscripciones activas para el usuario {usuario_id}.",
            )

        # Mapeo y enmascaramiento al vuelo para cumplir con la auditoría SBS
        response_data = []
        for r in rows:
            response_data.append(
                SuscripcionResponse(
                    id=r["id"],
                    usuario_id=r["usuario_id"],
                    servicio_id=r["servicio_id"],
                    codigo_suministro_enmascarado=enmascarar_suministro(
                        r["codigo_suministro"]
                    ),
                    saldo_pendiente=float(r["saldo_pendiente"]),
                    fecha_vencimiento=r["fecha_venc_str"],
                    estado=r["estado"],
                )
            )
        return response_data
