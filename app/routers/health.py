from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
def health():
    return {"status": "ok", "service": "planificador-integra"}


@router.get("/")
def root():
    return {"message": "Planificador Fundación Integra v1.0"}