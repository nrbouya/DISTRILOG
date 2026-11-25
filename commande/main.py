import json
import os
import uuid

import httpx
import pika
import pybreaker
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

CATALOGUE_URL = os.environ.get("CATALOGUE_URL", "http://localhost:8001")
STOCK_URL = os.environ.get("STOCK_URL", "http://localhost:8002")
RABBIT_HOST = os.environ.get("RABBIT_HOST", "localhost")

engine = create_engine(os.environ.get("DATABASE_URL", "sqlite:///commande.db"))

class Base(DeclarativeBase):
    pass

class Commande(Base):
    __tablename__ = "commandes"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sku: Mapped[str]
    quantite: Mapped[int]
    total: Mapped[float]
    statut: Mapped[str] = mapped_column(default="creee")  # AJOUT

Base.metadata.create_all(engine)


app = FastAPI(title="commande")


class CommandeIn(BaseModel):
    sku: str
    quantite: int

class StatutIn(BaseModel):  # AJOUT
    statut: str

def en_dict(c: Commande) -> dict:
    return {"id": c.id, "sku": c.sku, "quantite": c.quantite, "total": c.total, "statut": c.statut}

# Disjoncteur : après 3 échecs, on arrête d'appeler Stock pendant 10 secondes
disjoncteur_stock = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=10)

@retry(
    retry=retry_if_exception_type(httpx.TransportError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.2),
    reraise=True,
)
def appeler(methode: str, url: str, **kwargs):
    """Un appel HTTP avec timeout (2 s) et 3 essais."""
    return httpx.request(methode, url, timeout=2.0, **kwargs)

def reserver_stock(sku: str, quantite: int):
    return disjoncteur_stock.call(
        appeler,
        "POST",
        f"{STOCK_URL}/stock/{sku}/reserve",
        json={"quantite": quantite},
    )

def publier_evenement(evenement: dict):
    try:
        connexion = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
        canal = connexion.channel()
        canal.exchange_declare(
            exchange="distrilog.events", exchange_type="topic", durable=True
        )
        canal.basic_publish(
            exchange="distrilog.events",
            routing_key="commande.passee",
            body=json.dumps(evenement),
        )
        connexion.close()
    except Exception as erreur:
        print("Événement non publié :", erreur)

@app.post("/commandes", status_code=201)
def passer_commande(c: CommandeIn):
    # 1. Le produit existe-t-il ? (on demande au Catalogue)
    try:
        rep = appeler("GET", f"{CATALOGUE_URL}/produits/{c.sku}")
    except httpx.TransportError:
        raise HTTPException(
            status_code=503,
            detail="Catalogue indisponible, réessayez plus tard",
        )
    if rep.status_code == 404:
        raise HTTPException(status_code=404, detail="Produit inconnu")
    produit = rep.json()
    
    try:
        rep = reserver_stock(c.sku, c.quantite)
    except (httpx.TransportError, pybreaker.CircuitBreakerError):
        # Mode dégradé : on répond proprement au lieu de tout bloquer
        raise HTTPException(
            status_code=503, detail="Stock indisponible, commande non prise"
        )
    if rep.status_code == 409:
        raise HTTPException(status_code=409, detail="Stock insuffisant")

    # 3. On enregistre la commande dans NOTRE base
    with Session(engine) as db:
        cmd = Commande(
            sku=c.sku, quantite=c.quantite, total=produit["prix"] * c.quantite
        )
        db.add(cmd)
        db.commit()
        db.refresh(cmd)
        resultat = en_dict(cmd)


    publier_evenement(
        {
            "event_id": str(uuid.uuid4()),
            "commande_id": cmd.id,
            "sku": c.sku,
            "quantite": c.quantite,
        }
    )
    return resultat

@app.get("/commandes")
def lister_commandes():
    with Session(engine) as db:
        return [en_dict(c) for c in db.scalars(select(Commande))]

@app.put("/commandes/{commande_id}")
def modifier_statut(commande_id: int, s: StatutIn):
    with Session(engine) as db:
        cmd = db.get(Commande, commande_id)
        if cmd is None:
            raise HTTPException(status_code=404, detail="Commande introuvable")
        cmd.statut = s.statut
        db.commit()
        return en_dict(cmd)