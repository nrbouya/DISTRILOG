import json
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager

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
JETON = os.environ.get("JETON", "secret-distrilog")  # AJOUT AUTHENTIFICATION

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

# AJOUT OUTBOX : la "boîte d'envoi". Chaque message attend ici jusqu'à son envoi.
class Outbox(Base):
    __tablename__ = "outbox"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(unique=True)
    contenu: Mapped[str]                                # le message, en JSON
    envoye: Mapped[bool] = mapped_column(default=False) # False = pas encore publié

Base.metadata.create_all(engine)


# AJOUT OUTBOX : le "facteur". Toutes les 2 s, il publie les messages en attente.
def facteur():
    while True:
        try:
            with Session(engine) as db:
                en_attente = db.scalars(
                    select(Outbox).where(Outbox.envoye == False).order_by(Outbox.id)
                ).all()
                for message in en_attente:
                    publier_evenement(json.loads(message.contenu))  # erreur si RabbitMQ est en panne
                    message.envoye = True
                    db.commit()
                    print("Facteur : message envoyé, event_id", message.event_id, flush=True)
        except Exception as erreur:
            print("Facteur : RabbitMQ indisponible, nouvel essai dans 2 s :", type(erreur).__name__, flush=True)
        time.sleep(2)

@asynccontextmanager
async def demarrage(app):
    threading.Thread(target=facteur, daemon=True).start()
    yield


app = FastAPI(title="commande", lifespan=demarrage)


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
        headers={"x-jeton": JETON},  # AJOUT AUTHENTIFICATION : on présente le jeton
    )

def publier_evenement(evenement: dict):
    # MODIFIÉ OUTBOX : plus de try/except ici. Si RabbitMQ est en panne,
    # l'erreur remonte au facteur, qui garde le message et réessaie.
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
    if rep.status_code == 401:  # AJOUT AUTHENTIFICATION
        raise HTTPException(status_code=502, detail="Le Stock a refusé notre jeton")

    # 3. MODIFIÉ OUTBOX : la commande ET son message, dans le même commit
    with Session(engine) as db:
        cmd = Commande(
            sku=c.sku, quantite=c.quantite, total=produit["prix"] * c.quantite
        )
        db.add(cmd)
        db.flush()  # donne un numéro (id) à la commande avant le commit
        evenement = {
            "event_id": str(uuid.uuid4()),
            "commande_id": cmd.id,
            "sku": c.sku,
            "quantite": c.quantite,
        }
        db.add(Outbox(event_id=evenement["event_id"], contenu=json.dumps(evenement)))
        db.commit()  # les deux sont enregistrés ensemble, ou aucun des deux
        db.refresh(cmd)
        resultat = en_dict(cmd)

    # 4. Plus d'envoi direct : le facteur publiera le message dans 2 s au plus tard
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

# AJOUT OUTBOX : voir les messages de la boîte d'envoi (pour la démo)
@app.get("/outbox")
def lister_outbox():
    with Session(engine) as db:
        return [
            {"id": m.id, "event_id": m.event_id, "contenu": json.loads(m.contenu), "envoye": m.envoye}
            for m in db.scalars(select(Outbox).order_by(Outbox.id))
        ]
