import json
import os
import threading
import time
from contextlib import asynccontextmanager

import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

RABBIT_HOST = os.environ.get("RABBIT_HOST", "localhost")
engine = create_engine(os.environ.get("DATABASE_URL", "sqlite:///livraison.db"))

class Base(DeclarativeBase):
    pass

class BonLivraison(Base):
    __tablename__ = "bons"
    event_id: Mapped[str] = mapped_column(primary_key=True)  # clé unique = pas de doublon
    commande_id: Mapped[int]
    sku: Mapped[str]
    quantite: Mapped[int]
    statut: Mapped[str] = mapped_column(default="en_preparation")  # AJOUT

Base.metadata.create_all(engine)


def traiter(corps: bytes):
    evenement = json.loads(corps)
    with Session(engine) as db:
        db.add(
            BonLivraison(
                event_id=evenement["event_id"],
                commande_id=evenement["commande_id"],
                sku=evenement["sku"],
                quantite=evenement["quantite"],
            )
        )
        try:
            db.commit()
            print("Bon créé pour la commande", evenement["commande_id"], flush=True)
        except IntegrityError:
            print("Message déjà traité, on l'ignore", flush=True)

def au_message(ch, method, properties, body):
    traiter(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def ecouter():
    # MODIFIÉ : si RabbitMQ n'est pas prêt (ou tombe), on réessaie toutes les 3 s
    # au lieu d'abandonner. C'était la cause du message perdu au démarrage.
    while True:
        try:
            connexion = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
            canal = connexion.channel()
            canal.exchange_declare(
                exchange="distrilog.events", exchange_type="topic", durable=True
            )
            canal.queue_declare(queue="livraison.commandes", durable=True)
            canal.queue_bind(
                queue="livraison.commandes",
                exchange="distrilog.events",
                routing_key="commande.passee",
            )
            canal.basic_consume(queue="livraison.commandes", on_message_callback=au_message)
            print("Livraison : connectée à RabbitMQ, en écoute", flush=True)
            canal.start_consuming()
        except Exception as erreur:
            print("Livraison : RabbitMQ indisponible, nouvel essai dans 3 s :", type(erreur).__name__, flush=True)
            time.sleep(3)

@asynccontextmanager
async def demarrage(app):
    threading.Thread(target=ecouter, daemon=True).start()
    yield


app = FastAPI(title="livraison", lifespan=demarrage)


class StatutIn(BaseModel):  # AJOUT
    statut: str

def en_dict(b: BonLivraison) -> dict:
    return {"commande_id": b.commande_id, "sku": b.sku, "quantite": b.quantite, "statut": b.statut}

@app.get("/livraisons")
def lister_livraisons():
    with Session(engine) as db:
        return [en_dict(b) for b in db.scalars(select(BonLivraison))]

@app.put("/livraisons/{commande_id}")
def modifier_statut(commande_id: int, s: StatutIn):
    with Session(engine) as db:
        bon = db.scalars(
            select(BonLivraison).where(BonLivraison.commande_id == commande_id)
        ).first()
        if bon is None:
            raise HTTPException(status_code=404, detail="Livraison introuvable")
        bon.statut = s.statut
        db.commit()
        return en_dict(bon)
