import json
import os
import threading
import time
from contextlib import asynccontextmanager

import pika
from fastapi import FastAPI
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

# Adresses : noms des services dans Docker, localhost en local
RABBIT_HOST = os.environ.get("RABBIT_HOST", "localhost")
engine = create_engine(
    os.environ.get("DATABASE_URL", "sqlite:///livraison.db")
)


class Base(DeclarativeBase):
    pass


class BonLivraison(Base):
    __tablename__ = "bons"
    event_id: Mapped[str] = mapped_column(primary_key=True)
    commande_id: Mapped[int]
    sku: Mapped[str]
    quantite: Mapped[int]


Base.metadata.create_all(engine)


# Traiter un message : event_id en clé primaire = pas de doublon (idempotence)
def traiter(corps: bytes):
    evenement = json.loads(corps)
    with Session(engine) as db:
        db.add(BonLivraison(
            event_id=evenement["event_id"],
            commande_id=evenement["commande_id"],
            sku=evenement["sku"],
            quantite=evenement["quantite"],
        ))
        try:
            db.commit()
            print("Bon créé pour la commande", evenement["commande_id"])
        except IntegrityError:
            db.rollback()
            print("Message déjà traité, on l'ignore")


# Écouter la file RabbitMQ (réessaie si RabbitMQ n'est pas encore prêt)
def ecouter():
    while True:
        try:
            connexion = pika.BlockingConnection(
                pika.ConnectionParameters(RABBIT_HOST)
            )
            canal = connexion.channel()
            canal.exchange_declare(
                exchange="distrilog.events",
                exchange_type="topic",
                durable=True
            )
            canal.queue_declare(queue="livraison.commandes", durable=True)
            canal.queue_bind(
                queue="livraison.commandes",
                exchange="distrilog.events",
                routing_key="commande.passee",
            )

            def au_message(ch, method, properties, body):
                traiter(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)

            canal.basic_consume(
                queue="livraison.commandes",
                on_message_callback=au_message
            )
            print("Livraison écoute la file livraison.commandes")
            canal.start_consuming()
        except Exception as erreur:
            print("RabbitMQ indisponible, nouvel essai dans 3 s :", erreur)
            time.sleep(3)


@asynccontextmanager
async def demarrage(app):
    threading.Thread(target=ecouter, daemon=True).start()
    yield


app = FastAPI(title="livraison", lifespan=demarrage)


@app.get("/livraisons")
def lister_livraisons():
    with Session(engine) as db:
        return [
            {"commande_id": b.commande_id, "sku": b.sku, "quantite": b.quantite}
            for b in db.scalars(select(BonLivraison))
        ]
