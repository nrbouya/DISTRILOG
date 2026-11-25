import json
import pika
import uuid
import httpx
import pybreaker

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
import os

#RABBITMQ_HOST = "localhost"
#CATALOGUE_URL= "http://localhost:8001"
#STOCK_URL = "http://localhost:8002"

RABBIT_HOST = os.environ.get("RABBIT_HOST", "localhost")


CATALOGUE_URL = os.environ.get("CATALOGUE_URL" , "http://localhost:8001")
STOCK_URL = os.environ.get("STOCK_URL", "http://localhost:8002")

                             
# Disjoncteur du service Stock
disjoncteur_stock = pybreaker.CircuitBreaker(
    fail_max=3,
    reset_timeout=10,
)


#engine = create_engine("sqlite:///commande.db")
engine = create_engine(os.environ.get("DATABASE_URL", "sqlite:///commande.db"))


# Fonction HTTP avec Retry
@retry(
    retry=retry_if_exception_type(httpx.RequestError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.2),
    reraise=True,
)
def appeler(methode: str, url: str, **kwargs):
    return httpx.request(
        methode,
        url,
        timeout=2.0,
        **kwargs
    )


# Réserver le stock avec Circuit Breaker et Retry
def reserver_stock(sku: str, quantite: int):
    return disjoncteur_stock.call(
        appeler,
        "POST",
        f"{STOCK_URL}/stock/{sku}/reserve",
        json={"quantite": quantite}
    )


# Compensation : rendre le stock si la commande n'a pas pu être enregistrée
def liberer_stock(sku: str, quantite: int):
    try:
        rep = appeler(
            "POST",
            f"{STOCK_URL}/stock/{sku}/liberer",
            json={"quantite": quantite}
        )
        print(f"Compensation {sku} x{quantite} : code {rep.status_code}")
    except httpx.RequestError as e:
        print(f"Compensation impossible : {quantite} x {sku} non rendus ({e})")


# Base de données
class Base(DeclarativeBase):
    pass


class Commande(Base):
    __tablename__ = "commandes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sku: Mapped[str]
    quantite: Mapped[int]
    total: Mapped[float]


Base.metadata.create_all(engine)


app = FastAPI(title="commande")


class CommandeIn(BaseModel):
    sku: str
    quantite: int


# Publier un événement dans RabbitMQ
def publier_evenement(evenement: dict):
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

        canal.basic_publish(
            exchange="distrilog.events",
            routing_key="commande.passee",
            body=json.dumps(evenement)
        )
        

        connexion.close()

    except Exception as e:
        print(f"Erreur lors de la publication de l'événement : {e!r}")


@app.post("/commandes", status_code=201)
def passer_commande(c: CommandeIn):

    # 1. Vérifier le produit dans le Catalogue
    try:
        rep = appeler(
            "GET",
            f"{CATALOGUE_URL}/produits/{c.sku}"
        )

    except httpx.RequestError:
        raise HTTPException(
            status_code=503,
            detail="Catalogue indisponible, réessayez plus tard"
        )

    if rep.status_code == 404:
        raise HTTPException(
            status_code=404,
            detail="Produit inconnu"
        )

    rep.raise_for_status()
    produit = rep.json()

    # 2. Réserver le stock
    try:
        rep = reserver_stock(c.sku, c.quantite)

    except (httpx.RequestError, pybreaker.CircuitBreakerError):
        raise HTTPException(
            status_code=503,
            detail="Stock indisponible, commande non prise"
        )

    if rep.status_code == 409:
        raise HTTPException(
            status_code=409,
            detail="Stock insuffisant"
        )

    rep.raise_for_status()

    # 3. Enregistrer la commande (si ça plante : compensation, on rend le stock)
    try:
        with Session(engine) as db:
            cmd = Commande(
                sku=c.sku,
                quantite=c.quantite,
                total=produit["prix"] * c.quantite
            )
            db.add(cmd)
            db.commit()
            db.refresh(cmd)

            resultat = {
                "id": cmd.id,
                "sku": cmd.sku,
                "quantite": cmd.quantite,
                "total": cmd.total
            }

    except Exception as e:
        print(f"Enregistrement impossible : {e}")
        liberer_stock(c.sku, c.quantite)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de l'enregistrement de la commande"
        )

    # 4. Publier l'événement (seulement si la commande est enregistrée)
    publier_evenement({
        "event_id": str(uuid.uuid4()),
        "commande_id": resultat["id"],
        "sku": resultat["sku"],
        "quantite": resultat["quantite"]
    })

    return resultat


@app.get("/commandes")
def lister_commandes():

    with Session(engine) as db:
        return [
            {
                "id": c.id,
                "sku": c.sku,
                "quantite": c.quantite,
                "total": c.total
            }
            for c in db.scalars(select(Commande))
        ]
