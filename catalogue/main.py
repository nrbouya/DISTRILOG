import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

# Base de données : PostgreSQL dans Docker, SQLite en local
engine = create_engine(
    os.environ.get("DATABASE_URL", "sqlite:///catalogue.db")
)


class Base(DeclarativeBase):
    pass


class Produit(Base):
    __tablename__ = "produits"
    sku: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    prix: Mapped[float]


Base.metadata.create_all(engine)

app = FastAPI(title="catalogue")


class ProduitIn(BaseModel):
    sku: str
    nom: str
    prix: float


def en_dict(p: Produit) -> dict:
    return {"sku": p.sku, "nom": p.nom, "prix": p.prix}


@app.post("/produits", status_code=201)
def creer_produit(p: ProduitIn):
    with Session(engine) as db:
        db.add(Produit(**p.model_dump()))
        try:
            db.commit()
        except IntegrityError:
            raise HTTPException(status_code=409, detail="Ce SKU existe déjà")
    return p


@app.get("/produits")
def lister_produits():
    with Session(engine) as db:
        return [en_dict(p) for p in db.scalars(select(Produit))]


@app.get("/produits/{sku}")
def lire_produit(sku: str):
    with Session(engine) as db:
        p = db.get(Produit, sku)
        if p is None:
            raise HTTPException(status_code=404, detail="Produit introuvable")
        return en_dict(p)
