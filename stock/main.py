
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

import os
# 1. Connexion à la base de données
#engine = create_engine("sqlite:///stock.db")
engine = create_engine(os.environ.get("DATABASE_URL", "sqlite:///stock.db"))

# 2. Classe de base
class Base(DeclarativeBase):
    pass


# 3. Création de la table Stock
class LigneStock(Base):
    __tablename__ = "stock"

    sku: Mapped[str] = mapped_column(primary_key=True)
    quantite: Mapped[int]


# 4. Création automatique de la table
Base.metadata.create_all(engine)


# 5. Création de l'application FastAPI
app = FastAPI(title="stock")


# 6. Structure de la quantité reçue
class Quantite(BaseModel):
    quantite: int


# 7. Définir le stock d'un produit
@app.put("/stock/{sku}")
def definir_stock(sku: str, q: Quantite):

    with Session(engine) as db:

        ligne = db.get(LigneStock, sku)

        if ligne is None:
            db.add(LigneStock(sku=sku, quantite=q.quantite))
        else:
            ligne.quantite = q.quantite

        db.commit()

    return {"sku": sku, "quantite": q.quantite}

#liberer un stock
@app.post("/stock/{sku}/liberer")
def liberer_stock(sku: str, q: Quantite):

    with Session(engine) as db:
        resultat = db.execute(
            update(LigneStock)
            .where(LigneStock.sku == sku)
            .values(quantite=LigneStock.quantite + q.quantite)
        )
        db.commit()
        if resultat.rowcount == 0:
            raise HTTPException(
                status_code=404,
                detail="SKU inconnu"
            )
    return {"sku": sku, "libere": q.quantite}
# 8. Lire le stock d'un produit
@app.get("/stock/{sku}")
def lire_stock(sku: str):

    with Session(engine) as db:

        ligne = db.get(LigneStock, sku)

        if ligne is None:
            raise HTTPException(
                status_code=404,
                detail="SKU inconnu"
            )

        return {"sku": sku, "quantite": ligne.quantite}


# 9. Réserver une quantité
@app.post("/stock/{sku}/reserve")
def reserver(sku: str, q: Quantite):

    with Session(engine) as db:

        resultat = db.execute(
            update(LigneStock)
            .where(
                LigneStock.sku == sku,
                LigneStock.quantite >= q.quantite
            )
            .values(
                quantite=LigneStock.quantite - q.quantite
            )
        )

        db.commit()

        if resultat.rowcount == 0:
            raise HTTPException(
                status_code=409,
                detail="Stock insuffisant"
            )

    return {"sku": sku, "reserve": q.quantite}
