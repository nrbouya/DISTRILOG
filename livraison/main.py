from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

engine = create_engine("sqlite:///livraison.db")

class Base(DeclarativeBase):
    pass

class Livraison(Base):
    __tablename__ = "livraisons"
    id: Mapped[int] = mapped_column(primary_key=True)
    commande_id: Mapped[int]
    adresse: Mapped[str]
    statut: Mapped[str] = mapped_column(default="en_preparation")

Base.metadata.create_all(engine)


app = FastAPI(title="livraison")


class LivraisonIn(BaseModel):
    commande_id: int
    adresse: str

class StatutIn(BaseModel):
    statut: str

def en_dict(l: Livraison) -> dict:
    return {"id": l.id, "commande_id": l.commande_id, "adresse": l.adresse, "statut": l.statut}

@app.post("/livraisons", status_code=201)
def creer_livraison(l: LivraisonIn):
    with Session(engine) as db:
        livraison = Livraison(commande_id=l.commande_id, adresse=l.adresse)
        db.add(livraison)
        db.commit()
        return en_dict(livraison)

@app.get("/livraisons")
def lister_livraisons():
    with Session(engine) as db:
        return [en_dict(l) for l in db.scalars(select(Livraison))]

@app.put("/livraisons/{id}")
def modifier_statut(id: int, s: StatutIn):
    with Session(engine) as db:
        livraison = db.get(Livraison, id)
        if livraison is None:
            raise HTTPException(status_code=404, detail="Livraison introuvable")
        livraison.statut = s.statut
        db.commit()
        return en_dict(livraison)