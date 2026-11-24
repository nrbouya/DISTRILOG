from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

engine = create_engine("sqlite:///stock.db")

class Base(DeclarativeBase):
    pass

class Stock(Base):
    __tablename__ = "stock"
    sku: Mapped[str] = mapped_column(primary_key=True)
    quantite: Mapped[int]

Base.metadata.create_all(engine)


app = FastAPI(title="stock")


class StockIn(BaseModel):
    quantite: int

@app.put("/stock/{sku}")
def modifier_stock(sku: str, s: StockIn):
    with Session(engine) as db:
        stock = db.get(Stock, sku)
        if stock is None:
            db.add(Stock(sku=sku, quantite=s.quantite))
        else:
            stock.quantite = s.quantite
        db.commit()
    return {"sku": sku, "quantite": s.quantite}

@app.get("/stock/{sku}")
def lire_stock(sku: str):
    with Session(engine) as db:
        stock = db.get(Stock, sku)
        if stock is None:
            raise HTTPException(status_code=404, detail="SKU inconnu")
        return {"sku": stock.sku, "quantite": stock.quantite}