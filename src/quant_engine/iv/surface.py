class IVSurfaceModel:
    def __init__(self, loader, model):
        self.loader = loader
        self.model = model

    def compute(self, date: str, symbol="BTC"):
        df = self.loader.load_daily(date, symbol)
        if df is None:
            return {"iv_skew": 0.0, "iv_level": 0.0}

        params = self.model.fit(df)

        # minimal feature example
        return {
            "iv_skew": params.get("rho", 0.0),
            "iv_level": params.get("alpha", 0.0),
        }