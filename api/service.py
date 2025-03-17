import logging
from typing import List

from fastapi import FastAPI, HTTPException, Query

from api.models import FirmResponse
from pipeline.config.run_configuration import RunConfiguration
from pipeline.sink.database_sink import DatabaseSink


class APIService:
    """FastAPI Service for SEC ADV Data"""

    def __init__(self, config: RunConfiguration):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.db_manager = DatabaseSink(config)
        self.app = FastAPI(title="SEC ADV Data API", description="API for SEC ADV Data")
        self.setup_routes()

    def setup_routes(self):
        """Setup API routes"""

        @self.app.get("/firms", tags=["Firms"])
        async def get_firms(limit: int = Query(10, ge=1, le=100)):
            """Get all firms"""
            try:
                df = self.db_manager.query_all().head(limit)

                return df.to_dict(orient="records")
            except Exception as e:
                self.logger.error(f"Error getting firms: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/firms/{firm_crd_nb}", tags=["Firms"])
        async def get_firm(firm_crd_nb: int):
            """Get a specific firm by CRD number"""
            try:
                df = self.db_manager.query_all()
                firm = df[df["firm_crd_nb"] == firm_crd_nb]

                if firm.empty:
                    raise HTTPException(status_code=404, detail=f"Firm with CRD {firm_crd_nb} not found")

                return firm.iloc[0].to_dict()
            except HTTPException as e:
                raise e
            except Exception as e:
                self.logger.error(f"Error getting firm {firm_crd_nb}: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/compensation/{firm_crd_nb}", tags=["Compensation"])
        async def get_compensation(firm_crd_nb: int):
            """Fetch compensation arrangements for a firm"""
            try:
                data = self.db_manager.fetch_compensation(firm_crd_nb)
                if not data:
                    return {"firm_crd_nb": firm_crd_nb, "compensation": [], "message": "No compensation data found."}
                return {"firm_crd_nb": firm_crd_nb, "compensation": data}
            except Exception as e:
                self.logger.error(f"Error fetching compensation data for {firm_crd_nb}: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/client_types/{firm_crd_nb}", tags=["Client Types"])
        async def get_client_types(firm_crd_nb: int):
            """Fetch client types and AUM"""
            try:
                data = self.db_manager.fetch_client_types(firm_crd_nb)
                if not data:
                    return {"firm_crd_nb": firm_crd_nb, "client_types": {}, "message": "No client type data found."}
                return {"firm_crd_nb": firm_crd_nb, "client_types": data}
            except Exception as e:
                self.logger.error(f"Error fetching client types for {firm_crd_nb}: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/private_funds/{firm_crd_nb}", tags=["Private Funds"])
        async def get_private_funds(firm_crd_nb: int):
            """Fetch private funds for a firm"""
            try:
                data = self.db_manager.fetch_private_funds(firm_crd_nb)
                if not data:
                    return {"firm_crd_nb": firm_crd_nb, "private_funds": [], "message": "No private funds found."}
                return {"firm_crd_nb": firm_crd_nb, "private_funds": data}
            except Exception as e:
                self.logger.error(f"Error fetching private funds for {firm_crd_nb}: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
