import json
import logging
import msal
import requests


class SPGraphWorker:
    def __init__(self,
                 sp_tenant_id,
                 sp_client_id,
                 sp_secret
                 ):

        self.sp_tenant_id = sp_tenant_id
        self.sp_client_id = sp_client_id
        self.sp_secret = sp_secret

        self.default_scope = ["https://graph.microsoft.com/.default"]
        self.authority = "https://login.microsoftonline.com/" + self.sp_tenant_id

        self.req_headers = {"Authorization": "Bearer " + self._graph_auth()["access_token"],
                            "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly",
                            "ConsistencyLevel": "eventual"}

    def _graph_auth(self):
        try:
            app = msal.ConfidentialClientApplication(client_id=self.sp_client_id,
                                                     authority=self.authority,
                                                     client_credential=self.sp_secret)

        except Exception as eAuthClientTenantProblems:
            logging.info(eAuthClientTenantProblems)
            raise eAuthClientTenantProblems

        try:
            access_token = app.acquire_token_for_client(scopes=self.default_scope)
        except Exception as eAcquireTokenProblems:
            logging.info(eAcquireTokenProblems)
            raise eAcquireTokenProblems

        return access_token
