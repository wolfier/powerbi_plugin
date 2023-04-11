import msal

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException


class PowerBIHook(BaseHook):
    """
    Interact with Power BI REST API.

    :param powerbi_conn_id: HTTP connection with the host as the REST API base url
    :param tenant_id: The subscription that the Azure Active Directory is associated with
    :param client_id: The id of the Azure Active Directory application
    :param client_secret: The value of the service principal secret
    """
    base_url = "https://api.powerbi.com/v1.0"
    authority = "https://login.microsoftonline.com"
    scope = ["https://analysis.windows.net/powerbi/api/.default"]

    def __init__(
            self,
            powerbi_conn_id: str = "powerbi_default",
            check_interval=60,
            tenant_id: str = None,
            client_id: str = None,
            client_secret: str = None,
    ):
        super().__init__()
        self.check_interval = check_interval
        self.tenant_id = tenant_id or Variable.get('tenant_id')
        self.client_id = client_id or Variable.get('client_id')
        self.client_secret = client_secret or Variable.get('client_secret')
        self.powerbi_conn_id = powerbi_conn_id

    def _get_access_token(self) -> str:
        """
        Retrieve the `access_token` to authenticate against the PowerBI REST API.
        """
        client_app = msal.ConfidentialClientApplication(
            authority=f"{self.authority}/{self.tenant_id}",
            client_id=self.client_id,
            client_credential=self.client_secret
        )
        result = client_app.acquire_token_for_client(scopes=self.scope)

        if 'error' in result:
            raise PermissionError(
                "Permissions not authorized."
            )

        return result['access_token']

    def _prep_request_header(self) -> dict:
        """
        Prepare the headers to authenticate against the PowerBI REST API with.
        """
        access_token = self._get_access_token()
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    def submit_refresh_dataset_in_group(
            self,
            group_id: str,
            dataset_id: str,
    ) -> str:
        """
        Triggers an enhanced refresh for the specified dataset from the specified workspace.

        https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group
        https://learn.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh
        :param group_id: The workspace ID
        :param dataset_id: The dataset ID
        """
        hook = HttpHook(http_conn_id=self.powerbi_conn_id, method="post")
        response = hook.run(
            endpoint=f"myorg/groups/{group_id}/datasets/{dataset_id}/refreshes",
            headers=self._prep_request_header(),
            json={"type": "full"}
        )
        return response.headers["RequestId"]

    def check_refresh_status(
            self,
            group_id: str,
            dataset_id: str,
            refresh_id: str,
    ):
        """
        Returns execution details of an enhanced refresh operation for the specified dataset
        from the specified workspace.

        https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-execution-details-in-group
        https://learn.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh

        :param group_id: The workspace ID
        :param dataset_id: The dataset ID
        :param refresh_id: The refresh ID
        """
        hook = HttpHook(http_conn_id=self.powerbi_conn_id, method="get")
        try:
            response = hook.run(
                endpoint=f"myorg/groups/{group_id}/datasets/{dataset_id}/refreshes/{refresh_id}",
                headers=self._prep_request_header()
            )
            extended_status = response.json()["extendedStatus"]

            self.log.info(f"The current status of the refresh is %s.", extended_status)
            return extended_status
        except Exception:
            raise AirflowException("Failed to check the status of the refresh.")
