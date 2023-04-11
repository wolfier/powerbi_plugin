import time
from datetime import timedelta

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.email import send_email
from airflow.utils import timezone

from powerbi_plugin.hooks.powerbi import PowerBIHook


class PowerBIDatasetInGroupRefreshOperator(BaseOperator):
    """
    Triggers a refresh for the specified dataset from the specified workspace.
    By default, the operator will wait until the refresh has completed before
    exiting. The refresh status is checked every 60 seconds as a default. The
    operator will alert the user if the refresh is not completed by 600 seconds
    as a default.

    :param group_id: The workspace ID
    :param dataset_id: The dataset ID
    :param tenant_id: The subscription that the Azure Active Directory is associated with
    :param client_id: The id of the Azure Active Directory application
    :param client_secret: The value of the service principal secret
    :param check_interval: The interval to check for the refresh status
    :param delay_threshold: The threshold to alert the user of a delayed refresh
    :param powerbi_conn_id: Airflow Connection ID that contains the connection
        information for the Power BI account used for authentication.
    """
    template_fields = []
    template_ext = []
    ui_color = '#e4f0e8'

    failed_status = {"TimedOut", "Failed", "Disabled", "Cancelled"}
    non_terminal_status = {"Unknown", "NotStarted", "InProgress"}
    success_status = {"Completed"}

    def __init__(self,
                 group_id: str,
                 dataset_id: str,
                 tenant_id: str = None,
                 client_id: str = None,
                 client_secret: str = None,
                 check_interval: int = 60,
                 delay_threshold: int = 600,
                 wait_for_completion: bool = True,
                 powerbi_conn_id: str = 'powerbi_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.group_id = group_id
        self.dataset_id = dataset_id
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.check_interval = check_interval
        self.delay_threshold = delay_threshold
        self.wait_for_completion = wait_for_completion
        self.powerbi_conn_id = powerbi_conn_id
        self.alerted_delay = False

    def execute(self, context):
        hook = PowerBIHook(
            powerbi_conn_id=self.powerbi_conn_id,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            check_interval=self.check_interval,
        )

        request_id = hook.submit_refresh_dataset_in_group(
            self.group_id,
            self.dataset_id,
        )
        self.log.info('Refresh for %s initiated.', self.dataset_id)

        if self.wait_for_completion:
            timeout = timezone.utcnow() + timedelta(seconds=self.delay_threshold)

            while True:
                time.sleep(self.check_interval)

                status = hook.check_refresh_status(
                    group_id=self.group_id,
                    dataset_id=self.dataset_id,
                    refresh_id=request_id,
                )

                if status in self.failed_status:
                    raise AirflowException(f"Refresh terminated with status {status}")
                elif status not in self.non_terminal_status:
                    break

                if not self.alerted_delay and timezone.utcnow() > timeout:
                    try:
                        send_email(
                            context['task'].email,
                            f"Dataset refresh for {self.dataset_id} is delayed.",
                            f"Dataset refresh for {self.dataset_id} is delayed. Please contact Airflow Admin.",
                        )
                    except OSError:
                        self.log.info("Unable to send email to inform user that dataset refresh is delayed.")
                    self.alerted_delay = True

            if status in self.success_status:
                self.log.info('Refresh completed with status %s.', status)
