import os

import requests
from dagster import ConfigurableResource
from dagster_sync.types import ClientApiResponse
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

PAGE_SIZE = 250


class ClientServiceResource(ConfigurableResource):
    """Cliente HTTP para client-service (fuente de clientes)."""

    base_url: str = os.getenv(
        'CLIENT_SERVICE_URL', 'https://client.distrisuper.com'
    )

    def _get_page(self, session: requests.Session, page: int) -> dict:
        """Fetch a single page, returns the full response body."""

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
            reraise=True,
        )
        def _fetch() -> dict:
            response = session.get(
                f'{self.base_url}/v1',
                params={
                    'page[number]': page,
                    'page[size]': PAGE_SIZE,
                    'includeExcel': 'true',
                },
                timeout=30,
            )
            response.raise_for_status()
            return response.json()

        return _fetch()

    def fetch_all_clients(self) -> list[ClientApiResponse]:
        """Paginated fetch of all clients, reusing a single HTTP session."""
        all_clients: list[ClientApiResponse] = []
        page = 1

        with requests.Session() as session:
            while True:
                body = self._get_page(session, page)
                items = body.get('data', [])

                if not items:
                    break

                all_clients.extend(items)

                # Prefer links.next (JSON:API standard) over empty-page detection
                links = body.get('links') or {}
                if not links.get('next'):
                    break

                page += 1

        if not all_clients:
            raise ValueError(
                'client-service returned 0 clients — aborting to prevent '
                'cache wipe'
            )

        return all_clients
