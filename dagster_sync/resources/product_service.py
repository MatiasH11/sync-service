import os

import requests
from dagster import ConfigurableResource
from dagster_sync.types.product_api import ProductApiResponse
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

PAGE_SIZE = 250


class ProductServiceResource(ConfigurableResource):
    """Cliente HTTP para product-service (fuente de artículos)."""

    base_url: str = os.getenv(
        'PRODUCT_SERVICE_URL', 'https://product.distrisuper.com'
    )

    def _get_page(self, session: requests.Session, page: int) -> dict:
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
                },
                timeout=30,
            )
            response.raise_for_status()
            return response.json()

        return _fetch()

    def fetch_all_products(self) -> list[ProductApiResponse]:
        """Paginated fetch of all products, reusing a single HTTP session."""
        all_products: list[ProductApiResponse] = []
        page = 1

        with requests.Session() as session:
            while True:
                body = self._get_page(session, page)
                items = body.get('data', [])

                if not items:
                    break

                all_products.extend(items)

                links = body.get('links') or {}
                if not links.get('next'):
                    break

                page += 1

        if not all_products:
            raise ValueError(
                'product-service returned 0 products — aborting to prevent '
                'cache wipe'
            )

        return all_products
