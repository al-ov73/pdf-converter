from logging import Logger

import requests
from dataclasses import dataclass, asdict
from typing import Optional

from fastapi import Request

@dataclass
class Visit:
    ip: str
    provider: Optional[str] = None
    organization: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    landing_page: Optional[str] = "superpdf.ru"
    is_new_user: Optional[bool] = None


def get_info_by_ip(ip: str, logger: Logger) -> Optional[Visit]:
    try:
        response = requests.get(url=f"http://ip-api.com/json/{ip}", timeout=5).json()
        ip = response.get("query")

        return Visit(
            ip=ip,
            provider=response.get("isp"),
            organization=response.get("org"),
            country=response.get("country"),
            region=response.get("regionName"),
            city=response.get("city"),
            landing_page="superpdf.ru",
        )

    except requests.exceptions.RequestException as e:
        logger.info(f"[!] Error getting IP info: {e}")
        return None


def send_visit_info(request: Request, logger: Logger) -> Optional[dict]:
    """Основная функция для отправки визита"""
    user_ip = request.client.host
    user_data = get_info_by_ip(user_ip, logger)

    if not user_data:
        logger.info("Failed to get user data")
        return None

    url = "https://memovoz.ru:8000/visit"
    headers = {"Content-Type": "application/json"}
    visit_data = asdict(user_data)

    try:
        response = requests.post(
            url,
            json=visit_data,
            headers=headers,
            timeout=10,
            verify=True
        )
        response.raise_for_status()
        logger.info(f"Handeled visit from ip -{visit_data['ip']}-")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.info(f"Error sending visit data: {e}")
        return None