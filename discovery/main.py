import asyncio
from discovery_vn import TripAdvisorVNDiscovery


async def main():
    discovery = TripAdvisorVNDiscovery(
        headless=False,          # False nếu muốn nhìn trình duyệt
        max_provinces=None,     # None = chạy hết provinces.yaml
    )
    await discovery.run()


if __name__ == "__main__":
    asyncio.run(main())